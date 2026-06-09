"""
Dead Letter Queue Consumer
==========================
Reads from documents.failed, detects retryable errors (OCR rate limits,
timeouts, connection issues), and requeues the document back to
documents.received with an incremented retry_count.

After DLQ_MAX_RETRIES attempts the document is marked permanently failed
in the DB and dropped from the queue.

Run as a service:  python -m app.kafka.dlq_consumer
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS
from app.persistence.db_writer import DocumentManager

logger = logging.getLogger(__name__)

MAX_RETRIES = int(os.getenv("DLQ_MAX_RETRIES", "3"))
RETRY_DELAY_SEC = int(os.getenv("DLQ_RETRY_DELAY_SEC", "60"))

# Substrings that identify retryable, transient errors
_RETRYABLE = ["rate limit", "timeout", "connection", "503", "429", "retries"]


class DLQConsumer:
    """
    Reads DOCUMENT_FAILED events, re-enqueues retryable documents, and
    permanently marks documents that have exhausted their retry budget.
    """

    def __init__(self):
        self.db = DocumentManager()
        self._running = True

        self._consumer = KafkaConsumer(
            KAFKA_TOPICS["failed"],
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="kafka-dlq-consumer",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            session_timeout_ms=60_000,
            heartbeat_interval_ms=20_000,
        )
        self._producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            acks="all",
            retries=3,
        )
        logger.info(
            "DLQConsumer ready — reading %s, max_retries=%d, delay=%ds",
            KAFKA_TOPICS["failed"], MAX_RETRIES, RETRY_DELAY_SEC,
        )

    # ── public ────────────────────────────────────────────────────────────────

    def run(self):
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)
        logger.info("DLQ consumer started.")
        try:
            for message in self._consumer:
                if not self._running:
                    break
                self._handle(message)
        except Exception as exc:
            logger.error("DLQ consumer crashed: %s", exc, exc_info=True)
        finally:
            self._consumer.close()
            self._producer.flush()
            self._producer.close()
            logger.info("DLQ consumer stopped.")

    # ── internals ─────────────────────────────────────────────────────────────

    def _handle(self, message):
        payload = message.value or {}
        document_id = payload.get("document_id")
        error = payload.get("error", "")
        retry_count = int(payload.get("retry_count", 0))

        logger.info(
            "DLQ received  doc=%s  retry=%d  error=%.100s",
            document_id, retry_count, error,
        )

        if not document_id:
            logger.warning("DLQ event missing document_id — skipping.")
            return

        if not self._is_retryable(error):
            logger.info("Non-retryable error for %s — skipping.", document_id)
            return

        if retry_count >= MAX_RETRIES:
            logger.warning(
                "Max retries (%d) reached for %s — marking permanently failed.",
                MAX_RETRIES, document_id,
            )
            self._mark_permanently_failed(document_id, error, retry_count)
            return

        # Look up document details from the DB so we can reconstruct a full
        # DOCUMENT_RECEIVED event (the failed event may lack file_path etc.)
        doc = self.db.get_document(document_id)
        if not doc:
            logger.warning("Document %s not found in DB — cannot requeue.", document_id)
            return

        next_retry = retry_count + 1
        logger.info(
            "Requeuing doc=%s for retry #%d in %ds",
            document_id, next_retry, RETRY_DELAY_SEC,
        )
        time.sleep(RETRY_DELAY_SEC)

        retry_event = {
            "event_type":    "DOCUMENT_RECEIVED",
            "document_id":   document_id,
            "package_id":    str(doc["package_id"]) if doc.get("package_id") else None,
            "file_path":     doc.get("file_path"),
            "document_type": doc.get("document_type"),
            "source":        "dlq_retry",
            "retry_count":   next_retry,
            "timestamp":     datetime.now(timezone.utc).isoformat(),
        }

        try:
            self._producer.send(KAFKA_TOPICS["received"], key=document_id, value=retry_event)
            self._producer.flush()
            # Reset DB status so DAG / consumer picks it up on next pass
            self.db.update_document_status(document_id, "RECEIVED")
            self.db.log_processing_event(
                document_id=document_id,
                package_id=retry_event.get("package_id"),
                stage="DLQ_RETRY",
                status="REQUEUED",
                message=f"Requeued for retry #{next_retry} after: {error[:200]}",
            )
            logger.info("doc=%s requeued for retry #%d", document_id, next_retry)
        except Exception as exc:
            logger.error("Failed to requeue %s: %s", document_id, exc)

    @staticmethod
    def _is_retryable(error: str) -> bool:
        low = (error or "").lower()
        return any(marker in low for marker in _RETRYABLE)

    def _mark_permanently_failed(self, document_id: str, error: str, retry_count: int):
        try:
            self.db.update_document_status(document_id, "FAILED")
            self.db.log_processing_event(
                document_id=document_id,
                stage="DLQ_CONSUMER",
                status="FAILED",
                message=f"Permanently failed after {retry_count} retries",
                error_details=error[:500],
            )
        except Exception as exc:
            logger.warning("Could not persist permanent-fail for %s: %s", document_id, exc)

    def _shutdown(self, signum, frame):
        logger.info("Shutdown signal received — stopping DLQ consumer.")
        self._running = False


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    DLQConsumer().run()
