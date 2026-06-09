"""
Kafka Event Logger Service
==========================
Subscribes to ALL pipeline topics and persists every event to the
kafka_events table in pipeline_db, providing a full audit trail
visible in the Streamlit dashboard.

Run as a service:  python -m app.kafka.event_logger
"""

import json
import logging
import signal
import sys

from kafka import KafkaConsumer

from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS
from app.persistence.db_writer import DocumentManager

logger = logging.getLogger(__name__)

ALL_TOPICS = list(KAFKA_TOPICS.values())


class KafkaEventLogger:
    """
    Long-running consumer that subscribes to every pipeline topic and
    writes each message to the kafka_events table.  Acts as a passive
    observer — it never publishes back to Kafka.
    """

    def __init__(self):
        self.db = DocumentManager()
        self._running = True
        self._consumer = KafkaConsumer(
            *ALL_TOPICS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="kafka-event-logger",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            session_timeout_ms=30_000,
            heartbeat_interval_ms=10_000,
        )
        logger.info("KafkaEventLogger subscribed to: %s", ALL_TOPICS)

    # ── public ────────────────────────────────────────────────────────────────

    def run(self):
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)
        logger.info("Event logger started — logging to kafka_events table.")
        try:
            for message in self._consumer:
                if not self._running:
                    break
                self._log_event(message)
        except Exception as exc:
            logger.error("Event logger crashed: %s", exc, exc_info=True)
            sys.exit(1)
        finally:
            self._consumer.close()
            logger.info("Event logger stopped.")

    # ── internals ─────────────────────────────────────────────────────────────

    def _log_event(self, message):
        try:
            payload = message.value or {}
            event_type = payload.get("event_type", "UNKNOWN")
            document_id = payload.get("document_id")
            msg_key = message.key.decode("utf-8") if message.key else None

            self.db.log_kafka_event(
                topic=message.topic,
                message_key=msg_key,
                message_value=payload,
                event_type=event_type,
                document_id=document_id,
                partition=message.partition,
                offset=message.offset,
            )
            logger.debug(
                "Logged %s  topic=%s  partition=%d  offset=%d",
                event_type, message.topic, message.partition, message.offset,
            )
        except Exception as exc:
            # Log and continue — never crash the logger on a single bad message
            logger.warning(
                "Failed to log event from %s (offset=%d): %s",
                message.topic, message.offset, exc,
            )

    def _shutdown(self, signum, frame):
        logger.info("Shutdown signal received — stopping event logger.")
        self._running = False


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    KafkaEventLogger().run()
