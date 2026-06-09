"""
Multi-stage Kafka pipeline consumer.

Reads from documents.received and processes each document through:
  Extraction → Validation → Persistence

Publishes events to documents.extracted / documents.validated /
documents.persisted / documents.failed at each stage boundary.

Run standalone:  python -m app.kafka.consumer
"""

import json
import logging
import os
import time
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer

from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    KAFKA_CONSUMER_GROUP,
)
from app.extraction.bank_statement_extractor import extract_bank_statement_data
from app.extraction.document_type_detector import detect_document_type
from app.extraction.id_document_extractor import extract_id_document_data
from app.extraction.invoice_extractor import extract_invoice_data
# Utility functions for validation and DB mapping
from app.kafka.event_schema import map_to_db_type
from app.persistence.db_writer import DocumentManager
from app.validation.validator import validate_extracted_data

logger = logging.getLogger(__name__)

# Free-tier OCR.space rate limit (~1 req / 3 s)
_OCR_THROTTLE_SEC = 3.0


class PipelineConsumer:
    """
    Consumes DOCUMENT_RECEIVED events and runs the full pipeline:
    extract -> validate -> persist, publishing a Kafka event after each stage.
    """

    def __init__(self, bootstrap_servers=None, group_id=None):
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.group_id = group_id or f"{KAFKA_CONSUMER_GROUP}-worker"
        self.db = DocumentManager()
        self._consumer = KafkaConsumer(
            KAFKA_TOPICS["received"],
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            session_timeout_ms=30000,
        )
        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            retries=3,
        )
        logger.info("PipelineConsumer ready on %s", self.bootstrap_servers)

    # ── public entry point ─────────────────────────────────────────────────────

    def run(self, max_messages: int = None):
        """Start consuming.  Blocks until interrupted or max_messages reached."""
        count = 0
        try:
            for message in self._consumer:
                self._process(message.value)
                count += 1
                if max_messages and count >= max_messages:
                    logger.info("Reached max_messages=%d, stopping.", max_messages)
                    break
        except KeyboardInterrupt:
            logger.info("Consumer interrupted.")
        finally:
            self.close()

    # ── internal pipeline ──────────────────────────────────────────────────────

    def _process(self, event: dict):
        document_id = event.get("document_id")
        package_id  = event.get("package_id")
        file_path   = event.get("file_path")
        doc_type    = event.get("document_type")
        source      = event.get("source", "kafka")
        retry_count = int(event.get("retry_count", 0))

        try:
            # Normalise doc_type to canonical DB form
            if doc_type:
                doc_type = map_to_db_type(doc_type)
            if not doc_type or doc_type == "unknown":
                doc_type = map_to_db_type(detect_document_type(file_path))

            # Ensure package + document records exist (idempotent)
            if package_id:
                self.db.create_document_package(
                    package_name=f"kafka_pkg_{package_id[:8]}",
                    source=source,
                    total_documents=0,
                    package_id=package_id,
                    status="PENDING",
                )
            self.db.create_document_record(
                package_id=package_id,
                doc_type=doc_type,
                file_name=Path(file_path).name,
                file_path=file_path,
                source=source,
                file_size=os.path.getsize(file_path) if os.path.exists(file_path) else None,
                document_id=document_id,
            )

            # ── Stage 1: Extraction ────────────────────────────────────────────
            self.db.update_document_status(document_id, "PROCESSING")
            self.db.update_extraction_status(document_id, "IN_PROGRESS", started=True)

            if doc_type == "invoice":
                result = extract_invoice_data(file_path)
                time.sleep(_OCR_THROTTLE_SEC)
            elif doc_type == "bank_statement":
                result = extract_bank_statement_data(file_path)
            elif doc_type == "id_document":
                result = extract_id_document_data(file_path)
                time.sleep(_OCR_THROTTLE_SEC)
            else:
                raise ValueError(f"Unsupported document type: {doc_type}")

            if not result["success"]:
                raise RuntimeError(result.get("error", "Extraction failed"))

            extracted = result["data"]
            confidence = result.get("confidence", 0.0)

            self._publish(KAFKA_TOPICS["extracted"], {
                "event_type":    "DOCUMENT_EXTRACTED",
                "document_id":   document_id,
                "document_type": doc_type,
                "confidence":    confidence,
            })

            # ── Stage 2: Validation ────────────────────────────────────────────
            validation = validate_extracted_data(doc_type, extracted)
            if not validation["valid"]:
                raise RuntimeError(f"Validation failed: {validation['errors']}")

            self._publish(KAFKA_TOPICS["validated"], {
                "event_type":    "DOCUMENT_VALIDATED",
                "document_id":   document_id,
                "document_type": doc_type,
            })

            # ── Stage 3: Persistence ───────────────────────────────────────────
            self._persist(document_id, doc_type, extracted, confidence)

            self.db.update_document_status(document_id, "PERSISTED")
            self.db.update_extraction_status(document_id, "COMPLETED", completed=True)
            self.db.log_processing_event(
                document_id=document_id,
                package_id=package_id,
                stage="PIPELINE",
                status="SUCCESS",
                message=f"Processed {doc_type} with confidence={confidence:.2f}",
            )

            self._publish(KAFKA_TOPICS["persisted"], {
                "event_type":    "DOCUMENT_PERSISTED",
                "document_id":   document_id,
                "document_type": doc_type,
            })

            logger.info("OK  %s  type=%s  confidence=%.2f", document_id, doc_type, confidence)

        except Exception as exc:
            logger.error("FAIL %s: %s", document_id, exc, exc_info=True)
            try:
                self.db.update_document_status(document_id, "FAILED")
                self.db.update_extraction_status(document_id, "FAILED")
                self.db.log_processing_event(
                    document_id=document_id,
                    package_id=package_id,
                    stage="PIPELINE",
                    status="FAILED",
                    error_details=str(exc),
                )
            except Exception:
                pass
            self._publish(KAFKA_TOPICS["failed"], {
                "event_type":  "DOCUMENT_FAILED",
                "document_id": document_id,
                "package_id":  package_id,
                "error":       str(exc),
                "retry_count": retry_count,
            })

    def _persist(self, document_id: str, doc_type: str, data: dict, confidence: float):
        if doc_type == "invoice":
            self.db.save_invoice(document_id, {**data, "extraction_confidence": confidence})

        elif doc_type == "bank_statement":
            data = data.copy()
            transactions = data.pop("transactions", [])
            stmt_id = self.db.save_bank_statement(
                document_id, {**data, "transaction_count": len(transactions)}
            )
            if transactions:
                self.db.save_transactions(str(stmt_id), transactions)

        elif doc_type == "id_document":
            self.db.save_id_document(document_id, {**data, "extraction_confidence": confidence})

    def _publish(self, topic: str, payload: dict):
        try:
            self._producer.send(topic, value=payload)
        except Exception as exc:
            logger.warning("Could not publish to %s: %s", topic, exc)

    def close(self):
        if self._consumer:
            self._consumer.close()
        if self._producer:
            self._producer.flush()
            self._producer.close()
        logger.info("PipelineConsumer closed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    PipelineConsumer().run()
