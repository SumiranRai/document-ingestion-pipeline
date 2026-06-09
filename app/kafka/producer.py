"""
Kafka producer for the document ingestion pipeline.
Publishes document lifecycle events to Kafka topics.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from kafka import KafkaProducer
from kafka.errors import KafkaError

from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    DOCUMENT_TYPES,
)
from app.extraction.document_type_detector import detect_document_type, detect_document_type_from_name
from app.kafka.event_schema import map_to_db_type
from app.persistence.db_writer import DocumentManager

logger = logging.getLogger(__name__)

class DocumentProducer:
    """Publishes document events to Kafka."""

    def __init__(self, bootstrap_servers=None):
        # sets up broker addresses
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"), 
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            acks="all",
            retries=3,
        )
        logger.info("Kafka producer connected to %s", self.bootstrap_servers)

    def publish_received(
        self,
        document_id: str,
        package_id: str,
        file_path: str,
        document_type: str = None,
        source: str = None,
        file_bytes: bytes = None,
    ) -> bool:
        """Publish a DOCUMENT_RECEIVED event."""
        if not document_type:
            try:
                document_type = (
                    detect_document_type_from_name(file_path)
                    if file_bytes is not None
                    else detect_document_type(file_path)
                )
            except Exception:
                document_type = detect_document_type_from_name(file_path)

        db_type = map_to_db_type(document_type)

        message = {
            "event_type":    "DOCUMENT_RECEIVED",
            "document_id":   document_id,
            "package_id":    package_id,
            "file_path":     file_path,
            "document_type": db_type,
            "source":        source,
            "retry_count":   0,
            "timestamp":     datetime.now(timezone.utc).isoformat(),
        }
        return self._send(KAFKA_TOPICS["received"], document_id, message)

    def publish_extracted(
        self, document_id: str, doc_type: str, confidence: float,
        package_id: str = None
    ) -> bool:
        message = {
            "event_type":    "DOCUMENT_EXTRACTED",
            "document_id":   document_id,
            "package_id":    package_id,
            "document_type": doc_type,
            "confidence":    confidence,
            "timestamp":     datetime.now(timezone.utc).isoformat(),
        }
        return self._send(KAFKA_TOPICS["extracted"], document_id, message)

    def publish_validated(
        self, document_id: str, doc_type: str, package_id: str = None
    ) -> bool:
        message = {
            "event_type":    "DOCUMENT_VALIDATED",
            "document_id":   document_id,
            "package_id":    package_id,
            "document_type": doc_type,
            "timestamp":     datetime.now(timezone.utc).isoformat(),
        }
        return self._send(KAFKA_TOPICS["validated"], document_id, message)

    def publish_persisted(
        self, document_id: str, doc_type: str, package_id: str = None
    ) -> bool:
        message = {
            "event_type":    "DOCUMENT_PERSISTED",
            "document_id":   document_id,
            "package_id":    package_id,
            "document_type": doc_type,
            "timestamp":     datetime.now(timezone.utc).isoformat(),
        }
        return self._send(KAFKA_TOPICS["persisted"], document_id, message)

    def publish_failed(
        self, document_id: str, stage: str, error: str,
        package_id: str = None, retry_count: int = 0
    ) -> bool:
        message = {
            "event_type":  "DOCUMENT_FAILED",
            "document_id": document_id,
            "package_id":  package_id,
            "stage":       stage,
            "error":       error,
            "retry_count": retry_count,
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        }
        return self._send(KAFKA_TOPICS["failed"], document_id, message)

    def _send(self, topic: str, key: str, message: dict) -> bool:
        try:
            future = self._producer.send(topic, key=key, value=message)
            future.get(timeout=10)
            logger.debug("Published %s to %s", message.get("event_type"), topic)
            return True
        except KafkaError as e:
            logger.error("Failed to publish to %s: %s", topic, e)
            self._dlq(key, message, str(e))
            return False

    def _dlq(self, key: str, message: dict, reason: str):
        try:
            self._producer.send(
                KAFKA_TOPICS["dlq"],
                key=key,
                value={
                    "original":  message,
                    "error":     reason,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
        except Exception as e:
            logger.error("DLQ publish also failed: %s", e)

    def close(self):
        if self._producer:
            self._producer.flush()
            self._producer.close()


def scan_and_publish(ingestion_path: str) -> str:
    """
    Scan configured subdirectories, create a package, and publish DOCUMENT_RECEIVED events.
    Returns the package_id.
    """
    from uuid import uuid4

    producer = DocumentProducer()
    db = DocumentManager()
    base = Path(ingestion_path)

    documents = []
    for doc_type, cfg in DOCUMENT_TYPES.items():
        subdir = base / cfg["subdir"]
        if not subdir.exists():
            logger.info("Subdir not found, skipping: %s", subdir)
            continue
        exts = set(cfg["extensions"])
        for f in subdir.iterdir():
            if f.is_file() and f.suffix.lower() in exts:
                documents.append({"path": str(f), "name": f.name, "type": doc_type, "size": f.stat().st_size})

    if not documents:
        logger.warning("No documents found in %s", ingestion_path)

    package_id = str(uuid4())
    db.create_document_package(
        package_name=f"scan_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
        source="local_scan",
        total_documents=len(documents),
        package_id=package_id,
        status="PENDING",
    )

    for doc in documents:
        document_id = str(uuid4())
        db.create_document_record(
            package_id=package_id,
            doc_type=doc["type"],
            file_name=doc["name"],
            file_path=doc["path"],
            source="local_scan",
            file_size=doc["size"],
            document_id=document_id,
        )
        producer.publish_received(
            document_id=document_id,
            package_id=package_id,
            file_path=doc["path"],
            document_type=doc["type"],
            source="local_scan",
        )
        logger.info("Published: %s (%s)", doc["name"], doc["type"])

    producer.close()
    logger.info("Package %s published with %d documents", package_id, len(documents))
    return package_id


if __name__ == "__main__":
    import os
    import logging as _logging
    _logging.basicConfig(level=logging.INFO)
    scan_and_publish(os.getenv("DOCUMENT_INGESTION_PATH", "./sample"))
