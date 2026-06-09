import logging

from app.config import KAFKA_TOPICS
from app.kafka.consumers.base_consumer import BaseStageConsumer
from app.persistence.db_writer import DocumentManager

logger = logging.getLogger(__name__)


class DlqConsumer(BaseStageConsumer):
    def __init__(self):
        super().__init__(KAFKA_TOPICS["failed"], "dlq", None)
        self.db = DocumentManager()

    def process_event(self, event: dict):
        document_id = event.get("document_id")
        package_id = event.get("package_id")
        payload = event.get("payload", {})
        error_details = payload.get("error_details")

        if document_id:
            self.db.update_document_status(document_id, "FAILED")

        self.db.log_processing_event(
            document_id=document_id,
            package_id=package_id,
            stage="dlq",
            status="FAILED",
            message=payload.get("stage", "failed_event"),
            error_details=error_details,
        )

        self.db.update_package_status(package_id)
        self.producer_client.publish_dlq(event, error_details or "Document failed")
