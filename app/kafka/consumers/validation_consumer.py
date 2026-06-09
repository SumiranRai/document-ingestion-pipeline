import logging

from app.config import KAFKA_TOPICS
from app.kafka.consumers.base_consumer import BaseStageConsumer
from app.kafka.event_schema import map_event_to_db_type
from app.validation.validator import validate_extracted_data

logger = logging.getLogger(__name__)


class ValidationConsumer(BaseStageConsumer):
    def __init__(self):
        super().__init__(KAFKA_TOPICS["extracted"], "validation", KAFKA_TOPICS["validated"])

    def process_event(self, event: dict):
        document_type = event["document_type"]
        extracted_data = event["payload"].get("extracted_data")

        if extracted_data is None:
            raise ValueError("Missing extracted_data in event payload")

        db_document_type = map_event_to_db_type(document_type)
        validation_result = validate_extracted_data(db_document_type, extracted_data)

        if not validation_result.get("valid", False):
            self.producer_client.publish_failed(
                document_id=event["document_id"],
                package_id=event["package_id"],
                stage=self.topic,
                error_details=str(validation_result.get("errors", [])),
                original_event=event,
            )
            return

        payload = {
            "extracted_data": extracted_data,
            "validation_result": validation_result,
        }
        self.publish_next(event, payload)
