import logging

from app.config import KAFKA_TOPICS
from app.kafka.consumers.base_consumer import BaseStageConsumer
from app.kafka.event_schema import map_event_to_db_type
from app.persistence.db_writer import DocumentManager

logger = logging.getLogger(__name__)


class PersistenceConsumer(BaseStageConsumer):
    def __init__(self):
        super().__init__(KAFKA_TOPICS["enriched"], "persistence", KAFKA_TOPICS["persisted"])
        self.db = DocumentManager()

    def process_event(self, event: dict):
        document_type = event["document_type"]
        enriched_data = event["payload"].get("enriched_data")

        if enriched_data is None:
            raise ValueError("Missing enriched_data in event payload")

        if document_type == "invoice":
            self.db.save_invoice(event["document_id"], enriched_data)
        elif document_type == "bank":
            statement_id = self.db.save_bank_statement(event["document_id"], enriched_data)
            transactions = enriched_data.get("transactions")
            if transactions and statement_id:
                self.db.save_transactions(statement_id, transactions)
        elif document_type in {"pan", "aadhaar"}:
            self.db.save_id_document(event["document_id"], enriched_data)
        else:
            raise ValueError(f"Unsupported document_type for persistence: {document_type}")

        self.db.update_document_status(event["document_id"], "PERSISTED")
        self.db.update_package_status(event["package_id"])

        payload = {
            "enriched_data": enriched_data,
            "document_status": "PERSISTED",
        }
        self.publish_next(event, payload)
