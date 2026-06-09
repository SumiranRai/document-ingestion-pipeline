import logging
from pathlib import Path

from app.config import KAFKA_TOPICS
from app.extraction.bank_statement_extractor import extract_bank_statement_data
from app.extraction.id_document_extractor import extract_id_document_data
from app.extraction.invoice_extractor import extract_invoice_data
from app.kafka.consumers.base_consumer import BaseStageConsumer

logger = logging.getLogger(__name__)


class ExtractionConsumer(BaseStageConsumer):
    def __init__(self):
        super().__init__(KAFKA_TOPICS["preprocessed"], "extraction", KAFKA_TOPICS["extracted"])

    def _fetch_source(self, source_path: str) -> str:
        return source_path

    def process_event(self, event: dict):
        document_type = event["document_type"]
        source_path = event["source_path"]
        resolved_path = self._fetch_source(source_path)

        try:
            if document_type == "invoice":
                result = extract_invoice_data(resolved_path)
            elif document_type == "bank":
                result = extract_bank_statement_data(resolved_path)
            elif document_type in {"pan", "aadhaar"}:
                result = extract_id_document_data(resolved_path)
            else:
                raise ValueError(f"Unsupported document_type for extraction: {document_type}")

            if not result.get("success"):
                raise RuntimeError(result.get("error", "Extraction failed"))

            payload = {
                "extracted_data": result.get("data", {}),
                "confidence": result.get("confidence", 0.0),
                "raw_text": result.get("raw_text"),
            }
            self.publish_next(event, payload)
        finally:
            pass
