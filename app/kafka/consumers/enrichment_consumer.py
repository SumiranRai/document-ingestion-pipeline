import logging

from app.config import KAFKA_TOPICS
from app.kafka.consumers.base_consumer import BaseStageConsumer

logger = logging.getLogger(__name__)


def _normalize_text(value):
    if not value:
        return value
    return " ".join(str(value).strip().split())


def _normalize_gstin(value):
    if not value:
        return value
    return str(value).strip().upper()


def _enrich_invoice(data: dict) -> dict:
    enriched = dict(data)
    enriched["vendor_name"] = _normalize_text(enriched.get("vendor_name"))
    enriched["customer_name"] = _normalize_text(enriched.get("customer_name"))
    enriched["vendor_gstin"] = _normalize_gstin(enriched.get("vendor_gstin"))
    enriched["customer_gstin"] = _normalize_gstin(enriched.get("customer_gstin"))
    enriched["payment_status"] = enriched.get("payment_status", "PENDING")
    return enriched


def _enrich_bank(data: dict) -> dict:
    enriched = dict(data)
    enriched["bank_name"] = _normalize_text(enriched.get("bank_name"))
    enriched["account_number"] = _normalize_text(enriched.get("account_number"))
    enriched["currency"] = _normalize_text(enriched.get("currency")) or "INR"
    return enriched


def _enrich_id_document(data: dict) -> dict:
    enriched = dict(data)
    enriched["holder_name"] = _normalize_text(enriched.get("holder_name"))
    enriched["id_number"] = _normalize_text(enriched.get("pan_number") or enriched.get("aadhaar_number"))
    enriched["id_type"] = enriched.get("id_type")
    return enriched


class EnrichmentConsumer(BaseStageConsumer):
    def __init__(self):
        super().__init__(KAFKA_TOPICS["validated"], "enrichment", KAFKA_TOPICS["enriched"])

    def process_event(self, event: dict):
        document_type = event["document_type"]
        extracted_data = event["payload"].get("extracted_data")

        if extracted_data is None:
            raise ValueError("Missing extracted_data in event payload")

        if document_type == "invoice":
            enriched_data = _enrich_invoice(extracted_data)
        elif document_type == "bank":
            enriched_data = _enrich_bank(extracted_data)
        elif document_type in {"pan", "aadhaar"}:
            enriched_data = _enrich_id_document(extracted_data)
        else:
            raise ValueError(f"Unsupported document_type for enrichment: {document_type}")

        payload = {
            "enriched_data": enriched_data,
            "validation_result": event["payload"].get("validation_result"),
        }
        self.publish_next(event, payload)
