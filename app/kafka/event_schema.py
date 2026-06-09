"""
Kafka event schemas and document-type helpers.
"""

import datetime
import json
import uuid
from typing import Any, Dict, Optional

# ── Document type normalisation ───────────────────────────────────────────────
# Maps any variant a caller might pass → the canonical DB document type.
_TO_DB_TYPE: Dict[str, str] = {
    "invoice":        "invoice",
    "bank":           "bank_statement",
    "bank_statement": "bank_statement",
    "pan":            "id_document",
    "aadhaar":        "id_document",
    "id_document":    "id_document",
}

SUPPORTED_DB_TYPES = {"invoice", "bank_statement", "id_document"}


def map_to_db_type(document_type: str) -> str:
    """Return the canonical DB document type for any input variant."""
    if not document_type:
        return "unknown"
    return _TO_DB_TYPE.get(str(document_type).strip().lower(), "unknown")


# Backward-compat aliases used by ingestion_service.py
normalize_document_type = map_to_db_type
map_event_to_db_type    = map_to_db_type


# ── Event construction ─────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def build_event(
    event_type: str,
    document_id: str,
    package_id: str,
    document_type: str,
    file_path: str,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a standard pipeline event dict."""
    return {
        "event_id":      str(uuid.uuid4()),
        "event_type":    event_type,
        "timestamp":     _now_iso(),
        "document_id":   str(document_id),
        "package_id":    str(package_id),
        "document_type": map_to_db_type(document_type),
        "file_path":     file_path,
        "payload":       payload or {},
    }


def safe_serialize(obj: Any) -> str:
    try:
        return json.dumps(obj)
    except TypeError:
        return json.dumps(str(obj))
