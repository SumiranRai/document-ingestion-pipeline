"""
Central configuration for the document ingestion pipeline.
All environment variables and constants live here.
"""

import os

# ── Pipeline Database ──────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "pipeline_db")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

DB_CONNECTION_STRING = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ── Kafka ──────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

KAFKA_TOPICS = {
    "received":  "documents.received",
    "extracted": "documents.extracted",
    "validated": "documents.validated",
    "persisted": "documents.persisted",
    "failed":    "documents.failed",
    "dlq":       "documents.dlq",
}

KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "document-pipeline")

# ── Ingestion ──────────────────────────────────────────────────────────────────
DOCUMENT_INGESTION_PATH = os.getenv("DOCUMENT_INGESTION_PATH", "./sample")

# ── Document type registry ────────────────────────────────────────────────────
DOCUMENT_TYPES = {
    "invoice": {
        "extensions": [".pdf", ".jpg", ".jpeg", ".png"],
        "subdir":     "Invoices",
        "description": "Tax invoices and purchase invoices",
    },
    "bank_statement": {
        "extensions": [".csv", ".pdf", ".xlsx"],
        "subdir":     "Bank Statement",
        "description": "Bank account statements",
    },
    "id_document": {
        "extensions": [".jpg", ".jpeg", ".png", ".pdf"],
        "subdir":     "PAN",
        "description": "Identity documents (PAN card)",
    },
}

# ── Extraction ─────────────────────────────────────────────────────────────────
EXTRACTION_CONFIDENCE_THRESHOLD = float(os.getenv("EXTRACTION_CONFIDENCE_THRESHOLD", "0.5"))
MAX_RETRIES                      = int(os.getenv("MAX_RETRIES", "3"))

# ── OCR.space ──────────────────────────────────────────────────────────────────
OCR_API_KEY        = os.getenv("OCR_API_KEY", "helloworld")
OCR_SPACE_API_KEY  = OCR_API_KEY   # alias used in ocr_space.py
OCR_SPACE_URL      = os.getenv("OCR_SPACE_URL", "https://api.ocr.space/parse/image")
OCR_SPACE_LANGUAGE = os.getenv("OCR_SPACE_LANGUAGE", "eng")
OCR_SPACE_OCR_ENGINE = os.getenv("OCR_SPACE_OCR_ENGINE", "2")

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
