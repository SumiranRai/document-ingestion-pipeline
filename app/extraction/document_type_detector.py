"""Document type detection helpers.

The detector uses file metadata and lightweight content heuristics to choose
between invoices, bank statements, and ID documents without requiring manual input.
"""

import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from app.extraction.ocr_space import ocr_space_extract_text

logger = logging.getLogger(__name__)

_PAN_PATTERN = re.compile(r"\b[A-Z]{5}[0-9]{4}[A-Z]\b")
_AADHAAR_PATTERN = re.compile(r"(?:\d{4}\s?){3}\d{4}")
_INVOICE_KEYWORDS = re.compile(r"\binvoice\b|\binvoice no\b|\btotal amount\b|\bgst\b|\btax invoice\b", re.IGNORECASE)
_BANK_KEYWORDS = re.compile(r"\baccount number\b|\bstatement\b|\bdebit\b|\bcredit\b|\bbalance\b|\btransaction\b", re.IGNORECASE)
_ID_KEYWORDS = re.compile(r"\bPAN\b|\bAadhaar\b|\bUIDAI\b|\bआधार\b|\bपैन\b", re.IGNORECASE)

_CSV_BANK_HEADERS = {
    "date", "drcr", "debit", "credit", "amount", "balance", "description", "narration", "particulars"
}


def _normalize_file_name(file_name: str) -> str:
    return file_name.strip().lower()


def detect_document_type_from_name(file_name: str) -> str:
    """Detect document type from filename and extension only."""
    name = _normalize_file_name(file_name)
    ext = Path(file_name).suffix.lower()

    if ext in {".csv", ".xls", ".xlsx"}:
        return "bank"
    if ext == ".txt":
        return "invoice"

    if any(keyword in name for keyword in ["pan"]):
        return "pan"
    if any(keyword in name for keyword in ["aadhaar", "uidai", "aadhar"]):
        return "aadhaar"
    if any(keyword in name for keyword in ["invoice", "bill", "gst", "tax"]):
        return "invoice"
    if any(keyword in name for keyword in ["statement", "bank", "txn", "transaction"]):
        return "bank"

    if ext in {".jpg", ".jpeg", ".png", ".pdf"}:
        return "invoice"

    return "invoice"


def _load_text(path: Path) -> str:
    ext = path.suffix.lower()

    try:
        if ext == ".txt":
            return path.read_text(encoding="utf-8", errors="replace")

        if ext == ".pdf":
            try:
                import pdfplumber
            except ImportError:
                logger.warning("pdfplumber not installed; falling back to filename detection")
                return ""

            text_parts = []
            with pdfplumber.open(str(path)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    text_parts.append(page_text)
            text = "\n".join(text_parts).strip()
            if len(text) < 100:
                return ocr_space_extract_text(str(path))
            return text

        if ext in {".jpg", ".jpeg", ".png"}:
            return ocr_space_extract_text(str(path))

        if ext in {".csv", ".xls", ".xlsx"}:
            return ""

    except Exception as exc:
        logger.warning(f"Could not load text for detection from {path}: {exc}")

    return ""


def _detect_from_text(text: str) -> Optional[str]:
    if not text:
        return None

    if _PAN_PATTERN.search(text):
        return "pan"
    if _AADHAAR_PATTERN.search(text):
        return "aadhaar"
    if _ID_KEYWORDS.search(text):
        return "pan"

    if _BANK_KEYWORDS.search(text):
        return "bank"

    if _INVOICE_KEYWORDS.search(text):
        return "invoice"

    return None


def _detect_spreadsheet_type(file_path: Path) -> str:
    try:
        if file_path.suffix.lower() == ".csv":
            df = pd.read_csv(file_path, nrows=5, dtype=str, keep_default_na=False)
        else:
            df = pd.read_excel(file_path, nrows=5, dtype=str)

        columns = {str(col).strip().lower() for col in df.columns}
        if columns & _CSV_BANK_HEADERS:
            return "bank_statement"
    except Exception as exc:
        logger.warning(f"Could not inspect spreadsheet for type detection: {exc}")

    return "bank_statement"


def detect_document_type(file_path: str) -> str:
    """Detect document type by filename, extension, and lightweight content heuristics."""
    path = Path(file_path)
    ext = path.suffix.lower()

    if ext in {".csv", ".xls", ".xlsx"}:
        return _detect_spreadsheet_type(path)

    if ext == ".txt":
        return "invoice"

    if ext in {".jpg", ".jpeg", ".png", ".pdf"}:
        text = _load_text(path)
        document_type = _detect_from_text(text)
        if document_type:
            return document_type
        return detect_document_type_from_name(path.name)

    return detect_document_type_from_name(path.name)
