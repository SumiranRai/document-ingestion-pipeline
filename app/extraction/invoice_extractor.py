"""
Invoice Extractor
Handles:
    - .txt  → structured regex parsing
    - .jpg / .png / .jpeg → OCR.space OCR then regex parsing
    - .pdf  → pdfplumber text extraction then regex parsing

Extracted fields
────────────────
invoice_number, invoice_date, due_date,
vendor_name, vendor_gstin, vendor_address,
customer_name, customer_gstin, customer_address,
line_items (list), subtotal, tax_amount, total_amount,
currency, payment_terms, payment_status
"""

import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

from app.extraction.ocr_space import ocr_space_extract_text

# ── regex patterns ─────────────────────────────────────────────────────────────

_INVOICE_NUMBER = re.compile(
    r"(?:invoice\s*(?:no|number|#)\s*[:\-]?\s*)([A-Z0-9\-/]+)",
    re.IGNORECASE,
)
_DATE = re.compile(
    r"(?:invoice\s*date|date\s*of\s*issue|date)[:\s]+(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{4}[\/\-]\d{2}[\/\-]\d{2})",
    re.IGNORECASE,
)
_DUE_DATE = re.compile(
    r"(?:due\s*date|payment\s*due)[:\s]+(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{4}[\/\-]\d{2}[\/\-]\d{2})",
    re.IGNORECASE,
)
_VENDOR_NAME = re.compile(
    r"(?:vendor\s*name|from|vendor|supplier|billed?\s*by|issued?\s*by|sold\s*by)"
    r"[:\s]+([A-Za-z0-9\s&.,'\-]+?)(?:\r?\n|gstin|gst\s|pan\s|$)",
    re.IGNORECASE,
)
_CUSTOMER_NAME = re.compile(
    r"(?:customer\s*name|to|bill\s*to|ship\s*to|customer|client|buyer)"
    r"[:\s]+([A-Za-z0-9\s&.,'\-]+?)(?:\r?\n|gstin|gst\s|pan\s|$)",
    re.IGNORECASE,
)
_GSTIN = re.compile(r"\b(\d{2}[A-Z]{5}\d{4}[A-Z]{1}[A-Z\d]{1}[Z]{1}[A-Z\d]{1})\b")
# Currency-prefix-aware amount patterns: "Total Amount : INR 47,215.00" or "Total : ₹ 1,234"
_AMOUNT   = re.compile(
    r"(?:total\s*amount|grand\s*total|amount\s*due|total)[:\s]+(?:INR|USD|EUR|GBP|Rs\.?|₹)?\s*([0-9,]+(?:\.\d{2})?)",
    re.IGNORECASE,
)
_SUBTOTAL = re.compile(
    r"(?:sub\s*total|subtotal|amount\s*before\s*tax)[:\s]+(?:INR|USD|EUR|Rs\.?|₹)?\s*([0-9,]+(?:\.\d{2})?)",
    re.IGNORECASE,
)
_TAX      = re.compile(
    r"(?:gst\s*@\s*\d+%|igst|cgst|sgst|vat|tax\s*amount|tax)[:\s]+(?:INR|Rs\.?|₹)?\s*([0-9,]+(?:\.\d{2})?)",
    re.IGNORECASE,
)
_CURRENCY = re.compile(r"\b(INR|USD|EUR|GBP|₹)\b", re.IGNORECASE)

# Line-item table row: "1  Widget A  10  500.00  5000.00"
_LINE_ITEM = re.compile(
    r"^\s*(\d+)\s+(.+?)\s+(\d+(?:\.\d+)?)\s+([0-9,]+(?:\.\d{2})?)\s+([0-9,]+(?:\.\d{2})?)\s*$",
    re.MULTILINE,
)


# ── helpers ────────────────────────────────────────────────────────────────────

def _parse_amount(raw: str) -> Optional[float]:
    try:
        return float(raw.replace(",", ""))
    except (ValueError, AttributeError):
        return None


def _parse_date(raw: str) -> Optional[str]:
    """Normalise various date formats to YYYY-MM-DD.
    Tries unambiguous ISO format first, then common regional formats.
    MM/DD/YYYY (US) is tried last to avoid mis-parsing DD/MM/YYYY dates.
    """
    for fmt in (
        "%Y-%m-%d", "%Y/%m/%d",           # unambiguous ISO
        "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y",  # DD/MM/YYYY (common in India)
        "%d/%m/%y", "%d-%m-%y",
        "%m/%d/%Y", "%m-%d-%Y",            # MM/DD/YYYY (US format, try last)
        "%m/%d/%y",
    ):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()


def _extract_from_text(text: str) -> Dict[str, Any]:
    """Run all regex patterns against raw text and return field dict."""
    data: Dict[str, Any] = {}

    m = _INVOICE_NUMBER.search(text)
    data["invoice_number"] = m.group(1).strip() if m else None

    m = _DATE.search(text)
    data["invoice_date"] = _parse_date(m.group(1)) if m else None

    m = _DUE_DATE.search(text)
    data["due_date"] = _parse_date(m.group(1)) if m else None

    m = _VENDOR_NAME.search(text)
    data["vendor_name"] = m.group(1).strip()[:200] if m else None

    m = _CUSTOMER_NAME.search(text)
    data["customer_name"] = m.group(1).strip()[:200] if m else None

    gstins = _GSTIN.findall(text)
    data["vendor_gstin"]   = gstins[0] if len(gstins) > 0 else None
    data["customer_gstin"] = gstins[1] if len(gstins) > 1 else None

    m = _AMOUNT.search(text)
    data["total_amount"] = _parse_amount(m.group(1)) if m else None

    m = _SUBTOTAL.search(text)
    data["subtotal"] = _parse_amount(m.group(1)) if m else None

    # Sum all tax matches (CGST + SGST + IGST etc.)
    tax_matches = _TAX.findall(text)
    if tax_matches:
        amounts = [_parse_amount(t) for t in tax_matches]
        amounts = [a for a in amounts if a is not None]
        data["tax_amount"] = round(sum(amounts), 2) if amounts else None
    else:
        data["tax_amount"] = None

    m = _CURRENCY.search(text)
    raw_cur = m.group(1) if m else "INR"
    data["currency"] = "INR" if raw_cur == "₹" else raw_cur.upper()

    # Derive subtotal / tax from total if only one is missing
    if data["total_amount"] and data["subtotal"] and not data["tax_amount"]:
        data["tax_amount"] = round(data["total_amount"] - data["subtotal"], 2)
    if data["total_amount"] and data["tax_amount"] and not data["subtotal"]:
        data["subtotal"] = round(data["total_amount"] - data["tax_amount"], 2)

    # Line items
    line_items: List[Dict] = []
    for m in _LINE_ITEM.finditer(text):
        line_items.append({
            "sr_no":       int(m.group(1)),
            "description": m.group(2).strip(),
            "quantity":    float(m.group(3)),
            "unit_price":  _parse_amount(m.group(4)),
            "amount":      _parse_amount(m.group(5)),
        })
    data["line_items"] = line_items

    # Payment status heuristic
    text_lower = text.lower()
    if "paid" in text_lower:
        data["payment_status"] = "PAID"
    elif "overdue" in text_lower or "due" in text_lower:
        data["payment_status"] = "DUE"
    else:
        data["payment_status"] = "PENDING"

    return data


def _confidence(data: Dict[str, Any]) -> float:
    """Score extraction quality 0–1 based on key fields present."""
    key_fields = [
        "invoice_number", "invoice_date", "vendor_name",
        "customer_name", "total_amount",
    ]
    filled = sum(1 for f in key_fields if data.get(f))
    return round(filled / len(key_fields), 2)


# ── per-format readers ─────────────────────────────────────────────────────────

def _read_txt(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def _read_image_ocr(path: str) -> str:
    """OCR.space OCR on JPEG / PNG."""
    return ocr_space_extract_text(path)


def _read_pdf(path: str) -> str:
    """pdfplumber text extraction; falls back to OCR.space on scanned PDFs."""
    try:
        import pdfplumber
        text_parts = []
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
        text = "\n".join(text_parts)
        # If almost no text, it's probably a scanned PDF → OCR each page
        if len(text.strip()) < 50:
            logger.info("pdfplumber returned little text; falling back to OCR")
            text = _ocr_pdf_pages(path)
        return text
    except ImportError:
        logger.error("pdfplumber not installed. Run: pip install pdfplumber")
        raise


def _ocr_pdf_pages(path: str) -> str:
    """Convert scanned PDF to text using OCR.space."""
    return ocr_space_extract_text(path)


# ── public API ─────────────────────────────────────────────────────────────────

def extract_invoice_data(file_path: str) -> Dict[str, Any]:
    """
    Extract structured fields from an invoice file.

    Returns:
        {
            "success":    bool,
            "data":       dict of extracted fields,
            "confidence": float 0–1,
            "error":      str (only when success=False),
            "raw_text":   str (OCR/parsed text for debugging)
        }
    """
    path = Path(file_path)
    if not path.exists():
        return {"success": False, "error": f"File not found: {file_path}", "data": {}}

    ext = path.suffix.lower()
    logger.info(f"Extracting invoice from {path.name} (type={ext})")

    try:
        if ext == ".txt":
            raw_text = _read_txt(file_path)
        elif ext in {".jpg", ".jpeg", ".png"}:
            raw_text = _read_image_ocr(file_path)
        elif ext == ".pdf":
            raw_text = _read_pdf(file_path)
        else:
            return {
                "success": False,
                "error": f"Unsupported format: {ext}",
                "data": {},
            }

        if not raw_text or not raw_text.strip():
            return {
                "success": False,
                "error": "No text could be extracted from the file",
                "data": {},
            }

        data = _extract_from_text(raw_text)
        confidence = _confidence(data)

        logger.info(
            f"Invoice extraction complete for {path.name}: "
            f"confidence={confidence}, invoice_number={data.get('invoice_number')}"
        )

        return {
            "success":    True,
            "data":       data,
            "confidence": confidence,
            "raw_text":   raw_text[:2000],  # truncated for storage
        }

    except Exception as e:
        logger.error(f"Invoice extraction failed for {file_path}: {e}", exc_info=True)
        return {"success": False, "error": str(e), "data": {}}