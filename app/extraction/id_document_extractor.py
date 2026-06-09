"""
ID Document Extractor
Handles:
  - PAN Card: Extract PAN number, holder name
  - Aadhaar Card: Extract Aadhaar number, holder name, DOB, gender

Input formats:
    - .jpg / .png / .jpeg → OCR.space OCR then regex parsing
    - .pdf → pdfplumber text extraction then OCR.space on scanned PDFs

Extracted fields
────────────────
For PAN:
  - id_type: "PAN"
  - pan_number, holder_name, father_name, dob, gender

For Aadhaar:
  - id_type: "AADHAAR"
  - aadhaar_number, holder_name, dob, gender, address

Common:
  - id_type, holder_name, document_date, extraction_method
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

from app.extraction.ocr_space import ocr_space_extract_text


# ── Regex Patterns ─────────────────────────────────────────────────────────────

# PAN format: AAAPL5055K
_PAN_PATTERN = re.compile(r"\b([A-Z]{5}[0-9]{4}[A-Z]{1})\b")

# Aadhaar format: 1234 5678 9012 or 12345678901234
_AADHAAR_PATTERN = re.compile(r"(?:\d{4}\s?){3}\d{4}|\d{12}")

# Name patterns (4+ characters, mostly alphabetic + spaces)
_NAME_PATTERN = re.compile(
    r"(?:name|naam|नाम)[:\s]*([A-Za-z\s\.]+?)(?:\n|father|father's|पिता|dob|date|$)",
    re.IGNORECASE | re.MULTILINE,
)

_FATHER_NAME_PATTERN = re.compile(
    r"(?:father's?\s*name|पिता का नाम)[:\s]*([A-Za-z\s\.]+?)(?:\n|dob|date|$)",
    re.IGNORECASE,
)

# Date patterns (DD/MM/YYYY, DD-MM-YYYY, etc.)
_DOB_PATTERN = re.compile(
    r"(?:d\.o\.b|dob|date\s*of\s*birth|जन्म\s*तिथि)[:\s]*(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4})",
    re.IGNORECASE,
)

# Gender patterns
_GENDER_PATTERN = re.compile(r"\b(male|female|m|f|महिला|पुरुष|man|woman)\b", re.IGNORECASE)

# Address (capture multi-line, up to 200 chars)
_ADDRESS_PATTERN = re.compile(
    r"(?:address|पता)[:\s]*([A-Za-z0-9\s,\-\.]+?)(?:\n\n|$)",
    re.IGNORECASE | re.MULTILINE,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_date(raw: str) -> Optional[str]:
    """Normalise various date formats to YYYY-MM-DD."""
    if not raw:
        return None
    
    formats = [
        "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y",
        "%d/%m/%y", "%d-%m-%y", "%d.%m.%y",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    
    return raw.strip()


def _normalize_name(raw: str) -> Optional[str]:
    """Clean and normalize a name."""
    if not raw:
        return None
    
    name = raw.strip()
    # Remove extra spaces, keep only alphanumeric + spaces
    name = re.sub(r"[^\w\s\.]", "", name, flags=re.UNICODE)
    name = re.sub(r"\s+", " ", name).strip()
    
    if len(name) < 2:
        return None
    
    return name[:200]  # cap at 200 chars


def _normalize_aadhaar(raw: str) -> Optional[str]:
    """Extract and normalize 12-digit Aadhaar number."""
    if not raw:
        return None
    
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 12:
        return digits
    
    return None


def _normalize_pan(raw: str) -> Optional[str]:
    """Ensure PAN is uppercase 10-char format."""
    if not raw:
        return None
    
    pan = raw.strip().upper()
    if re.match(r"^[A-Z]{5}[0-9]{4}[A-Z]$", pan):
        return pan
    
    return None


def _gender_from_text(text: str) -> Optional[str]:
    """Extract gender from text."""
    m = _GENDER_PATTERN.search(text)
    if not m:
        return None
    
    val = m.group(1).lower()
    if val in ("male", "m", "पुरुष", "man"):
        return "M"
    elif val in ("female", "f", "महिला", "woman"):
        return "F"
    
    return None


# ── Text extraction from images/PDFs ────────────────────────────────────────────

def _read_image_ocr(path: str) -> str:
    """OCR.space OCR on JPEG / PNG."""
    return ocr_space_extract_text(path)


def _read_pdf_with_ocr(path: str) -> str:
    """Extract text from PDF; if mostly scanned, OCR each page."""
    try:
        import pdfplumber
        
        text_parts = []
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
        
        text = "\n".join(text_parts)
        
        # If almost no text, it's a scanned PDF → OCR
        if len(text.strip()) < 50:
            logger.info("pdfplumber returned little text; falling back to OCR")
            text = _ocr_pdf_pages(path)
        
        return text
    except ImportError as e:
        logger.error(f"pdfplumber not installed: {e}")
        raise


def _ocr_pdf_pages(path: str) -> str:
    """Convert scanned PDF to text using OCR.space."""
    return ocr_space_extract_text(path)


# ── Extraction logic ───────────────────────────────────────────────────────────

def _extract_pan_data(text: str) -> Dict[str, Any]:
    """Extract PAN card data from OCR'd text."""
    data: Dict[str, Any] = {}
    
    # Extract PAN number
    pan_match = _PAN_PATTERN.search(text)
    data["pan_number"] = _normalize_pan(pan_match.group(0)) if pan_match else None
    
    # Extract name
    name_match = _NAME_PATTERN.search(text)
    data["holder_name"] = _normalize_name(name_match.group(1)) if name_match else None
    
    # Father's name
    father_match = _FATHER_NAME_PATTERN.search(text)
    data["father_name"] = _normalize_name(father_match.group(1)) if father_match else None
    
    # DOB (less common in PAN but may be present)
    dob_match = _DOB_PATTERN.search(text)
    data["dob"] = _parse_date(dob_match.group(1)) if dob_match else None
    
    # Gender
    data["gender"] = _gender_from_text(text)
    
    data["id_type"] = "PAN"
    
    return data


def _extract_aadhaar_data(text: str) -> Dict[str, Any]:
    """Extract Aadhaar card data from OCR'd text."""
    data: Dict[str, Any] = {}
    
    # Extract Aadhaar number
    aadhaar_match = _AADHAAR_PATTERN.search(text)
    data["aadhaar_number"] = _normalize_aadhaar(aadhaar_match.group(0)) if aadhaar_match else None
    
    # Extract name
    name_match = _NAME_PATTERN.search(text)
    data["holder_name"] = _normalize_name(name_match.group(1)) if name_match else None
    
    # DOB
    dob_match = _DOB_PATTERN.search(text)
    data["dob"] = _parse_date(dob_match.group(1)) if dob_match else None
    
    # Gender
    data["gender"] = _gender_from_text(text)
    
    # Address (Aadhaar has address field)
    addr_match = _ADDRESS_PATTERN.search(text)
    data["address"] = _normalize_name(addr_match.group(1)) if addr_match else None
    
    data["id_type"] = "AADHAAR"
    
    return data


def _detect_id_type(text: str) -> str:
    """Detect whether the text is from a PAN or Aadhaar card."""
    text_lower = text.lower()
    
    # Aadhaar-specific keywords
    if any(kw in text_lower for kw in ["aadhaar", "आधार", "uid", "uidai"]):
        return "AADHAAR"
    
    # PAN-specific keywords
    if any(kw in text_lower for kw in ["pan", "पैन", "income tax"]):
        return "PAN"
    
    # Heuristic: if 12-digit number found, likely Aadhaar; 10-digit, likely PAN
    if _AADHAAR_PATTERN.search(text):
        return "AADHAAR"
    
    if _PAN_PATTERN.search(text):
        return "PAN"
    
    # Default: assume Aadhaar (more common)
    return "AADHAAR"


def _confidence(data: Dict[str, Any]) -> float:
    """Score extraction quality 0–1 based on key fields present."""
    id_type = data.get("id_type")
    
    if id_type == "PAN":
        key_fields = ["pan_number", "holder_name"]
    else:  # AADHAAR
        key_fields = ["aadhaar_number", "holder_name"]
    
    filled = sum(1 for f in key_fields if data.get(f))
    return round(filled / len(key_fields), 2)


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_id_document_data(file_path: str) -> Dict[str, Any]:
    """
    Extract structured data from an ID document (PAN or Aadhaar).

    Returns:
        {
            "success":    bool,
            "data":       dict of extracted fields,
            "id_type":    "PAN" or "AADHAAR",
            "confidence": float 0–1,
            "error":      str (only when success=False),
            "raw_text":   str (OCR'd text for debugging)
        }
    """
    path = Path(file_path)
    if not path.exists():
        return {
            "success": False,
            "error": f"File not found: {file_path}",
            "data": {},
            "id_type": None,
        }
    
    ext = path.suffix.lower()
    logger.info(f"Extracting ID document from {path.name} (type={ext})")
    
    try:
        # Extract text based on file type
        if ext in {".jpg", ".jpeg", ".png"}:
            raw_text = _read_image_ocr(file_path)
        elif ext == ".pdf":
            raw_text = _read_pdf_with_ocr(file_path)
        else:
            return {
                "success": False,
                "error": f"Unsupported format: {ext}",
                "data": {},
                "id_type": None,
            }
        
        if not raw_text or not raw_text.strip():
            return {
                "success": False,
                "error": "No text could be extracted from the file",
                "data": {},
                "id_type": None,
            }
        
        # Detect document type
        doc_type = _detect_id_type(raw_text)
        
        # Extract data based on type
        if doc_type == "PAN":
            data = _extract_pan_data(raw_text)
        else:
            data = _extract_aadhaar_data(raw_text)
        
        confidence = _confidence(data)
        
        logger.info(
            f"ID document extraction complete for {path.name}: "
            f"type={doc_type}, confidence={confidence}, "
            f"id={data.get('pan_number') or data.get('aadhaar_number')}"
        )
        
        return {
            "success": True,
            "data": data,
            "id_type": doc_type,
            "confidence": confidence,
            "raw_text": raw_text[:2000],  # truncated for storage
        }
    
    except Exception as e:
        logger.error(f"ID document extraction failed for {file_path}: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "data": {},
            "id_type": None,
        }
