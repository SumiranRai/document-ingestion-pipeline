"""OCR.space integration helper.

This module centralises OCR.space calls for invoice and ID document extraction.
Includes automatic retry with exponential backoff to handle free-tier rate limits (429).
"""

import logging
import time
from pathlib import Path
import os

import requests

from app.config import OCR_API_KEY, OCR_SPACE_URL, OCR_SPACE_LANGUAGE, OCR_SPACE_OCR_ENGINE

logger = logging.getLogger(__name__)

# Free-tier limit: ~1 request / 3 seconds. We back off 4s, 8s, 16s on 429.
_MAX_RETRIES = 3
_BASE_BACKOFF_SECONDS = 4


def ocr_space_extract_text(file_path: str, language: str = None) -> str:
    """Send a file to OCR.space and return extracted text.

    Retries up to _MAX_RETRIES times on HTTP 429 (Too Many Requests)
    using exponential backoff so the free-tier key is not exhausted.
    """
    api_key = OCR_API_KEY
    if not api_key:
        raise EnvironmentError(
            "OCR.space API key is not configured. Set OCR_API_KEY or OCR_SPACE_API_KEY."
        )

    payload = {
        "apikey": api_key,
        "language": language or OCR_SPACE_LANGUAGE,
        "isOverlayRequired": False,
        "OCREngine": OCR_SPACE_OCR_ENGINE,
    }

    file_name = Path(file_path).name

    for attempt in range(1, _MAX_RETRIES + 1):
        with open(file_path, "rb") as f:
            files = {"file": (file_name, f)}
            response = requests.post(OCR_SPACE_URL, files=files, data=payload, timeout=120)

        if response.status_code == 429:
            wait = _BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))  # 4s, 8s, 16s
            logger.warning(
                f"OCR.space rate limit (429) for {file_name}. "
                f"Attempt {attempt}/{_MAX_RETRIES}. Retrying in {wait}s..."
            )
            time.sleep(wait)
            continue

        try:
            response.raise_for_status()
        except Exception as e:
            logger.error(f"OCR.space request failed for {file_path}: {e}")
            raise

        result = response.json()
        if result.get("IsErroredOnProcessing"):
            error_messages = result.get("ErrorMessage") or result.get("ErrorDetails") or "Unknown OCR.space error"
            if isinstance(error_messages, list):
                error_message = "; ".join(str(msg) for msg in error_messages)
            else:
                error_message = str(error_messages)
            raise RuntimeError(f"OCR.space processing failed: {error_message}")

        parsed_results = result.get("ParsedResults") or []
        if not parsed_results:
            raise RuntimeError("OCR.space returned no ParsedResults")

        text_parts = []
        for parsed in parsed_results:
            parsed_text = parsed.get("ParsedText")
            if parsed_text:
                text_parts.append(parsed_text)

        return "\n".join(text_parts).strip()

    # All retries exhausted
    raise RuntimeError(
        f"OCR.space rate limit persisted after {_MAX_RETRIES} retries for {file_name}. "
        "Consider using a paid API key or reducing request frequency."
    )
