"""
Bank Statement Extractor
Handles the CSV column format:
    date  DrCr  amount  balance  mode  name  Day  Month  Year  Tday

Also handles HDFC / ICICI / Axis / SBI / Kotak multi-bank variations
(different header capitalisations / orderings) via fuzzy column matching.

PDF bank statements are handled via pdfplumber text + table extraction.

Extracted top-level fields
──────────────────────────
bank_name, account_number, account_holder,
statement_period_from, statement_period_to,
opening_balance, closing_balance,
total_debits, total_credits, transaction_count, currency,
transactions: list[dict]

Each transaction dict
─────────────────────
transaction_date, value_date, description,
transaction_type (DR/CR), amount, running_balance,
reference_number, mode
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ── column name normaliser ─────────────────────────────────────────────────────

# Maps whatever the CSV header says -> our canonical column name
_COL_MAP = {
    # date variants
    "date":          "date",
    "txn date":      "date",
    "transaction date": "date",
    "trans date":    "date",
    "tran date":     "date",

    # debit/credit indicator
    "drcr":          "drcr",
    "dr/cr":         "drcr",
    "dr cr":         "drcr",
    "type":          "drcr",
    "txn type":      "drcr",

    # amount
    "amount":        "amount",
    "txn amount":    "amount",
    "transaction amount": "amount",
    "debit":         "debit",
    "withdrawal":    "debit",
    "withdrawal amt(inr)": "debit",
    "credit":        "credit",
    "deposit":       "credit",
    "deposit amt(inr)": "credit",

    # balance
    "balance":       "balance",
    "closing balance": "balance",
    "running balance": "balance",
    "balance(inr)":  "balance",

    # description / narration
    "name":          "description",
    "narration":     "description",
    "description":   "description",
    "particulars":   "description",
    "transaction details": "description",
    "details":       "description",
    "remarks":       "description",

    # mode / channel
    "mode":          "mode",
    "payment mode":  "mode",
    "channel":       "mode",

    # reference
    "chq no":        "reference",
    "chq/ref no":    "reference",
    "reference no":  "reference",
    "ref no":        "reference",
    "utr":           "reference",
    "cheque number": "reference",

    # convenience date parts 
    "day":   "day",
    "month": "month",
    "year":  "year",
    "tday":  "tday",
}

def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename DataFrame columns using _COL_MAP (case-insensitive, strip spaces)."""
    mapping = {}
    for col in df.columns:
        key = col.strip().lower()
        if key in _COL_MAP:
            mapping[col] = _COL_MAP[key]
    df = df.rename(columns=mapping)
    return df


# ── date parser ────────────────────────────────────────────────────────────────

_DATE_FORMATS = [
    "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y",
    "%d-%m-%y", "%d %b %Y", "%d %B %Y", "%d-%b-%Y",
    "%d/%b/%Y", "%b %d, %Y",
]


def _parse_date(val) -> Optional[str]:
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Try pandas
    try:
        return pd.to_datetime(s, dayfirst=True).strftime("%Y-%m-%d")
    except Exception:
        return s  # return as-is rather than None


# ── amount helpers ─────────────────────────────────────────────────────────────

def _to_float(val) -> Optional[float]:
    if pd.isna(val) or val is None or str(val).strip() in ("", "-", "nil", "NIL"):
        return None
    try:
        return float(str(val).replace(",", "").replace("₹", "").strip())
    except ValueError:
        return None


# ── bank name detection ────────────────────────────────────────────────────────

_BANK_PATTERNS = {
    "HDFC Bank":  re.compile(r"hdfc", re.IGNORECASE),
    "ICICI Bank": re.compile(r"icici", re.IGNORECASE),
    "SBI":        re.compile(r"sbi|state bank", re.IGNORECASE),
    "Axis Bank":  re.compile(r"axis", re.IGNORECASE),
    "Kotak Bank": re.compile(r"kotak", re.IGNORECASE),
}


def _detect_bank(file_path: str, text: str = "") -> str:
    """Detect bank name from filename or header text."""
    combined = (Path(file_path).name + " " + text).lower()
    for bank, pat in _BANK_PATTERNS.items():
        if pat.search(combined):
            return bank
    return "Unknown Bank"


# ── CSV parser ─────────────────────────────────────────────────────────────────

def _parse_csv(file_path: str) -> Dict[str, Any]:
    """
    Parse bank statement CSV with flexible column mapping.
    Handles your exact format: date DrCr amount balance mode name Day Month Year Tday
    """
    try:
        df = pd.read_csv(file_path, dtype=str)
    except Exception as e:
        raise ValueError(f"Could not read CSV {file_path}: {e}")

    df = _normalise_columns(df)
    df = df.dropna(how="all")

    transactions: List[Dict] = []
    total_debits = 0.0
    total_credits = 0.0
    opening_balance: Optional[float] = None
    closing_balance: Optional[float] = None

    has_drcr   = "drcr" in df.columns
    has_debit  = "debit" in df.columns
    has_credit = "credit" in df.columns
    has_amount = "amount" in df.columns

    for _, row in df.iterrows():
        # ── date ──
        raw_date = row.get("date")
        # Fall back to Day/Month/Year columns if present (your format)
        if (pd.isna(raw_date) or not str(raw_date).strip()) and "day" in row:
            try:
                d = int(float(str(row.get("day", "1")).strip()))
                m = int(float(str(row.get("month", "1")).strip()))
                y = int(float(str(row.get("year", "2000")).strip()))
                raw_date = f"{d:02d}/{m:02d}/{y}"
            except (ValueError, TypeError):
                raw_date = None

        txn_date = _parse_date(raw_date)

        # ── amount + direction ──
        amount: Optional[float] = None
        txn_type: Optional[str] = None

        if has_drcr and has_amount:
            amount = _to_float(row.get("amount"))
            drcr   = str(row.get("drcr", "")).strip().upper()
            txn_type = "DR" if drcr in ("DR", "D", "DEBIT", "WITHDRAWAL") else "CR"

        elif has_debit and has_credit:
            debit_val  = _to_float(row.get("debit"))
            credit_val = _to_float(row.get("credit"))
            if debit_val and debit_val > 0:
                amount   = debit_val
                txn_type = "DR"
            elif credit_val and credit_val > 0:
                amount   = credit_val
                txn_type = "CR"

        elif has_amount:
            amount = _to_float(row.get("amount"))
            txn_type = "DR"  # default if no direction indicator

        if amount is None or amount == 0:
            continue   # skip header-like or empty rows

        # ── running balance ──
        balance = _to_float(row.get("balance"))

        if opening_balance is None:
            opening_balance = balance
        closing_balance = balance

        # ── totals ──
        if txn_type == "DR":
            total_debits += amount
        else:
            total_credits += amount

        transactions.append({
            "transaction_date": txn_date,
            "value_date":       txn_date,         # same unless separate col exists
            "description":      str(row.get("description", "")).strip()[:500] or None,
            "transaction_type": txn_type,
            "amount":           round(amount, 2),
            "running_balance":  round(balance, 2) if balance is not None else None,
            "reference_number": str(row.get("reference", "")).strip()[:100] or None,
            "mode":             str(row.get("mode", "")).strip()[:50] or None,
        })

    period_dates = [t["transaction_date"] for t in transactions if t["transaction_date"]]
    period_from  = min(period_dates) if period_dates else None
    period_to    = max(period_dates) if period_dates else None

    return {
        "bank_name":             _detect_bank(file_path),
        "account_number":        None,           # not present in CSV format
        "account_holder":        None,
        "statement_period_from": period_from,
        "statement_period_to":   period_to,
        "opening_balance":       round(opening_balance, 2) if opening_balance else None,
        "closing_balance":       round(closing_balance, 2) if closing_balance else None,
        "total_debits":          round(total_debits, 2),
        "total_credits":         round(total_credits, 2),
        "transaction_count":     len(transactions),
        "currency":              "INR",
        "transactions":          transactions,
    }


# ── PDF parser ─────────────────────────────────────────────────────────────────

def _parse_pdf(file_path: str) -> Dict[str, Any]:
    """Extract bank statement data from a PDF using pdfplumber."""
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("pdfplumber not installed. Run: pip install pdfplumber")

    all_text  = []
    all_rows: List[List] = []

    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                all_text.append(t)
            tables = page.extract_tables()
            for table in (tables or []):
                all_rows.extend(table)

    full_text = "\n".join(all_text)
    bank_name = _detect_bank(file_path, full_text)

    # Try to find account number
    acc_match = re.search(r"account\s*(?:no|number)[:\s]+(\d[\d\s]{8,18}\d)", full_text, re.IGNORECASE)
    account_number = acc_match.group(1).replace(" ", "") if acc_match else None

    # Build DataFrame from extracted table rows
    if not all_rows:
        logger.warning("No table rows found in PDF bank statement")
        return {
            "bank_name": bank_name, "account_number": account_number,
            "account_holder": None, "statement_period_from": None,
            "statement_period_to": None, "opening_balance": None,
            "closing_balance": None, "total_debits": 0.0,
            "total_credits": 0.0, "transaction_count": 0,
            "currency": "INR", "transactions": [],
        }

    # First row is usually the header
    headers = [str(h).strip() if h else "" for h in all_rows[0]]
    data_rows = all_rows[1:]
    df = pd.DataFrame(data_rows, columns=headers, dtype=str)
    df = _normalise_columns(df)

    # Re-use CSV logic now that we have a normalised DataFrame
    # (write to temp CSV and re-parse — simple and reliable)
    import tempfile, os
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name
    try:
        result = _parse_csv(tmp_path)
    finally:
        os.unlink(tmp_path)

    result["bank_name"]      = bank_name
    result["account_number"] = account_number
    return result


# ── confidence scorer ──────────────────────────────────────────────────────────

def _confidence(data: Dict[str, Any]) -> float:
    key_fields = ["bank_name", "statement_period_from", "statement_period_to",
                  "transaction_count", "closing_balance"]
    filled = sum(1 for f in key_fields if data.get(f))
    base   = filled / len(key_fields)
    # Bonus if we have actual transactions
    if data.get("transaction_count", 0) > 0:
        base = min(1.0, base + 0.1)
    return round(base, 2)


# ── public API ─────────────────────────────────────────────────────────────────

def extract_bank_statement_data(file_path: str) -> Dict[str, Any]:
    """
    Extract structured data from a bank statement file.

    Returns:
        {
            "success":    bool,
            "data":       dict (see module docstring),
            "confidence": float 0–1,
            "error":      str (only when success=False)
        }
    """
    path = Path(file_path)
    if not path.exists():
        return {"success": False, "error": f"File not found: {file_path}", "data": {}}

    ext = path.suffix.lower()
    logger.info(f"Extracting bank statement from {path.name} (type={ext})")

    try:
        if ext == ".csv":
            data = _parse_csv(file_path)
        elif ext == ".pdf":
            data = _parse_pdf(file_path)
        elif ext in {".xlsx", ".xls"}:
            # Read Excel → write temp CSV → reuse CSV parser
            import tempfile, os
            df = pd.read_excel(file_path, dtype=str)
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
                df.to_csv(tmp.name, index=False)
                tmp_path = tmp.name
            try:
                data = _parse_csv(tmp_path)
            finally:
                os.unlink(tmp_path)
            data["bank_name"] = _detect_bank(file_path)
        else:
            return {"success": False, "error": f"Unsupported format: {ext}", "data": {}}

        if not data.get("transactions"):
            logger.warning(f"No transactions extracted from {path.name}")

        confidence = _confidence(data)
        logger.info(
            f"Bank statement extraction complete for {path.name}: "
            f"confidence={confidence}, transactions={data.get('transaction_count', 0)}"
        )

        return {"success": True, "data": data, "confidence": confidence}

    except Exception as e:
        logger.error(f"Bank statement extraction failed for {file_path}: {e}", exc_info=True)
        return {"success": False, "error": str(e), "data": {}}