"""
Validation module for extracted data.
Validates invoices, bank statements, and ID documents against business rules.
"""

import re
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class ValidationError:
    """Represents a validation error."""
    def __init__(self, field: str, error: str, severity: str = "error"):
        self.field = field
        self.error = error
        self.severity = severity  # "error", "warning"
    
    def __str__(self):
        return f"{self.severity}: {self.field} - {self.error}"


def validate_invoice_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate extracted invoice data.
    
    Args:
        data: Extracted invoice fields
    
    Returns:
        Dict with validation result
    """
    errors = []
    warnings = []
    
    # Required fields
    required_fields = ["invoice_number", "vendor_name", "invoice_date", "total_amount"]
    
    for field in required_fields:
        if not data.get(field):
            errors.append(ValidationError(field, f"Missing required field: {field}"))
    
    # Validate invoice number format
    invoice_num = data.get("invoice_number")
    if invoice_num and not re.match(r"^[A-Z0-9\-/]+$", invoice_num):
        warnings.append(ValidationError("invoice_number", "Invalid invoice number format"))
    
    # Validate invoice date format
    invoice_date = data.get("invoice_date")
    if invoice_date:
        try:
            # Try parsing common date formats
            for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]:
                try:
                    datetime.strptime(invoice_date, fmt)
                    break
                except ValueError:
                    continue
            else:
                errors.append(ValidationError("invoice_date", f"Invalid date format: {invoice_date}"))
        except Exception as e:
            errors.append(ValidationError("invoice_date", f"Date validation error: {str(e)}"))
    
    # Validate total amount
    total_amount = data.get("total_amount")
    if total_amount is not None:
        try:
            amount = float(total_amount)
            if amount <= 0:
                errors.append(ValidationError("total_amount", "Total amount must be positive"))
            elif amount > 10000000:  # More than 1 crore
                warnings.append(ValidationError("total_amount", "Unusually high invoice amount"))
        except (ValueError, TypeError):
            errors.append(ValidationError("total_amount", "Total amount must be numeric"))
    
    # Validate tax amount if present
    tax_amount = data.get("tax_amount")
    if tax_amount is not None:
        try:
            tax = float(tax_amount)
            if tax < 0:
                errors.append(ValidationError("tax_amount", "Tax amount cannot be negative"))
            if total_amount and tax > total_amount:
                errors.append(ValidationError("tax_amount", "Tax amount cannot exceed total amount"))
        except (ValueError, TypeError):
            warnings.append(ValidationError("tax_amount", "Tax amount is not numeric"))
    
    # Validate GSTIN if present
    vendor_gstin = data.get("vendor_gstin")
    if vendor_gstin:
        if not is_valid_gstin(vendor_gstin):
            warnings.append(ValidationError("vendor_gstin", f"Invalid GSTIN format: {vendor_gstin}"))
    
    # Validate subtotal if all three amounts are present
    subtotal = data.get("subtotal")
    if subtotal and total_amount and tax_amount:
        try:
            if abs(float(subtotal) + float(tax_amount) - float(total_amount)) > 0.01:
                warnings.append(
                    ValidationError("amounts", 
                    "Subtotal + Tax != Total (discrepancy detected)")
                )
        except (ValueError, TypeError):
            pass
    
    result = {
        "valid": len(errors) == 0,
        "errors": [str(e) for e in errors],
        "warnings": [str(w) for w in warnings],
        "error_count": len(errors),
        "warning_count": len(warnings)
    }
    
    logger.info(f"Invoice validation result: {result}")
    return result


def validate_bank_statement_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate extracted bank statement data.
    
    Args:
        data: Extracted statement fields
    
    Returns:
        Dict with validation result
    """
    errors = []
    warnings = []
    
    # Required fields (account_number is not embedded in CSV exports, so it is a warning only)
    required_fields = ["bank_name", "statement_period_from", "statement_period_to"]

    for field in required_fields:
        if not data.get(field):
            errors.append(ValidationError(field, f"Missing required field: {field}"))

    # account_number is optional for CSV-sourced statements
    account_num = data.get("account_number")
    if not account_num:
        warnings.append(ValidationError("account_number", "Account number not found in file", severity="warning"))
    elif not re.match(r"^\d{9,18}$", str(account_num)):
        warnings.append(ValidationError("account_number", "Unexpected account number format", severity="warning"))
    
    # Validate date fields
    for date_field in ["statement_period_from", "statement_period_to"]:
        date_val = data.get(date_field)
        if date_val:
            if not is_valid_date(date_val):
                errors.append(ValidationError(date_field, f"Invalid date format: {date_val}"))
    
    # Validate period dates
    period_from = data.get("statement_period_from")
    period_to = data.get("statement_period_to")
    if period_from and period_to:
        try:
            from_date = parse_date(period_from)
            to_date = parse_date(period_to)
            if from_date > to_date:
                errors.append(ValidationError("period", "Statement period start date is after end date"))
        except:
            pass
    
    # Validate amounts
    opening_bal = data.get("opening_balance")
    closing_bal = data.get("closing_balance")
    
    for bal_field in ["opening_balance", "closing_balance"]:
        val = data.get(bal_field)
        if val is not None:
            try:
                float(val)
            except (ValueError, TypeError):
                errors.append(ValidationError(bal_field, f"{bal_field} must be numeric"))
    
    # Validate transaction totals
    total_debits = data.get("total_debits", 0)
    total_credits = data.get("total_credits", 0)
    
    if total_debits < 0:
        warnings.append(ValidationError("total_debits", "Total debits should not be negative"))
    if total_credits < 0:
        warnings.append(ValidationError("total_credits", "Total credits should not be negative"))
    
    # Validate transaction count
    tx_count = data.get("transaction_count", 0)
    if tx_count == 0:
        warnings.append(ValidationError("transactions", "No transactions found in statement"))
    
    result = {
        "valid": len(errors) == 0,
        "errors": [str(e) for e in errors],
        "warnings": [str(w) for w in warnings],
        "error_count": len(errors),
        "warning_count": len(warnings)
    }
    
    logger.info(f"Bank statement validation result: {result}")
    return result


def validate_id_document_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate extracted ID document data (PAN or Aadhaar).
    
    Args:
        data: Extracted ID document fields
    
    Returns:
        Dict with validation result
    """
    errors = []
    warnings = []
    
    id_type = data.get("id_type")
    
    # Validate id_type
    if not id_type:
        errors.append(ValidationError("id_type", "Document type not detected"))
    elif id_type not in ["PAN", "AADHAAR"]:
        errors.append(ValidationError("id_type", f"Unknown ID type: {id_type}"))
    
    # Common required fields
    holder_name = data.get("holder_name")
    if not holder_name:
        errors.append(ValidationError("holder_name", "Missing holder name"))
    
    # Type-specific validation
    if id_type == "PAN":
        pan_number = data.get("pan_number")
        if not pan_number:
            errors.append(ValidationError("pan_number", "Missing PAN number"))
        elif not is_valid_pan(pan_number):
            errors.append(ValidationError("pan_number", f"Invalid PAN format: {pan_number}"))
    
    elif id_type == "AADHAAR":
        aadhaar_number = data.get("aadhaar_number")
        if not aadhaar_number:
            errors.append(ValidationError("aadhaar_number", "Missing Aadhaar number"))
        elif not is_valid_aadhaar(aadhaar_number):
            errors.append(ValidationError("aadhaar_number", f"Invalid Aadhaar format: {aadhaar_number}"))
    
    # Validate DOB if present
    dob = data.get("dob")
    if dob:
        if not is_valid_date(dob):
            warnings.append(ValidationError("dob", f"Invalid date format: {dob}"))
        else:
            try:
                dob_date = parse_date(dob)
                today = datetime.now()
                age = (today - dob_date).days // 365
                if age < 0:
                    errors.append(ValidationError("dob", "DOB is in the future"))
                elif age > 150:
                    warnings.append(ValidationError("dob", "Unusually high age detected"))
            except:
                pass
    
    # Validate gender if present
    gender = data.get("gender")
    if gender and gender not in ["M", "F", "O"]:
        warnings.append(ValidationError("gender", f"Invalid gender value: {gender}"))
    
    result = {
        "valid": len(errors) == 0,
        "errors": [str(e) for e in errors],
        "warnings": [str(w) for w in warnings],
        "error_count": len(errors),
        "warning_count": len(warnings)
    }
    
    logger.info(f"ID document validation result: {result}")
    return result


def validate_extracted_data(doc_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate extracted data based on document type.
    
    Args:
        doc_type: Document type (invoice, bank_statement, id_document)
        data: Extracted fields
    
    Returns:
        Dict with validation result
    """
    if doc_type == "invoice":
        return validate_invoice_data(data)
    elif doc_type == "bank_statement":
        return validate_bank_statement_data(data)
    elif doc_type == "id_document":
        return validate_id_document_data(data)
    else:
        return {
            "valid": False,
            "errors": [f"Unknown document type: {doc_type}"],
            "warnings": [],
            "error_count": 1,
            "warning_count": 0
        }


def is_valid_gstin(gstin: str) -> bool:
    """Validate GSTIN format (Indian Tax ID)."""
    pattern = r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$"
    return bool(re.match(pattern, gstin))


def is_valid_date(date_str: str) -> bool:
    """Check if string is a valid date."""
    for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y"]:
        try:
            datetime.strptime(date_str, fmt)
            return True
        except ValueError:
            continue
    return False


def parse_date(date_str: str) -> datetime:
    """Parse date string to datetime object."""
    for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y"]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unable to parse date: {date_str}")


def is_valid_pan(pan: str) -> bool:
    """Validate PAN format (Indian tax ID)."""
    # PAN format: AAAPL5055K (5 letters, 4 digits, 1 letter)
    pattern = r"^[A-Z]{5}[0-9]{4}[A-Z]{1}$"
    return bool(re.match(pattern, pan))


def is_valid_aadhaar(aadhaar: str) -> bool:
    """Validate Aadhaar format (12-digit number)."""
    # Aadhaar is 12-digit number
    pattern = r"^\d{12}$"
    return bool(re.match(pattern, aadhaar))
