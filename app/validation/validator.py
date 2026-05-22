def validate_invoice_data(data):

    errors = []

    required_fields = [
        "invoice_number",
        "vendor_name",
        "invoice_date",
        "total_amount"
    ]

    for field in required_fields:
        if not data.get(field):
            errors.append(f"Missing field: {field}")

    if data.get("total_amount"):
        if data["total_amount"] <= 0:
            errors.append("Invalid total amount")

    return errors