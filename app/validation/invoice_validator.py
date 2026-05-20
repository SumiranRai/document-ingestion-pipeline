def validate_invoice_data(data):

    required_fields = [
        "invoice_number",
        "total_amount"
    ]

    missing_fields = []

    for field in required_fields:

        if not data.get(field):
            missing_fields.append(field)

    if missing_fields:

        return {
            "valid": False,
            "missing_fields": missing_fields
        }

    return {
        "valid": True
    }