def validate_transactions(transactions):

    errors = []

    if len(transactions) == 0:
        errors.append("No transactions found")

    for txn in transactions:

        if txn["amount"] == 0:
            errors.append("Invalid amount")

        if txn["type"] not in ["DEBIT", "CREDIT"]:
            errors.append("Invalid transaction type")

    return errors