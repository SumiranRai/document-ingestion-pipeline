from app.extraction.bank_statement_extractor import (
    extract_bank_transactions
)

from app.persistence.bank_db_writer import (
    save_bank_transactions
)

file_path = "sample_documents/bank_statements/bank1.csv"

transactions = extract_bank_transactions(file_path)

print(transactions)

save_bank_transactions(transactions, file_path)