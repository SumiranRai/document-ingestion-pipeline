import pandas as pd


def extract_bank_transactions(file_path):

    df = pd.read_csv(file_path)

    transactions = []

    for _, row in df.iterrows():

        transaction = {
            "transaction_date": row["date"],
            "description": row["description"],
            "amount": float(row["amount"]),
            "balance": float(row["balance"])
        }

        transactions.append(transaction)

    return transactions


if __name__ == "__main__":

    file_path = "sample_documents/bank_statements/bank1.csv"

    transactions = extract_bank_transactions(file_path)

    print("\n===== BANK TRANSACTIONS =====\n")

    for t in transactions:
        print(t)