import psycopg2
import uuid
from datetime import datetime


def save_bank_transactions(transactions, file_path):

    conn = psycopg2.connect(
        host="postgres",
        database="documentdb",
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()

    document_id = str(uuid.uuid4())

    # Save document metadata
    cursor.execute(
        """
        INSERT INTO documents (
            document_id,
            document_type,
            file_name,
            file_path,
            source,
            status,
            processed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            document_id,
            "bank_statement",
            file_path.split("/")[-1],
            file_path,
            "local_upload",
            "processed",
            datetime.now()
        )
    )

    # Save transactions
    for transaction in transactions:

        transaction_id = str(uuid.uuid4())

        cursor.execute(
            """
            INSERT INTO bank_transactions (
                transaction_id,
                document_id,
                transaction_date,
                description,
                amount,
                balance
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                transaction_id,
                document_id,
                transaction["transaction_date"],
                transaction["description"],
                transaction["amount"],
                transaction["balance"]
            )
        )

    conn.commit()

    cursor.close()
    conn.close()

    print("Bank transactions saved successfully")