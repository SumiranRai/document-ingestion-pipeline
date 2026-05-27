import psycopg2
import uuid
from datetime import datetime


def save_invoice_data(parsed_data, file_path):

    conn = psycopg2.connect(
        host="postgres",
        database="documentdb",
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()

    document_id = str(uuid.uuid4())
    invoice_id = str(uuid.uuid4())

    # Insert into documents table
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
            "invoice",
            file_path.split("/")[-1],
            file_path,
            "local_upload",
            "processed",
            datetime.now()
        )
    )

    # Insert invoice fields
    cursor.execute(
        """
        INSERT INTO invoice_data (
            invoice_id,
            document_id,
            invoice_number,
            vendor_name,
            invoice_date,
            total_amount
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            invoice_id,
            document_id,
            parsed_data.get("invoice_number"),
            parsed_data.get("vendor_name"),
            parsed_data.get("invoice_date"),
            parsed_data.get("total_amount")
        )
    )

    cursor.execute(
        """
        INSERT INTO processing_logs (
            log_id,
            document_id,
            stage,
            status,
            message,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            str(uuid.uuid4()),
            document_id,
            "DB_SAVE",
            "SUCCESS",
            "Invoice saved successfully",
            datetime.now()
        )
    )
    conn.commit()

    cursor.close()
    conn.close()

    print("Invoice saved successfully")