import psycopg2
import uuid
from datetime import datetime


def insert_log(document_id, stage, status, message):

    conn = psycopg2.connect(
        host="postgres",
        database="documentdb",
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()

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
            stage,
            status,
            message,
            datetime.now()
        )
    )

    conn.commit()