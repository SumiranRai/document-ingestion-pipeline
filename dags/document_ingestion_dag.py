from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime


with DAG(
    dag_id="document_ingestion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    run_producer = BashOperator(
        task_id="run_kafka_producer",

        bash_command="""
        export KAFKA_SERVER=kafka:29092 && \
        python3 /opt/airflow/app/kafka/producer.py
        """
    )