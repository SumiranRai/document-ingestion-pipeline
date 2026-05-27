from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


with DAG(
    dag_id="fanout_document_pipeline",
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    start = EmptyOperator(
        task_id="start_pipeline"
    )

    process_invoice = BashOperator(
        task_id="process_invoice",

        bash_command="""
        cd /opt/airflow/project &&
        python test_invoice_pipeline.py
        """
    )

    process_bank_statement = BashOperator(
        task_id="process_bank_statement",

        bash_command="""
        cd /opt/airflow/project &&
        python test_bank_pipeline.py
        """
    )

    combine_results = EmptyOperator(
        task_id="combine_results"
    )

    end = EmptyOperator(
        task_id="end_pipeline"
    )

    start >> [process_invoice, process_bank_statement]

    [process_invoice, process_bank_statement] >> combine_results

    combine_results >> end