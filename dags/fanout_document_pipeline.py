"""
Document Pipeline Monitoring DAG
=================================
Runs daily at 06:00 to:
  1. Check database connectivity.
  2. Reconcile stale package statuses.
  3. Report Kafka topic lag.
  4. Generate a document-count summary report.
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

DEFAULT_ARGS = {
    "owner":           "data-engineering",
    "retries":         1,
    "retry_delay":     timedelta(minutes=10),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry":  False,
}

dag = DAG(
    dag_id="document_pipeline_monitoring",
    default_args=DEFAULT_ARGS,
    description="Daily health check, package reconciliation, and Kafka lag report",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["monitoring"],
)


def health_check(**context):
    import psycopg2
    from app.config import DB_CONNECTION_STRING
    conn = psycopg2.connect(DB_CONNECTION_STRING)
    conn.close()
    logger.info("Health check passed.")
    context["task_instance"].xcom_push(key="db_status", value="healthy")


def reconcile_packages(**context):
    """Re-compute status for any package that is still PENDING/PROCESSING."""
    import psycopg2
    from app.config import DB_CONNECTION_STRING
    from app.persistence.db_writer import DocumentManager

    db = DocumentManager()
    conn = psycopg2.connect(DB_CONNECTION_STRING)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT package_id FROM document_packages "
                "WHERE status IN ('PENDING', 'PROCESSING', 'PARTIAL_SUCCESS')"
            )
            package_ids = [str(row[0]) for row in cur.fetchall()]
    finally:
        conn.close()

    for pid in package_ids:
        db.update_package_status(pid)
        logger.info("Reconciled package %s", pid)

    context["task_instance"].xcom_push(key="packages_reconciled", value=len(package_ids))


def check_kafka_lag(**context):
    from app.monitoring.kafka_lag import check_kafka_lag as _lag
    _lag(**context)


def generate_report(**context):
    import psycopg2
    import psycopg2.extras
    from app.config import DB_CONNECTION_STRING

    conn = psycopg2.connect(DB_CONNECTION_STRING)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT document_type, status, COUNT(*) AS cnt "
                "FROM documents GROUP BY document_type, status ORDER BY document_type, status"
            )
            summary = [dict(r) for r in cur.fetchall()]

            cur.execute("SELECT COUNT(*) AS total FROM document_packages")
            total_packages = cur.fetchone()["total"]
    finally:
        conn.close()

    report = {
        "document_summary": summary,
        "total_packages":   total_packages,
        "timestamp":        datetime.now(timezone.utc).isoformat(),
    }
    logger.info("Monitoring report: %s", report)
    context["task_instance"].xcom_push(key="monitoring_report", value=report)


start = EmptyOperator(task_id="start", dag=dag)
end   = EmptyOperator(task_id="end",   dag=dag)

t_health    = PythonOperator(task_id="health_check",       python_callable=health_check,    dag=dag)
t_reconcile = PythonOperator(task_id="reconcile_packages", python_callable=reconcile_packages, dag=dag)
t_lag       = PythonOperator(task_id="check_kafka_lag",    python_callable=check_kafka_lag, dag=dag)
t_report    = PythonOperator(task_id="generate_report",    python_callable=generate_report, dag=dag)

start >> t_health >> t_reconcile >> t_lag >> t_report >> end
