"""
Document Ingestion Pipeline DAG
================================
Implements a fan-out / join pattern:

  start
    └─ health_check
         └─ scan_and_publish   (publishes to Kafka, creates DB records)
               ├─ process_bank_statements  ─┐
               ├─ process_invoices          ├─ (parallel fan-out)
               └─ process_id_documents     ─┘
                        └─ aggregate_results  (join + package status update)
                               └─ end

Each processing task also publishes Kafka events at every stage boundary
(extracted → validated → persisted / failed).
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

# Ensure the project root is on sys.path so `app.*` is importable inside Airflow
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ── DAG defaults ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":           "data-engineering",
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry":  False,
}

dag = DAG(
    dag_id="document_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingest documents: fan-out by type, join to aggregate results",
    schedule_interval="0 2 * * *",   # daily at 02:00
    start_date=days_ago(1),
    catchup=False,
    tags=["document-ingestion", "kafka"],
)

# ── Task callables ─────────────────────────────────────────────────────────────

def health_check(**context):
    """Verify pipeline database is reachable."""
    import psycopg2
    from app.config import DB_CONNECTION_STRING
    conn = psycopg2.connect(DB_CONNECTION_STRING)
    conn.close()
    logger.info("Health check passed — pipeline DB is reachable.")


def scan_and_publish(**context):
    """
    Scan the document directory, create a package record, publish
    DOCUMENT_RECEIVED events to Kafka for every file found.
    """
    from app.ingestion.ingestion_service import DocumentIngestionService

    ingestion_path = os.getenv("DOCUMENT_INGESTION_PATH", "/opt/airflow/sample")
    pkg_name = f"airflow_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    service = DocumentIngestionService(ingestion_path)
    try:
        package_id = service.scan_and_publish_documents(
            directory=ingestion_path,
            package_name=pkg_name,
            source="airflow_scan",
        )
    finally:
        service.close()

    logger.info("Scan complete — package_id=%s", package_id)
    context["task_instance"].xcom_push(key="package_id", value=package_id)
    return package_id


def process_document_type(doc_type: str, **context):
    """
    Fan-out task: extract → validate → persist every document of `doc_type`
    that belongs to the current package and has status=RECEIVED.
    Publishes a Kafka event after each stage.
    """
    import psycopg2
    import psycopg2.extras
    from kafka import KafkaProducer
    from app.config import DB_CONNECTION_STRING, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS
    from app.extraction.bank_statement_extractor import extract_bank_statement_data
    from app.extraction.id_document_extractor import extract_id_document_data
    from app.extraction.invoice_extractor import extract_invoice_data
    from app.persistence.db_writer import DocumentManager
    from app.validation.validator import validate_extracted_data

    package_id = context["task_instance"].xcom_pull(
        task_ids="scan_and_publish", key="package_id"
    )
    if not package_id:
        logger.warning("No package_id in XCom — nothing to process for %s", doc_type)
        return {"doc_type": doc_type, "processed": 0, "failed": 0}

    db = DocumentManager()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        acks="all",
    )

    conn = psycopg2.connect(DB_CONNECTION_STRING)
    processed = failed = 0

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT document_id, file_path
                FROM   documents
                WHERE  package_id = %s
                  AND  document_type = %s
                  AND  status IN ('RECEIVED')
                """,
                (package_id, doc_type),
            )
            docs = cur.fetchall()
    finally:
        conn.close()

    def _publish_and_log(topic_key: str, event: dict, doc_id: str):
        """Send to Kafka and immediately log to kafka_events for audit."""
        try:
            producer.send(KAFKA_TOPICS[topic_key], key=doc_id, value=event)
            db.log_kafka_event(
                topic=KAFKA_TOPICS[topic_key],
                message_key=doc_id,
                message_value=event,
                event_type=event.get("event_type", topic_key.upper()),
                document_id=doc_id,
            )
        except Exception as exc:
            logger.warning("Kafka publish failed for %s on %s: %s", doc_id, topic_key, exc)

    for doc in docs:
        document_id = str(doc["document_id"])
        file_path   = doc["file_path"]
        retry_count = 0  # tracked in Kafka event payload by DLQ consumer

        try:
            db.update_document_status(document_id, "PROCESSING")
            db.update_extraction_status(document_id, "IN_PROGRESS", started=True)

            # ── Extract ───────────────────────────────────────────────────────
            if doc_type == "invoice":
                result = extract_invoice_data(file_path)
                time.sleep(1)          # brief throttle; paid key has higher limits
            elif doc_type == "bank_statement":
                result = extract_bank_statement_data(file_path)
            elif doc_type == "id_document":
                result = extract_id_document_data(file_path)
                time.sleep(1)
            else:
                raise ValueError(f"Unknown doc_type: {doc_type}")

            if not result["success"]:
                raise RuntimeError(result.get("error", "Extraction failed"))

            extracted  = result["data"]
            confidence = result.get("confidence", 0.0)

            _publish_and_log("extracted", {
                "event_type":    "DOCUMENT_EXTRACTED",
                "document_id":   document_id,
                "package_id":    package_id,
                "document_type": doc_type,
                "confidence":    confidence,
                "retry_count":   retry_count,
                "timestamp":     datetime.now(timezone.utc).isoformat(),
            }, document_id)

            # ── Validate ──────────────────────────────────────────────────────
            validation = validate_extracted_data(doc_type, extracted)
            if not validation["valid"]:
                raise RuntimeError(f"Validation failed: {validation['errors']}")

            _publish_and_log("validated", {
                "event_type":    "DOCUMENT_VALIDATED",
                "document_id":   document_id,
                "package_id":    package_id,
                "document_type": doc_type,
                "retry_count":   retry_count,
                "timestamp":     datetime.now(timezone.utc).isoformat(),
            }, document_id)

            # ── Persist ───────────────────────────────────────────────────────
            if doc_type == "invoice":
                db.save_invoice(document_id, {**extracted, "extraction_confidence": confidence})

            elif doc_type == "bank_statement":
                data = extracted.copy()
                txns = data.pop("transactions", [])
                stmt_id = db.save_bank_statement(document_id, {**data, "transaction_count": len(txns)})
                if txns:
                    db.save_transactions(str(stmt_id), txns)

            elif doc_type == "id_document":
                db.save_id_document(document_id, {**extracted, "extraction_confidence": confidence})

            db.update_document_status(document_id, "PERSISTED")
            db.update_extraction_status(document_id, "COMPLETED", completed=True)
            db.log_processing_event(
                document_id=document_id,
                package_id=package_id,
                stage="FULL_PIPELINE",
                status="SUCCESS",
                message=f"Processed {doc_type} confidence={confidence:.2f}",
            )

            _publish_and_log("persisted", {
                "event_type":    "DOCUMENT_PERSISTED",
                "document_id":   document_id,
                "package_id":    package_id,
                "document_type": doc_type,
                "confidence":    confidence,
                "retry_count":   retry_count,
                "timestamp":     datetime.now(timezone.utc).isoformat(),
            }, document_id)

            processed += 1
            logger.info("OK  %s  %s  confidence=%.2f", doc_type, document_id, confidence)

        except Exception as exc:
            logger.error("FAIL %s %s: %s", doc_type, document_id, exc, exc_info=True)
            try:
                db.update_document_status(document_id, "FAILED")
                db.update_extraction_status(document_id, "FAILED")
                db.log_processing_event(
                    document_id=document_id,
                    package_id=package_id,
                    stage="FULL_PIPELINE",
                    status="FAILED",
                    error_details=str(exc),
                )
                _publish_and_log("failed", {
                    "event_type":  "DOCUMENT_FAILED",
                    "document_id": document_id,
                    "package_id":  package_id,
                    "document_type": doc_type,
                    "error":       str(exc),
                    "retry_count": retry_count,
                    "timestamp":   datetime.now(timezone.utc).isoformat(),
                }, document_id)
            except Exception:
                pass
            failed += 1

    producer.flush()
    producer.close()

    logger.info("%s complete — processed=%d  failed=%d", doc_type, processed, failed)
    return {"doc_type": doc_type, "processed": processed, "failed": failed}


def aggregate_results(**context):
    """
    Join step: pull XCom results from all three processing tasks,
    update the package status, and push a consolidated report.
    """
    from app.persistence.db_writer import DocumentManager

    package_id = context["task_instance"].xcom_pull(
        task_ids="scan_and_publish", key="package_id"
    )

    bank_result     = context["task_instance"].xcom_pull(task_ids="process_bank_statements") or {}
    invoice_result  = context["task_instance"].xcom_pull(task_ids="process_invoices")        or {}
    id_result       = context["task_instance"].xcom_pull(task_ids="process_id_documents")    or {}

    total_processed = sum(r.get("processed", 0) for r in [bank_result, invoice_result, id_result])
    total_failed    = sum(r.get("failed",    0) for r in [bank_result, invoice_result, id_result])

    db = DocumentManager()
    if package_id:
        db.update_package_status(package_id)

    report = {
        "package_id":     package_id,
        "total_processed": total_processed,
        "total_failed":    total_failed,
        "by_type": {
            "bank_statement": bank_result,
            "invoice":        invoice_result,
            "id_document":    id_result,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    logger.info("Pipeline run complete: %s", report)
    context["task_instance"].xcom_push(key="pipeline_report", value=report)
    return report


# ── Task definitions ──────────────────────────────────────────────────────────

start = EmptyOperator(task_id="start", dag=dag)
end   = EmptyOperator(task_id="end",   dag=dag)

t_health  = PythonOperator(task_id="health_check",    python_callable=health_check,   dag=dag)
t_scan    = PythonOperator(task_id="scan_and_publish", python_callable=scan_and_publish, dag=dag)

t_banks   = PythonOperator(
    task_id="process_bank_statements",
    python_callable=process_document_type,
    op_kwargs={"doc_type": "bank_statement"},
    dag=dag,
)
t_invoices = PythonOperator(
    task_id="process_invoices",
    python_callable=process_document_type,
    op_kwargs={"doc_type": "invoice"},
    dag=dag,
)
t_ids = PythonOperator(
    task_id="process_id_documents",
    python_callable=process_document_type,
    op_kwargs={"doc_type": "id_document"},
    dag=dag,
)

t_aggregate = PythonOperator(
    task_id="aggregate_results",
    python_callable=aggregate_results,
    dag=dag,
)

# ── Fan-out → join dependency graph ──────────────────────────────────────────
start >> t_health >> t_scan >> [t_banks, t_invoices, t_ids] >> t_aggregate >> end
