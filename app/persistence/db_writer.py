"""
Database operations for persistence layer.
"""

import json
import logging
from datetime import datetime
from uuid import UUID
from contextlib import contextmanager
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from app.config import DB_CONNECTION_STRING

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages PostgreSQL database connections."""
    
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.conn = None
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            logger.info("Database connection established")
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        try:
            self.connect()
            yield self.conn
        finally:
            self.disconnect()


def execute_query(conn, query, params=None, fetch=False):
    """Execute a database query safely."""
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params or ())
        
        if fetch:
            results = cursor.fetchall()
            cursor.close()
            return results
        else:
            conn.commit()
            cursor.close()
            return True
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise


class DocumentManager:
    """Manages document records in PostgreSQL."""
    
    def __init__(self, connection_string=DB_CONNECTION_STRING):
        self.db = DatabaseConnection(connection_string)
    
    def create_document_package(self, package_name: str, source: str, total_documents: int,
                                package_id: str = None, status: str = 'PENDING'):
        """Create or update a document package record."""
        if package_id:
            query = sql.SQL("""
                INSERT INTO document_packages 
                (package_id, package_name, source, total_documents, status, received_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (package_id) DO UPDATE SET
                    package_name = EXCLUDED.package_name,
                    source = EXCLUDED.source,
                    total_documents = EXCLUDED.total_documents,
                    status = EXCLUDED.status,
                    updated_at = NOW()
                RETURNING package_id
            """)
            params = (package_id, package_name, source, total_documents, status, datetime.now())
        else:
            query = sql.SQL("""
                INSERT INTO document_packages 
                (package_name, source, total_documents, status, received_at)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING package_id
            """)
            params = (package_name, source, total_documents, status, datetime.now())

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchone()
            conn.commit()
            package_id_result = result[0]
            logger.info(f"Created or updated package: {package_id_result}")
            return package_id_result
    
    def create_document_record(self, package_id: str, doc_type: str,
                              file_name: str, file_path: str, source: str,
                              file_size: int = None, document_id: str = None):
        """Create or update a document record."""
        if document_id:
            query = sql.SQL("""
                INSERT INTO documents 
                (document_id, package_id, document_type, file_name, file_path, source, 
                 file_size, status, extraction_status, validation_status, received_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (document_id) DO UPDATE SET
                    package_id = EXCLUDED.package_id,
                    document_type = EXCLUDED.document_type,
                    file_name = EXCLUDED.file_name,
                    file_path = EXCLUDED.file_path,
                    source = EXCLUDED.source,
                    file_size = EXCLUDED.file_size,
                    status = EXCLUDED.status,
                    extraction_status = EXCLUDED.extraction_status,
                    validation_status = EXCLUDED.validation_status,
                    updated_at = NOW()
                RETURNING document_id
            """)
            params = (
                document_id,
                package_id,
                doc_type,
                file_name,
                file_path,
                source,
                file_size,
                'RECEIVED',
                'PENDING',
                'PENDING',
                datetime.now()
            )
        else:
            query = sql.SQL("""
                INSERT INTO documents 
                (package_id, document_type, file_name, file_path, source, 
                 file_size, status, extraction_status, validation_status, received_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING document_id
            """)
            params = (
                package_id,
                doc_type,
                file_name,
                file_path,
                source,
                file_size,
                'RECEIVED',
                'PENDING',
                'PENDING',
                datetime.now()
            )

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchone()
            conn.commit()
            document_id_result = result[0]
            logger.info(f"Created or updated document record: {document_id_result}")
            return document_id_result
    
    def update_document_status(self, document_id: str, status: str):
        """Update document status."""
        query = sql.SQL("""
            UPDATE documents 
            SET status = %s, updated_at = %s
            WHERE document_id = %s
        """)
        
        with self.db.get_connection() as conn:
            execute_query(conn, query, (status, datetime.now(), document_id))
            logger.info(f"Updated document {document_id} status to {status}")

    def mark_document_failed(self, document_id: str):
        """Mark a document as failed for package reconciliation."""
        self.update_document_status(document_id, "FAILED")

    def update_package_status(self, package_id: str):
        """Recompute package status from document statuses."""
        query = sql.SQL("""
            SELECT status, COUNT(*) AS count
            FROM documents
            WHERE package_id = %s
            GROUP BY status
        """)

        with self.db.get_connection() as conn:
            counts = execute_query(conn, query, (package_id,), fetch=True)

        status_counts = {row["status"]: row["count"] for row in counts}
        total = sum(status_counts.values())

        if total == 0:
            package_status = "PENDING"
        elif status_counts.get("FAILED", 0) == total:
            package_status = "FAILED"
        elif status_counts.get("PERSISTED", 0) == total:
            package_status = "SUCCESS"
        elif status_counts.get("PERSISTED", 0) > 0:
            package_status = "PARTIAL_SUCCESS"
        else:
            package_status = "PROCESSING"

        query = sql.SQL("""
            UPDATE document_packages
            SET status = %s, updated_at = NOW()
            WHERE package_id = %s
        """)

        with self.db.get_connection() as conn:
            execute_query(conn, query, (package_status, package_id))
            logger.info(f"Updated package {package_id} status to {package_status}")
    
    def update_extraction_status(self, document_id: str, status: str, 
                                started: bool = False, completed: bool = False):
        """Update extraction status for a document."""
        updates = {"extraction_status": status}
        if started:
            updates["extraction_started_at"] = datetime.now()
        if completed:
            updates["extraction_completed_at"] = datetime.now()
        
        set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
        values = list(updates.values()) + [datetime.now(), document_id]
        
        query = f"UPDATE documents SET {set_clause}, updated_at = %s WHERE document_id = %s"
        
        with self.db.get_connection() as conn:
            execute_query(conn, query, values)
            logger.info(f"Updated extraction status for {document_id}")
    
    def save_invoice(self, document_id: str, invoice_data: dict):
        """Save extracted invoice data."""
        query = sql.SQL("""
            INSERT INTO invoices 
            (document_id, invoice_number, invoice_date, vendor_name, vendor_gstin,
             customer_name, customer_gstin, subtotal, tax_amount, total_amount,
             currency, extraction_confidence, raw_extracted_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (document_id) DO UPDATE SET
                invoice_number = EXCLUDED.invoice_number,
                invoice_date = EXCLUDED.invoice_date,
                vendor_name = EXCLUDED.vendor_name,
                total_amount = EXCLUDED.total_amount,
                extraction_confidence = EXCLUDED.extraction_confidence,
                updated_at = NOW()
            RETURNING invoice_id
        """)
        
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                query,
                (
                    document_id,
                    invoice_data.get("invoice_number"),
                    invoice_data.get("invoice_date"),
                    invoice_data.get("vendor_name"),
                    invoice_data.get("vendor_gstin"),
                    invoice_data.get("customer_name"),
                    invoice_data.get("customer_gstin"),
                    invoice_data.get("subtotal"),
                    invoice_data.get("tax_amount"),
                    invoice_data.get("total_amount"),
                    invoice_data.get("currency", "INR"),
                    invoice_data.get("extraction_confidence", 0.0),
                    json.dumps(invoice_data)
                )
            )
            result = cursor.fetchone()
            conn.commit()
            logger.info(f"Saved invoice for document {document_id}")
            return result[0]
    
    def save_bank_statement(self, document_id: str, statement_data: dict):
        """Save extracted bank statement data."""
        query = sql.SQL("""
            INSERT INTO bank_statements 
            (document_id, bank_name, account_number, statement_period_from,
             statement_period_to, opening_balance, closing_balance,
             total_debits, total_credits, currency, transaction_count, raw_extracted_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (document_id) DO UPDATE SET
                bank_name = EXCLUDED.bank_name,
                account_number = EXCLUDED.account_number,
                closing_balance = EXCLUDED.closing_balance,
                updated_at = NOW()
            RETURNING statement_id
        """)
        
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                query,
                (
                    document_id,
                    statement_data.get("bank_name"),
                    statement_data.get("account_number"),
                    statement_data.get("statement_period_from"),
                    statement_data.get("statement_period_to"),
                    statement_data.get("opening_balance"),
                    statement_data.get("closing_balance"),
                    statement_data.get("total_debits"),
                    statement_data.get("total_credits"),
                    statement_data.get("currency", "INR"),
                    statement_data.get("transaction_count", 0),
                    json.dumps(statement_data)
                )
            )
            result = cursor.fetchone()
            conn.commit()
            logger.info(f"Saved bank statement for document {document_id}")
            return result[0]
    
    def save_id_document(self, document_id: str, id_data: dict):
        """Save extracted ID document data (PAN or Aadhaar)."""
        id_type = id_data.get("id_type", "UNKNOWN")
        query = sql.SQL("""
            INSERT INTO id_documents 
            (document_id, id_type, id_number, holder_name, dob, gender, 
             father_name, address, extraction_confidence, raw_extracted_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (document_id) DO UPDATE SET
                id_type = EXCLUDED.id_type,
                id_number = EXCLUDED.id_number,
                holder_name = EXCLUDED.holder_name,
                extraction_confidence = EXCLUDED.extraction_confidence,
                updated_at = NOW()
            RETURNING id_document_id
        """)
        
        # Extract the correct ID number based on type
        if id_type == "PAN":
            id_number = id_data.get("pan_number")
        elif id_type == "AADHAAR":
            id_number = id_data.get("aadhaar_number")
        else:
            id_number = None
        
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                query,
                (
                    document_id,
                    id_type,
                    id_number,
                    id_data.get("holder_name"),
                    id_data.get("dob"),
                    id_data.get("gender"),
                    id_data.get("father_name"),
                    id_data.get("address"),
                    id_data.get("extraction_confidence", 0.0),
                    json.dumps(id_data)
                )
            )
            result = cursor.fetchone()
            conn.commit()
            logger.info(f"Saved ID document for {document_id}: type={id_type}, id={id_number}")
            return result[0]
    
    def save_transactions(self, statement_id: str, transactions: list):
        """Save bank transactions."""
        query = sql.SQL("""
            INSERT INTO bank_transactions 
            (statement_id, transaction_date, value_date, description, 
             transaction_type, amount, running_balance, reference_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """)
        
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            for tx in transactions:
                cursor.execute(
                    query,
                    (
                        statement_id,
                        tx.get("transaction_date"),
                        tx.get("value_date"),
                        tx.get("description"),
                        tx.get("transaction_type"),
                        tx.get("amount"),
                        tx.get("running_balance"),
                        tx.get("reference_number")
                    )
                )
            conn.commit()
            logger.info(f"Saved {len(transactions)} transactions for statement {statement_id}")
    
    def log_processing_event(self, document_id: str = None, package_id: str = None,
                           stage: str = None, status: str = None, 
                           message: str = None, error_details: str = None, 
                           duration_ms: int = None):
        """Log processing event to audit trail."""
        query = sql.SQL("""
            INSERT INTO processing_logs 
            (document_id, package_id, stage, status, message, error_details, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """)
        
        with self.db.get_connection() as conn:
            execute_query(
                conn, query,
                (document_id, package_id, stage, status, message, error_details, duration_ms)
            )
    
    def log_kafka_event(self, topic: str, message_key: str, message_value: dict,
                       event_type: str, document_id: str = None, 
                       partition: int = None, offset: int = None):
        """Log Kafka event for audit trail."""
        query = sql.SQL("""
            INSERT INTO kafka_events 
            (topic_name, message_key, message_value, event_type, document_id, partition, msg_offset, published_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """)
        
        with self.db.get_connection() as conn:
            execute_query(
                conn, query,
                (topic, message_key, json.dumps(message_value), event_type, 
                 document_id, partition, offset, datetime.now())
            )
    
    def get_document(self, document_id: str):
        """Retrieve document details."""
        query = "SELECT * FROM documents WHERE document_id = %s"
        
        with self.db.get_connection() as conn:
            results = execute_query(conn, query, (document_id,), fetch=True)
            return results[0] if results else None
    
    def get_documents_by_status(self, status: str, limit: int = 100):
        """Get documents by status."""
        query = "SELECT * FROM documents WHERE status = %s ORDER BY created_at DESC LIMIT %s"
        
        with self.db.get_connection() as conn:
            return execute_query(conn, query, (status, limit), fetch=True)


