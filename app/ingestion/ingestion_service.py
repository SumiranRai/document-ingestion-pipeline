"""
High-level document ingestion service.
Used by both the Airflow DAG (scan_and_publish) and the Streamlit upload form.
"""

import uuid
from pathlib import Path
from typing import Optional

from app.config import DOCUMENT_INGESTION_PATH, DOCUMENT_TYPES
from app.extraction.document_type_detector import detect_document_type, detect_document_type_from_name
from app.kafka.event_schema import map_to_db_type
from app.kafka.producer import DocumentProducer
from app.persistence.db_writer import DocumentManager


class DocumentIngestionService:
    def __init__(self, ingestion_path: str = DOCUMENT_INGESTION_PATH):
        self.ingestion_path = Path(ingestion_path)
        self.ingestion_path.mkdir(parents=True, exist_ok=True)
        self.producer = DocumentProducer()
        self.db = DocumentManager()

    # ── file upload (Streamlit) ────────────────────────────────────────────────

    def save_uploaded_file(self, file_name: str, file_bytes: bytes, subdir: str = "uploads") -> str:
        out_dir = self.ingestion_path / subdir
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / file_name
        out_path.write_bytes(file_bytes)
        return str(out_path)

    def publish_uploaded_file(
        self,
        file_name: str,
        file_bytes: bytes,
        package_id: str,
        source: str = "web_upload",
        document_type: Optional[str] = None,
    ) -> str:
        saved_path = self.save_uploaded_file(file_name, file_bytes)
        doc_type = document_type or detect_document_type_from_name(file_name)
        db_type = map_to_db_type(doc_type)

        document_id = str(uuid.uuid4())
        self.db.create_document_record(
            package_id=package_id,
            doc_type=db_type,
            file_name=file_name,
            file_path=saved_path,
            source=source,
            file_size=len(file_bytes),
            document_id=document_id,
        )
        self.producer.publish_received(
            document_id=document_id,
            package_id=package_id,
            file_path=saved_path,
            document_type=db_type,
            source=source,
            file_bytes=file_bytes,
        )
        return document_id

    # ── directory scan (Airflow) ───────────────────────────────────────────────

    def scan_and_publish_documents(
        self,
        directory: str,
        package_name: Optional[str] = None,
        source: str = "local_scan",
    ) -> str:
        """
        Scan each configured document-type subdirectory, create a package record,
        and publish DOCUMENT_RECEIVED events for each file found.
        Returns the package_id (even if 0 documents were found).
        """
        base = Path(directory)
        if not base.exists():
            raise FileNotFoundError(f"Ingestion directory not found: {directory}")

        documents = []
        for doc_type, cfg in DOCUMENT_TYPES.items():
            subdir = base / cfg["subdir"]
            if not subdir.exists():
                continue
            exts = set(cfg["extensions"])
            for f in subdir.iterdir():
                if f.is_file() and f.suffix.lower() in exts:
                    documents.append({
                        "path":  str(f),
                        "name":  f.name,
                        "type":  doc_type,
                        "size":  f.stat().st_size,
                    })

        package_id = str(uuid.uuid4())
        pkg_name = package_name or f"scan_{package_id[:8]}"
        self.db.create_document_package(
            package_name=pkg_name,
            source=source,
            total_documents=len(documents),
            package_id=package_id,
            status="PENDING",
        )

        for doc in documents:
            document_id = str(uuid.uuid4())
            self.db.create_document_record(
                package_id=package_id,
                doc_type=doc["type"],
                file_name=doc["name"],
                file_path=doc["path"],
                source=source,
                file_size=doc["size"],
                document_id=document_id,
            )
            self.producer.publish_received(
                document_id=document_id,
                package_id=package_id,
                file_path=doc["path"],
                document_type=doc["type"],
                source=source,
            )

        return package_id

    def close(self):
        self.producer.close()
