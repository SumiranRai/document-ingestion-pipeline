import logging
import tempfile
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from app.config import KAFKA_TOPICS
from app.extraction.document_type_detector import detect_document_type
from app.extraction.ocr_space import ocr_space_extract_text
from app.kafka.consumers.base_consumer import BaseStageConsumer

logger = logging.getLogger(__name__)


class PreprocessingConsumer(BaseStageConsumer):
    def __init__(self):
        super().__init__(KAFKA_TOPICS["received"], "preprocessing", KAFKA_TOPICS["preprocessed"])

    def _fetch_source(self, source_path: str) -> str:
        return source_path

    def _read_text(self, file_path: str) -> str:
        path = Path(file_path)
        suffix = path.suffix.lower()

        if suffix == ".txt":
            return path.read_text(encoding="utf-8", errors="replace")
        if suffix in {".jpg", ".jpeg", ".png", ".pdf"}:
            return ocr_space_extract_text(file_path)

        return ""

    def _read_tabular(self, file_path: str) -> Optional[Dict[str, object]]:
        path = Path(file_path)
        suffix = path.suffix.lower()

        try:
            if suffix == ".csv":
                df = pd.read_csv(file_path, dtype=str)
            elif suffix in {".xlsx", ".xls"}:
                df = pd.read_excel(file_path, dtype=str)
            else:
                return None

            df = df.fillna("")
            return {
                "headers": list(df.columns.astype(str)),
                "rows": df.astype(str).replace("nan", "", regex=False).to_dict(orient="records"),
            }
        except Exception as exc:
            logger.warning("Could not parse tabular file %s: %s", file_path, exc)
            return None

    def process_event(self, event: dict):
        source_path = event["source_path"]
        local_path = self._fetch_source(source_path)
        normalized_text = ""
        table_data = None
        document_type = event.get("document_type")

        try:
            if local_path:
                if Path(local_path).suffix.lower() in {".csv", ".xls", ".xlsx"}:
                    table_data = self._read_tabular(local_path)
                else:
                    normalized_text = self._read_text(local_path)

            if not document_type:
                document_type = detect_document_type(local_path)

            payload = {
                "document_type": document_type,
                "file_name": Path(source_path).name,
                "normalized_text": normalized_text,
                "table_data": table_data,
            }

            self.publish_next(event, payload)
        finally:
            pass
