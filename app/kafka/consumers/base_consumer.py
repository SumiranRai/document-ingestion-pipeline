import logging
import time
from abc import ABC, abstractmethod
from typing import Optional

from kafka import KafkaConsumer

from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP_PREFIX,
    KAFKA_RETRY_ATTEMPTS,
    KAFKA_RETRY_BACKOFF_SECONDS,
)
from app.kafka.event_schema import create_event, validate_event
from app.kafka.stage_producer import KafkaProducerClient

logger = logging.getLogger(__name__)


class BaseStageConsumer(ABC):
    def __init__(self, topic: str, group_suffix: str, next_topic: Optional[str] = None):
        self.topic = topic
        self.next_topic = next_topic
        self.group_id = f"{KAFKA_CONSUMER_GROUP_PREFIX}-{group_suffix}"
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: v.decode("utf-8") and __import__("json").loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )
        self.producer_client = KafkaProducerClient()

    def start(self, max_messages: Optional[int] = None):
        processed_count = 0

        for message in self.consumer:
            event = message.value
            try:
                validate_event(event)
                self._process_with_retries(event)
            except Exception as exc:
                logger.exception("Failed to validate or process event: %s", exc)
                self.producer_client.publish_failed(
                    document_id=event.get("document_id"),
                    package_id=event.get("package_id"),
                    stage=self.topic,
                    error_details=str(exc),
                    original_event=event,
                )

            processed_count += 1
            if max_messages and processed_count >= max_messages:
                break

    def _process_with_retries(self, event: dict):
        for attempt in range(1, KAFKA_RETRY_ATTEMPTS + 1):
            try:
                self.process_event(event)
                return
            except Exception as exc:
                logger.exception(
                    "Error processing %s event (attempt %s/%s): %s",
                    self.topic,
                    attempt,
                    KAFKA_RETRY_ATTEMPTS,
                    exc,
                )
                if attempt < KAFKA_RETRY_ATTEMPTS:
                    time.sleep(KAFKA_RETRY_BACKOFF_SECONDS * attempt)

        self.producer_client.publish_failed(
            document_id=event.get("document_id"),
            package_id=event.get("package_id"),
            stage=self.topic,
            error_details=f"Exceeded retry attempts for stage {self.topic}",
            original_event=event,
        )

    def publish_next(self, event: dict, payload: dict):
        if not self.next_topic:
            raise RuntimeError("No next_topic configured for this consumer")

        next_event = create_event(
            event_type=self.next_topic,
            document_id=event["document_id"],
            package_id=event["package_id"],
            document_type=event["document_type"],
            source_path=event["source_path"],
            payload=payload,
        )
        self.producer_client.publish(
            topic=self.next_topic,
            event=next_event,
            partition_key=event["package_id"],
        )

    @abstractmethod
    def process_event(self, event: dict):
        raise NotImplementedError()
