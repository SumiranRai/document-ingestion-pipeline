"""
Kafka lag checker — importable by Airflow DAGs.
"""

import logging

from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS

logger = logging.getLogger(__name__)


def check_kafka_lag(**context) -> dict:
    """
    Compute end-to-beginning offset lag for every configured topic/partition.
    Safe to call even when topics have no messages yet.
    """
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )

    lag_report: dict = {}
    try:
        for name, topic in KAFKA_TOPICS.items():
            partitions = consumer.partitions_for_topic(topic) or set()
            for partition in partitions:
                tp = TopicPartition(topic, partition)
                consumer.assign([tp])
                consumer.seek_to_end(tp)
                end_offset = consumer.position(tp)
                consumer.seek_to_beginning(tp)
                start_offset = consumer.position(tp)
                lag_report[f"{topic}:{partition}"] = max(0, end_offset - start_offset)
    finally:
        consumer.close()

    logger.info("Kafka lag report: %s", lag_report)

    if context.get("task_instance"):
        context["task_instance"].xcom_push(key="kafka_lag", value=lag_report)

    return lag_report
