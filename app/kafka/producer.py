from kafka import KafkaProducer
import json
from datetime import datetime


import os

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_SERVER", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

message = {
    "document_type": "invoice",
    "file_path": "sample_documents/invoices/invoice1.jpg",
    "received_at": str(datetime.now())
}


producer.send(
    'documents',
    value=message
)

producer.flush()

print("Message sent successfully")