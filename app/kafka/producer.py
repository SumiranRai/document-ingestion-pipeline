from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

message = {
    "document_id": "123",
    "document_type": "invoice",
    "file_path": "sample_documents/invoices/invoice1.jpg"
}

producer.send("documents.raw", message)

producer.flush()

print("Message sent successfully")