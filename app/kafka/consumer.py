from kafka import KafkaConsumer
import json
import os

from app.validation.validator import validate_invoice_data
from app.extraction.invoice_extractor import (
    extract_text_from_image,
    parse_invoice_fields
)
from app.persistence.db_writer import save_invoice_data


KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")

consumer = KafkaConsumer(
    "documents",
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="latest",
    group_id="document-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Waiting for messages...")

for message in consumer:

    data = message.value

    print("\nReceived message:")
    print(data)

    file_path = data["file_path"]

    # OCR extraction
    extracted_text = extract_text_from_image(file_path)

    print("\n===== EXTRACTED TEXT =====\n")
    print(extracted_text)

    # Parse fields
    parsed_fields = parse_invoice_fields(extracted_text)

    print("\n===== PARSED FIELDS =====\n")
    print(parsed_fields)

    # VALIDATION MUST BE INSIDE LOOP
    validation_errors = validate_invoice_data(parsed_fields)

    if validation_errors:
        print("\n❌ VALIDATION FAILED")
        print(validation_errors)
        continue

    # SAVE TO DB
    save_invoice_data(parsed_fields, file_path)

    print("\n✅ Pipeline completed successfully")