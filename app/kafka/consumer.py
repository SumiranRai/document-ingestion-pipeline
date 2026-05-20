from kafka import KafkaConsumer
import json

from app.extraction.invoice_extractor import (
    extract_text_from_image,
    parse_invoice_fields
)

from app.validation.invoice_validator import (
    validate_invoice_data
)

from app.persistence.db_writer import (
    save_invoice_data
)


consumer = KafkaConsumer(
    "documents.raw",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='document-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Waiting for messages...")


for message in consumer:

    payload = message.value

    print("\nReceived message:")
    print(payload)

    file_path = payload["file_path"]

    try:

        print("\nExtracting text...")

        extracted_text = extract_text_from_image(file_path)

        print("\nParsing invoice fields...")

        parsed_data = parse_invoice_fields(extracted_text)

        print(parsed_data)

        print("\nValidating data...")

        validation_result = validate_invoice_data(parsed_data)

        print(validation_result)

        if validation_result["valid"]:

            print("\nSaving to PostgreSQL...")

            save_invoice_data(parsed_data, file_path)

        else:

            print("Validation failed")

    except Exception as e:

        print(f"Processing failed: {e}")