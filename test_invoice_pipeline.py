from app.extraction.invoice_extractor import (
    extract_text_from_image,
    parse_invoice_fields
)

from app.validation.validator import validate_invoice_data

from app.persistence.db_writer import save_invoice_data
from app.persistence.logger import insert_log

file_path = "sample_documents/invoices/invoice1.jpg"


# OCR extraction
extracted_text = extract_text_from_image(file_path)

print("\n===== EXTRACTED TEXT =====\n")
print(extracted_text)


# Parse fields
parsed_fields = parse_invoice_fields(extracted_text)

print("\n===== PARSED FIELDS =====\n")
print(parsed_fields)


# Validation
validation_errors = validate_invoice_data(parsed_fields)


if validation_errors:

    print("\nVALIDATION FAILED")
    print(validation_errors)

else:

    save_invoice_data(
        parsed_fields,
        file_path
    )

    print("\nPipeline completed successfully")