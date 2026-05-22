from dataclasses import fields
from app.persistence.db_writer import save_invoice_data

import requests
import re

API_KEY = "helloworld"


def extract_text_from_image(image_path):

    with open(image_path, "rb") as image_file:

        response = requests.post(
            "https://api.ocr.space/parse/image",
            files={"filename": image_file},
            data={
                "apikey": API_KEY,
                "language": "eng"
            }
        )

    result = response.json()

    parsed_results = result.get("ParsedResults")

    if not parsed_results:
        return ""

    text = parsed_results[0].get("ParsedText", "")

    return text

def parse_invoice_fields(text):

    import re

    fields = {}

    # Invoice Number
    invoice_number = re.search(
        r"Invoice\s*(No|Number)?[:\-]?\s*(\S+)",
        text,
        re.IGNORECASE
    )

    if invoice_number:
        fields["invoice_number"] = invoice_number.group(2)

    # Vendor
    vendor = re.search(
        r"(Seller|Vendor)[:\-]?\s*\n?(.+)",
        text,
        re.IGNORECASE
    )

    if vendor:
        fields["vendor_name"] = vendor.group(2).strip()

    # Invoice Date
    invoice_date = re.search(
        r"(\d{2}/\d{2}/\d{4})",
        text
    )

    if invoice_date:
        fields["invoice_date"] = invoice_date.group(1)

    # GSTIN
    gstin = re.search(
        r"GSTIN[:\-]?\s*(\S+)",
        text,
        re.IGNORECASE
    )

    if gstin:
        fields["gstin"] = gstin.group(1)

    # Total Amount
    # Total Amount

    amount_matches = re.findall(
        r"\d[\d\s]*,\d{2}",
        text
    )

    cleaned_amounts = []

    for amt in amount_matches:

        cleaned = amt.replace(" ", "").replace(",", ".")

        try:
            value = float(cleaned)

            # Ignore unrealistic huge numbers
            if value < 1000000:
                cleaned_amounts.append(value)

        except:
            pass

    if cleaned_amounts:
        fields["total_amount"] = max(cleaned_amounts)

    return fields


if __name__ == "__main__":

    image_path = "sample_documents/invoices/invoice1.jpg"

    extracted_text = extract_text_from_image(image_path)

    print("\n===== EXTRACTED TEXT =====\n")

    print(extracted_text)

    parsed_fields = parse_invoice_fields(extracted_text)

    print("\n===== PARSED FIELDS =====\n")

    print(parsed_fields)
