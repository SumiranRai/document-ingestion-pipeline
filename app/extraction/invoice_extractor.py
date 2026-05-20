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

    fields = {}

    invoice_number = re.search(
        r"Invoice\s*(No|Number)?[:\-]?\s*(\S+)",
        text,
        re.IGNORECASE
    )

    vendor_name = re.search(
        r"Vendor[:\-]?\s*(.+)",
        text,
        re.IGNORECASE
    )

    gstin = re.search(
        r"GSTIN[:\-]?\s*(\S+)",
        text,
        re.IGNORECASE
    )

    total_amount = re.search(
        r"Total\s*Amount[:\-]?\s*([\d,.]+)",
        text,
        re.IGNORECASE
    )

    if invoice_number:
        fields["invoice_number"] = invoice_number.group(2)

    if vendor_name:
        fields["vendor_name"] = vendor_name.group(1)

    if gstin:
        fields["gstin"] = gstin.group(1)

    if total_amount:
        fields["total_amount"] = total_amount.group(1)

    return fields


if __name__ == "__main__":

    image_path = "sample_documents/invoices/invoice1.jpg"

    extracted_text = extract_text_from_image(image_path)

    print("\n===== EXTRACTED TEXT =====\n")

    print(extracted_text)

    parsed_fields = parse_invoice_fields(extracted_text)

    print("\n===== PARSED FIELDS =====\n")

    print(parsed_fields)

