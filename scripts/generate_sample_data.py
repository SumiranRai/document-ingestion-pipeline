"""
Generate synthetic sample documents for pipeline testing.

Creates:
  sample/Bank Statement/  — 5 CSV files (named with bank in filename for detection)
  sample/Invoices/        — 5 JPEG invoice images
  sample/PAN/             — 5 JPEG PAN card images

Usage:
  python scripts/generate_sample_data.py
"""

import csv
import os
import random
import string
from datetime import date, timedelta
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

SAMPLE_DIR = Path(__file__).parent.parent / "sample"
SEED = 42
random.seed(SEED)


# ── font helper ────────────────────────────────────────────────────────────────

def _font(size: int):
    """Try common system font paths; fall back to PIL default."""
    candidates = [
        "arial.ttf",
        "Arial.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            pass
    return ImageFont.load_default()


def _bold_font(size: int):
    candidates = [
        "arialbd.ttf",
        "Arial Bold.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            pass
    return _font(size)


# ── image builder ──────────────────────────────────────────────────────────────

def _make_image(lines: list, out_path: Path, width: int = 620, font_size: int = 20):
    """Render a list of text lines onto a white JPEG image."""
    margin     = 40
    line_h     = font_size + 10
    height     = margin * 2 + line_h * len(lines)

    img  = Image.new("RGB", (width, height), color="white")
    draw = ImageDraw.Draw(img)

    regular = _font(font_size)
    bold    = _bold_font(font_size + 2)

    y = margin
    for idx, line in enumerate(lines):
        f = bold if (idx == 0 or line.startswith("=")) else regular
        draw.text((margin, y), line, fill="black", font=f)
        y += line_h

    img.save(str(out_path), "JPEG", quality=95)


# ── Bank statement CSVs ────────────────────────────────────────────────────────

BANKS = [
    ("hdfc",  "HDFC Bank"),
    ("icici", "ICICI Bank"),
    ("sbi",   "SBI"),
    ("axis",  "Axis Bank"),
    ("kotak", "Kotak Bank"),
]

MODES = ["UPI", "NEFT", "IMPS", "ATM", "Cheque", "RTGS"]
NARRATIONS = [
    "Salary Credit", "ATM Cash Withdrawal", "UPI Payment to merchant",
    "NEFT Transfer Received", "Bill Payment", "Online Purchase",
    "EMI Debit", "Interest Credit", "Dividend Credit", "Insurance Premium",
]


def generate_bank_statements(n: int = 5):
    out_dir = SAMPLE_DIR / "Bank Statement"
    out_dir.mkdir(parents=True, exist_ok=True)

    for i in range(n):
        bank_key, bank_name = BANKS[i]
        out_path = out_dir / f"{bank_key}_statement_{i + 1}.csv"

        start_date  = date(2024, 1, 1) + timedelta(days=i * 30)
        balance     = round(random.uniform(50_000, 2_00_000), 2)
        total_days  = 30

        rows = [["date", "DrCr", "amount", "balance", "mode", "name"]]
        for day_offset in range(total_days):
            txn_date = start_date + timedelta(days=day_offset)
            dr_cr    = random.choice(["Dr", "Dr", "Cr"])  # more debits
            amount   = round(random.uniform(200, 25_000), 2)
            balance  = round(
                balance - amount if dr_cr == "Dr" else balance + amount, 2
            )
            mode     = random.choice(MODES)
            desc     = f"{random.choice(NARRATIONS)} - {bank_name}"
            rows.append([str(txn_date), dr_cr, amount, balance, mode, desc])

        with open(out_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)

        print(f"  Created: {out_path.name}  ({total_days} transactions)")


# ── Invoice images ─────────────────────────────────────────────────────────────

VENDORS   = ["Tech Solutions Pvt Ltd", "Office Supplies Co", "Cloud Services Ltd",
             "Data Systems Inc", "Print Works Pvt Ltd"]
CUSTOMERS = ["ABC Corporation", "XYZ Enterprises", "LMN Industries",
             "PQR Holdings Ltd", "RST Group"]
GSTINS    = ["27AABCT1332L1ZW", "29AADCB2230M1ZP", "07AAACD2230P1ZV",
             "24AABCA1234C1ZY", "33AACCK1234F1ZX"]


def generate_invoices(n: int = 5):
    out_dir = SAMPLE_DIR / "Invoices"
    out_dir.mkdir(parents=True, exist_ok=True)

    for i in range(n):
        inv_no    = f"INV-2024-{1000 + i}"
        inv_date  = date(2024, random.randint(1, 12), random.randint(1, 28))
        due_date  = inv_date + timedelta(days=30)
        subtotal  = round(random.uniform(5_000, 50_000), 2)
        gst_rate  = 0.18
        tax_amt   = round(subtotal * gst_rate, 2)
        total     = round(subtotal + tax_amt, 2)
        vendor    = VENDORS[i]
        customer  = CUSTOMERS[i]
        gstin     = GSTINS[i]

        lines = [
            "TAX INVOICE",
            "=" * 55,
            f"Invoice Number : {inv_no}",
            f"Invoice Date   : {inv_date.strftime('%d/%m/%Y')}",
            f"Due Date       : {due_date.strftime('%d/%m/%Y')}",
            "",
            f"Vendor Name    : {vendor}",
            f"Vendor GSTIN   : {gstin}",
            "",
            f"Customer Name  : {customer}",
            "",
            "=" * 55,
            "Description              : Professional Services",
            f"Subtotal                 : INR {subtotal:,.2f}",
            f"GST @ 18%                : INR {tax_amt:,.2f}",
            "=" * 55,
            f"Total Amount             : INR {total:,.2f}",
            "=" * 55,
        ]

        out_path = out_dir / f"invoice_{i + 1:03d}.jpg"
        _make_image(lines, out_path, width=640)
        print(f"  Created: {out_path.name}  (total={total})")


# ── PAN card images ────────────────────────────────────────────────────────────

NAMES   = ["RAHUL SHARMA", "PRIYA PATEL", "AMIT KUMAR", "SUNITA VERMA", "RAJESH GUPTA"]
FATHERS = ["SURESH SHARMA", "DINESH PATEL", "VIJAY KUMAR", "RAMESH VERMA", "MOHAN GUPTA"]
DOBS    = ["01/01/1985", "15/06/1990", "22/03/1978", "08/11/1982", "30/07/1975"]


def _make_pan() -> str:
    return (
        "".join(random.choices(string.ascii_uppercase, k=5))
        + "".join(random.choices(string.digits, k=4))
        + random.choice(string.ascii_uppercase)
    )


def generate_pan_cards(n: int = 5):
    out_dir = SAMPLE_DIR / "PAN"
    out_dir.mkdir(parents=True, exist_ok=True)

    for i in range(n):
        pan = _make_pan()
        lines = [
            "INCOME TAX DEPARTMENT",
            "GOVT. OF INDIA",
            "=" * 40,
            "Permanent Account Number Card",
            "",
            f"PAN   :  {pan}",
            "",
            f"Name         : {NAMES[i]}",
            f"Father's Name: {FATHERS[i]}",
            f"Date of Birth: {DOBS[i]}",
            "",
            "Signature",
        ]

        out_path = out_dir / f"pan_{i + 1:03d}.jpg"
        _make_image(lines, out_path, width=580, font_size=22)
        print(f"  Created: {out_path.name}  (PAN={pan})")


# ── main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Generating synthetic sample data...")
    print("\n[Bank Statements]")
    generate_bank_statements(5)
    print("\n[Invoices]")
    generate_invoices(5)
    print("\n[PAN Cards]")
    generate_pan_cards(5)
    print(f"\nDone — files written to: {SAMPLE_DIR.resolve()}")
