import uuid
import re
from pathlib import Path
from PyPDF2 import PdfReader

CIRCULAR_NO_PATTERN = re.compile(
    r"Circular\s+No\.?\s*([0-9/ \-A-Za-z]+)",
    re.IGNORECASE
)

DATE_PATTERN = re.compile(
    r"Dated[:\s]+([0-9A-Za-z ,]+)",
    re.IGNORECASE
)

SUBJECT_PATTERN = re.compile(
    r"Subject[:\s]+(.+)",
    re.IGNORECASE
)


def parse_circular(pdf_path: str, category: str):
    reader = PdfReader(pdf_path)

    pages = []
    for page in reader.pages:
        if page.extract_text():
            pages.append(page.extract_text())

    raw_text = "\n".join(pages)

    circular_no = None
    date = None
    subject = None

    if m := CIRCULAR_NO_PATTERN.search(raw_text):
        circular_no = m.group(1).strip()

    if m := DATE_PATTERN.search(raw_text):
        date = m.group(1).strip()

    if m := SUBJECT_PATTERN.search(raw_text):
        subject = m.group(1).strip()

    return {
        "id": str(uuid.uuid4()),
        "category": category,  # circular-cgst / compensation-gst
        "circular_no": circular_no,
        "date": date,
        "subject": subject,
        "text": raw_text,
        "metadata": {
            "source_type": "CIRCULAR",
            "issuer": "CBIC",
            "file_name": Path(pdf_path).name
        }
    }
