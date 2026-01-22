import csv
import uuid

REQUIRED_COLUMNS = [
    "ID", "Title", "Chapter Number", "Chapter Name",
    "Sub Chapter Number", "Sub Chapter Name",
    "HSN Code", "Description",
    "CGST", "SGST", "IGST", "CESS"
]

def parse_hsn(csv_path):
    records = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        raise ValueError("HSN CSV file is empty")

    # ---- Detect header row (first 10 rows) ----
    header_index = None
    for i in range(min(10, len(rows))):
        row = [c.strip() for c in rows[i]]
        if all(col in row for col in REQUIRED_COLUMNS):
            header_index = i
            break

    if header_index is None:
        raise ValueError("HSN CSV header not detected")

    header = [c.strip() for c in rows[header_index]]
    data_rows = rows[header_index + 1 :]

    for row in data_rows:
        if len(row) < len(header):
            continue

        record = dict(zip(header, row))

        hsn_code = record.get("HSN Code", "").strip()
        if not hsn_code:
            continue

        records.append({
            "id": f"HSN-{hsn_code}",
            "content_type": "hsn",
            "chunk_type": "hsn",
            "is_statutory": False,
            "text": (
                f"HSN Code {hsn_code}: {record.get('Description')}. "
                f"GST Rates – CGST: {record.get('CGST')}, "
                f"SGST: {record.get('SGST')}, "
                f"IGST: {record.get('IGST')}, "
                f"CESS: {record.get('CESS')}."
            ),
            "metadata": {
                "source": "HSN Master",
                "hsn_code": hsn_code,
                "title": record.get("Title"),
                "chapter_number": record.get("Chapter Number"),
                "chapter_name": record.get("Chapter Name"),
                "sub_chapter_number": record.get("Sub Chapter Number"),
                "sub_chapter_name": record.get("Sub Chapter Name"),
                "cgst_rate": record.get("CGST"),
                "sgst_rate": record.get("SGST"),
                "igst_rate": record.get("IGST"),
                "cess_rate": record.get("CESS")
            }
        })

    return records
