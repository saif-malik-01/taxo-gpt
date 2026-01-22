import csv
import sys
import uuid

csv.field_size_limit(sys.maxsize)

REQUIRED_COLUMNS = {
    "ID",
    "Title",
    "Section",
    "SAC Code",
    "Description of Services",
    "CGST",
    "SGST",
    "IGST",
    "CESS"
}

def parse_sac(csv_path):
    records = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)

        header = None

        # ---- Detect header row ----
        for row in reader:
            if not row:
                continue

            cleaned = [c.strip() for c in row if c.strip()]
            if REQUIRED_COLUMNS.issubset(set(cleaned)):
                header = [c.strip() for c in row]
                break

        if not header:
            raise ValueError("SAC CSV header not detected")

        header_map = {name: idx for idx, name in enumerate(header)}

        def get(row, col):
            idx = header_map.get(col)
            if idx is None or idx >= len(row):
                return ""
            return row[idx].strip()

        # ---- Parse rows ----
        for row in reader:
            if not row or len(row) < len(header):
                continue

            sac_code = get(row, "SAC Code")
            desc = get(row, "Description of Services")

            if not sac_code or not desc:
                continue

            records.append({
                "id": f"SAC-{sac_code}",
                "chunk_type": "sac",
                "content_type": "sac",
                "is_statutory": False,
                "text": (
                    f"SAC Code {sac_code}: {desc}. "
                    f"GST Rates – CGST: {get(row, 'CGST')}, "
                    f"SGST: {get(row, 'SGST')}, "
                    f"IGST: {get(row, 'IGST')}, "
                    f"CESS: {get(row, 'CESS')}."
                ),
                "metadata": {
                    "source": "GST SAC",
                    "external_id": get(row, "ID"),
                    "sac_code": sac_code,
                    "title": get(row, "Title"),
                    "section": get(row, "Section"),
                    "cgst_rate": get(row, "CGST"),
                    "sgst_rate": get(row, "SGST"),
                    "igst_rate": get(row, "IGST"),
                    "cess_rate": get(row, "CESS")
                }
            })

    return records
