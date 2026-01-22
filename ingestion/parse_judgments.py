import csv
import sys
import uuid

csv.field_size_limit(sys.maxsize)

def parse_gst_judgments(csv_path):
    judgments = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)

        # ---- Detect header ----
        header = None
        for row in reader:
            if row and any(cell.strip() for cell in row):
                header = [cell.strip() for cell in row]
                break

        if not header:
            raise ValueError("No header row found in judgments CSV")

        def idx(col):
            return header.index(col) if col in header else None

        for row in reader:
            if not row or len(row) < len(header):
                continue

            description = row[idx("Judgement Description")].strip()
            if not description:
                continue

            judgments.append({
                "id": str(uuid.uuid4()),
                "external_id": row[idx("ID")].strip(),
                "title": row[idx("Title")].strip(),
                "text": description,
                "content_type": "judgment",
                "is_statutory": False,
                "metadata": {
                    "source": "GST Judgments",
                    "citation": row[idx("Citation")].strip(),
                    "case_number": row[idx("Case Number")].strip(),
                    "court": row[idx("Court")].strip(),
                    "state": row[idx("State")].strip(),
                    "year": row[idx("Year of Judgement")].strip(),
                    "judge": row[idx("Judge Name")].strip(),
                    "petitioner": row[idx("Petitioner/Appellant Title")].strip(),
                    "respondent": row[idx("Respondent Title")].strip(),
                    "decision": row[idx("Decision")].strip(),
                    "current_status": row[idx("Current Status")].strip(),
                    "law": row[idx("Law")].strip(),
                    "act_name": row[idx("Act Name")].strip(),
                    "section_number": row[idx("Section Number")].strip(),
                    "rule_name": row[idx("Rule Name")].strip(),
                    "rule_number": row[idx("Rule Number")].strip(),
                    "notification_number": row[idx("Notification / Circular Number")].strip(),
                    "case_note": row[idx("Case Note")].strip()
                }
            })

    return judgments
