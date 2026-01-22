import csv
import uuid

REQUIRED_HEADERS = [
    "ID",
    "Author Name",
    "Author Designation",
    "Title",
    "Description"
]

def parse_gst_articles(csv_path):
    articles = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)

        # --- Find header ---
        header = None
        for row in reader:
            if row and any(cell.strip() for cell in row):
                header = [cell.strip() for cell in row]
                break

        if not header:
            raise ValueError("No header row found in CSV")

        try:
            idx = {
                "id": header.index("ID"),
                "author": header.index("Author Name"),
                "designation": header.index("Author Designation"),
                "title": header.index("Title"),
                "description": header.index("Description"),
            }
        except ValueError as e:
            raise ValueError(f"Missing required column: {e}")

        # --- Read rows ---
        for row in reader:
            if not row or len(row) < len(header):
                continue

            description = row[idx["description"]].strip()
            if not description:
                continue

            articles.append({
                "id": str(uuid.uuid4()),
                "external_id": row[idx["id"]].strip(),
                "title": row[idx["title"]].strip(),
                "text": description,
                "content_type": "article",
                "is_statutory": False,
                "metadata": {
                    "source": "GST Articles",
                    "author": row[idx["author"]].strip(),
                    "author_designation": row[idx["designation"]].strip(),
                    "law_scope": "GST",        # CGST / IGST / GST (generic)
                }
            })

    return articles
