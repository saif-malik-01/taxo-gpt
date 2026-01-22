import json
from pathlib import Path
from parse_notification import parse_notification
from chunk_notification import chunk_notification

NOTIFICATIONS_ROOT = Path("data/raw/pdf/notifications")
OUTPUT_FILE = Path("data/processed/notification_chunks.json")

def run():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            all_chunks = json.load(f)
    else:
        all_chunks = []

    initial = len(all_chunks)

    for category_dir in NOTIFICATIONS_ROOT.iterdir():
        if not category_dir.is_dir():
            continue

        category = category_dir.name

        for year_dir in category_dir.iterdir():
            if not year_dir.is_dir():
                continue

            for pdf in year_dir.glob("*.pdf"):
                print(f"📄 Parsing {pdf}")

                notif = parse_notification(
                    pdf_path=str(pdf),
                    category=category
                )

                chunks = chunk_notification(notif)
                all_chunks.extend(chunks)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_chunks, f, ensure_ascii=False, indent=2)

    print(f"✅ Added {len(all_chunks) - initial} notification chunks")

if __name__ == "__main__":
    run()
