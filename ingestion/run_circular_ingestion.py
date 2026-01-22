import json
from pathlib import Path
from parse_circular import parse_circular
from chunk_circular import chunk_circular

CIRCULARS_ROOT = Path("data/raw/pdf/circulars")
OUTPUT_FILE = Path("data/processed/circular_chunks.json")

def run():
    # ✅ Ensure output directory exists
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    # ✅ Load existing chunks if file exists, else start fresh
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            all_chunks = json.load(f)
    else:
        all_chunks = []

    initial_count = len(all_chunks)

    for category_dir in CIRCULARS_ROOT.iterdir():
        if not category_dir.is_dir():
            continue

        category = category_dir.name  # gst / compensation-gst / etc.

        for year_dir in category_dir.iterdir():
            if not year_dir.is_dir():
                continue

            for pdf in year_dir.glob("*.pdf"):
                print(f"📄 Parsing {pdf}")

                circular = parse_circular(
                    pdf_path=str(pdf),
                    category=category
                )

                chunks = chunk_circular(circular)
                all_chunks.extend(chunks)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_chunks, f, ensure_ascii=False, indent=2)

    print(f"✅ Added {len(all_chunks) - initial_count} circular chunks")
    print(f"📦 Total circular chunks: {len(all_chunks)}")

if __name__ == "__main__":
    run()
