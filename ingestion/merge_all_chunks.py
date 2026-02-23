import json
import os

FILES = [
    "data/processed/cgst_chunks.json",
    "data/processed/igst_chunks.json",
    "data/processed/cgst_rules_chunks.json",
    "data/processed/igst_rules_chunks.json",
    "data/processed/notification_chunks.json",
    "data/processed/circular_chunks.json",
    "data/processed/judgment_chunks.json",
    "data/processed/article_chunks.json",
    "data/processed/hsn_chunks.json",
    "data/processed/sac_chunks.json",
    "data/processed/forms.json",
    "data/processed/gst_council_meetings.json",
    "data/processed/gstat_rules.json",
    "data/processed/gstat_forms.json",
    "data/processed/case_scenarios.json",
    "data/processed/section_analytical_review.json",
    "data/processed/faqs.json",
    "data/processed/draft_replies.json",
    "data/processed/case_studies.json",
    "data/processed/solved_query_chunks.json", 
    "data/processed/contempary_issues.json"
]

OUTPUT_FILE = "data/processed/all_chunks.json"

REQUIRED_KEYS = {"id", "text"}

def main():
    merged = []
    seen_ids = set()
    skipped_files = []
    skipped_chunks = 0

    for path in FILES:
        if not os.path.exists(path):
            skipped_files.append(path)
            continue

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        loaded = 0
        for chunk in data:
            # ---- basic validation ----
            if not REQUIRED_KEYS.issubset(chunk):
                skipped_chunks += 1
                continue

            cid = chunk["id"]
            if cid in seen_ids:
                continue

            seen_ids.add(cid)
            merged.append(chunk)
            loaded += 1

        print(f"Loaded {loaded:>6} chunks from {path}")

    # ---- write output ----
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print("\n==============================")
    print(f"[OK] TOTAL UNIQUE CHUNKS: {len(merged)}")
    print(f"[SKIP] Skipped invalid chunks: {skipped_chunks}")

    if skipped_files:
        print("\n[WARN] Skipped missing files:")
        for s in skipped_files:
            print(f" - {s}")

if __name__ == "__main__":
    main()
