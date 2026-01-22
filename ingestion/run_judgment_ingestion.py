import json
import os
from parse_judgments import parse_gst_judgments

INPUT_CSV = "data/raw/csv/gst_judgments.csv"
OUTPUT_JSON = "data/processed/judgments.json"

os.makedirs("data/processed", exist_ok=True)

judgments = parse_gst_judgments(INPUT_CSV)

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(judgments, f, ensure_ascii=False, indent=2)

print(f"Judgments Parsed: {len(judgments)}")
