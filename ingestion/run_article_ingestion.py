import json
import os
from parse_articles import parse_gst_articles

INPUT_CSV = "data/raw/csv/gst_articles.csv"
OUTPUT_JSON = "data/processed/articles.json"

os.makedirs("data/processed", exist_ok=True)

articles = parse_gst_articles(INPUT_CSV)

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(articles, f, ensure_ascii=False, indent=2)

print(f"Articles Parsed: {len(articles)}")
