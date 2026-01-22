import json
import os
from parse_igst_rules import parse_igst_rules

INPUT_DOCX = "data/raw/docx/igst_rules.docx"
OUTPUT_JSON = "data/processed/igst_rules.json"

os.makedirs("data/processed", exist_ok=True)

rules = parse_igst_rules(INPUT_DOCX)

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(rules, f, ensure_ascii=False, indent=2)

print(f"IGST Rules Parsed: {len(rules)}")
