import json
import os
from parse_cgst_rules import parse_cgst_rules

INPUT_DOCX = "data/raw/docx/cgst_rules.docx"
OUTPUT_JSON = "data/processed/cgst_rules.json"

os.makedirs("data/processed", exist_ok=True)

rules = parse_cgst_rules(INPUT_DOCX)

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(rules, f, ensure_ascii=False, indent=2)

print(f"CGST Rules Parsed: {len(rules)}")
