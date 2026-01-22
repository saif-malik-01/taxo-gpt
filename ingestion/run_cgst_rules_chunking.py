from parse_cgst_rules import parse_cgst_rules
from chunk_cgst_rule import chunk_rule

import json

rules = parse_cgst_rules("data/raw/docx/cgst_rules.docx")

all_chunks = []

for rule in rules:
    rule_chunks = chunk_rule(rule)
    all_chunks.extend(rule_chunks)

with open("data/processed/cgst_rules_chunks.json", "w", encoding="utf-8") as f:
    json.dump(all_chunks, f, ensure_ascii=False, indent=2)

print(f"Total CGST Rules chunks created: {len(all_chunks)}")
