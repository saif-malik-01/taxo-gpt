from parse_igst_rules import parse_igst_rules
from chunk_igst_rules import chunk_igst_rule
import json

rules = parse_igst_rules("data/raw/docx/igst_rules.docx")

all_chunks = []
for rule in rules:
    all_chunks.extend(chunk_igst_rule(rule))

with open("data/processed/igst_rules_chunks.json", "w", encoding="utf-8") as f:
    json.dump(all_chunks, f, ensure_ascii=False, indent=2)

print(f"Total IGST Rules chunks created: {len(all_chunks)}")
