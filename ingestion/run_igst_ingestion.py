import json
import os
from parse_cgst_sections import parse_cgst_act

INPUT_DOCX = "data/raw/docx/igst_act.docx"
OUTPUT_JSON = "data/processed/igst_sections.json"

os.makedirs("data/processed", exist_ok=True)

sections = parse_cgst_act(INPUT_DOCX)

# 🔑 Add Act metadata
for s in sections:
    s["metadata"]["act"] = "IGST"

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(sections, f, ensure_ascii=False, indent=2)

print(f"IGST Sections Parsed: {len(sections)}")
