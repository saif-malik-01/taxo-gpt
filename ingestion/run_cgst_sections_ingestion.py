import json
from parse_cgst_sections import parse_cgst_act

sections = parse_cgst_act("data/raw/docx/cgst_act.docx")

with open("data/processed/cgst_sections.json", "w", encoding="utf-8") as f:
    json.dump(sections, f, ensure_ascii=False, indent=2)

print(f"Total Sections Parsed: {len(sections)}")
