from parse_igst_act import parse_igst_act
from chunk_igst_act import chunk_igst_section
import json

sections = parse_igst_act("data/raw/docx/igst_act.docx")

all_chunks = []
for sec in sections:
    all_chunks.extend(chunk_igst_section(sec))

with open("data/processed/igst_chunks.json", "w", encoding="utf-8") as f:
    json.dump(all_chunks, f, ensure_ascii=False, indent=2)

print(f"Total IGST chunks created: {len(all_chunks)}")
