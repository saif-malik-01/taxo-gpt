import json
from chunk_judgments import chunk_judgment

with open("data/processed/judgments.json", "r", encoding="utf-8") as f:
    judgments = json.load(f)

all_chunks = []

for j in judgments:
    all_chunks.extend(chunk_judgment(j))

with open("data/processed/judgment_chunks.json", "w", encoding="utf-8") as f:
    json.dump(all_chunks, f, ensure_ascii=False, indent=2)

print(f"Judgment Chunks Created: {len(all_chunks)}")
