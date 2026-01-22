import json

INPUT_JSON = "data/processed/sac.json"
OUTPUT_JSON = "data/processed/sac_chunks.json"

def main():
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        sac_data = json.load(f)

    chunks = []

    for item in sac_data:
        chunks.append({
            "id": item["id"],
            "text": item["text"],
            "metadata": item["metadata"]
        })

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)

    print(f"SAC Chunks Created: {len(chunks)}")

if __name__ == "__main__":
    main()
