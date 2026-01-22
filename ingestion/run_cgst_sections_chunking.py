import json
from chunk_cgst_sections import chunk_section


def main():
    # Load CGST sections
    with open("data/processed/cgst_sections.json", "r", encoding="utf-8") as f:
        sections = json.load(f)

    # Chunk all sections
    all_chunks = []
    for section in sections:
        all_chunks.extend(chunk_section(section))

    # Save chunks to file
    with open("data/processed/cgst_chunks.json", "w", encoding="utf-8") as f:
        json.dump(all_chunks, f, ensure_ascii=False, indent=2)

    print(f"Total chunks created: {len(all_chunks)}")


if __name__ == "__main__":
    main()
