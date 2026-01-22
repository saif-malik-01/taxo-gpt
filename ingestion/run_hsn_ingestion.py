import json
from parse_hsn import parse_hsn

INPUT_CSV = "data/raw/csv/hsn.csv"
OUTPUT_JSON = "data/processed/hsn.json"

def main():
    hsn_data = parse_hsn(INPUT_CSV)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(hsn_data, f, ensure_ascii=False, indent=2)

    print(f"HSN Entries Parsed: {len(hsn_data)}")

if __name__ == "__main__":
    main()
