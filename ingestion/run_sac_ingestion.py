import json
from parse_sac import parse_sac

INPUT_CSV = "data/raw/csv/sac_codes.csv"
OUTPUT_JSON = "data/processed/sac.json"

def main():
    sac_data = parse_sac(INPUT_CSV)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(sac_data, f, ensure_ascii=False, indent=2)

    print(f"SAC Entries Parsed: {len(sac_data)}")

if __name__ == "__main__":
    main()
