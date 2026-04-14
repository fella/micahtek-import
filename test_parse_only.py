from pprint import pprint
from src.parse_crd import parse_crd_file, parse_crd_rows

file_path = "./fixtures/01015642.crd"

raw_records = parse_crd_file(file_path)
print(f"Loaded {len(raw_records)} raw records")

rows = parse_crd_rows(raw_records)
print(f"Parsed {len(rows)} rows")

for row in rows[:3]:
    pprint(row)
    print("-" * 80)