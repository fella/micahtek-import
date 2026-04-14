from pprint import pprint
from src.parse_crd import parse_crd_file, parse_crd_row

file_path = "./fixtures/01015642.crd"
raw_records = parse_crd_file(file_path)

for target_line in [17, 295, 424, 600, 615]:
    record = next(r for r in raw_records if r.source_line == target_line)
    row = parse_crd_row(record.raw_text)
    print(f"source_line={record.source_line}")
    pprint(row)
    print("-" * 80)