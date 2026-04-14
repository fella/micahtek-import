from src.parse_crd import parse_crd_file

file_path = "fixtures/01015642.crd"   # change this to your real file

records = parse_crd_file(file_path)
print(f"Loaded {len(records)} records")

for record in records[:5]:
    print(record.source_line, record.raw_text[:120])