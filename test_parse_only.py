from src.parse_crd import parse_crd_file

records = parse_crd_file("home/daves/projects/micahtek-import/src/fixtures/01015642.crd")
print(f"Loaded {len(records)} records")
 
for record in records[:5]:
    print(record.source_line, record.raw_text[:120])
    