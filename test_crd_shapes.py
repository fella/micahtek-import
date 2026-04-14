from src.parse_crd import parse_crd_file, debug_crd_shapes

file_path = "./fixtures/01015642.crd"

raw_records = parse_crd_file(file_path)
debug_crd_shapes(raw_records)
