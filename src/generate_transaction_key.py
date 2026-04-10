from src.normalize_record import NormalizedRecord, compute_record_hash


def generate_transaction_key(source_filename: str, record: NormalizedRecord) -> str:
    base = f"{source_filename}|{record.source_line}|{record.normalized_text}"
    return compute_record_hash(base)