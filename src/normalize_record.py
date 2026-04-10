from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import re

from src.parse_crd import RawRecord


@dataclass(frozen=True)
class NormalizedRecord:
    source_line: int
    raw_text: str
    donor_identifier: str
    amount: Decimal | None
    normalized_text: str


def normalize_donor_identifier(raw_text: str) -> str:
    lowered = raw_text.lower()
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered[:255]


def try_extract_amount(raw_text: str) -> Decimal | None:
    matches = re.findall(r"\d+\.\d{2}", raw_text)
    if not matches:
        return None

    try:
        return Decimal(matches[0])
    except (InvalidOperation, IndexError):
        return None


def normalize_record(record: RawRecord) -> NormalizedRecord:
    normalized_text = re.sub(r"\s+", " ", record.raw_text).strip()
    donor_identifier = normalize_donor_identifier(record.raw_text)
    amount = try_extract_amount(record.raw_text)

    return NormalizedRecord(
        source_line=record.source_line,
        raw_text=record.raw_text,
        donor_identifier=donor_identifier,
        amount=amount,
        normalized_text=normalized_text,
    )


def compute_record_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()