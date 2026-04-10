from pathlib import Path

from src.parse_crd import parse_crd_file


def test_sample_file_exists() -> None:
    sample = Path("sample-data/inbound/01015642.CRD")
    assert sample.exists()


def test_sample_file_is_readable() -> None:
    records = parse_crd_file("sample-data/inbound/01015642.CRD")
    assert len(records) >= 1