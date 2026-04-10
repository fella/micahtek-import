from pathlib import Path

from src.parse_crd import parse_crd_file


def test_parse_crd_file_reads_non_empty_lines(tmp_path: Path) -> None:
    sample = tmp_path / "01015642.CRD"
    sample.write_text("line one\n\nline two\n", encoding="utf-8")

    records = parse_crd_file(str(sample))

    assert len(records) == 2
    assert records[0].source_line == 1
    assert records[0].raw_text == "line one"
    assert records[1].source_line == 3
    assert records[1].raw_text == "line two"