#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

FIXED_FIELD_NAMES = [
    "record_id",
    "title",
    "first_name",
    "middle_name",
    "last_name",
    "suffix",
    "salutation_first_name",
    "organization_name",
    "department",
    "address_1",
    "city",
    "state",
    "postal_code",
    "country",
    "phone",
    "email",
    "record_prefix",
    "station_code",
    "batch_code",
    "gift_date",
    "gift_time",
    "record_status_code",
    "source_code",
    "comment",
    "reserved_24",
    "reserved_25",
    "reserved_26",
    "reserved_27",
    "reserved_28",
    "reserved_29",
    "reserved_30",
    "reserved_31",
    "payment_method",
    "fulfillment_code",
    "card_brand",
    "card_masked_number",
    "card_expiration",
    "card_cvv_masked",
    "cardholder_name",
    "ach_routing_masked",
    "ach_account_masked",
    "reserved_41",
    "gross_amount",
    "item_count",
    "transaction_status",
    "net_amount",
    "first_item_code",
]

ITEM_FIELD_NAMES = [
    "item_code",
    "item_description",
    "quantity",
    "unit_amount",
    "line_amount",
    "discount_amount",
    "tax_amount",
    "total_amount",
]

SHORT_RECORD_FIELD_NAMES = [
    "record_id",
    "title",
    "first_name",
    "middle_name",
    "last_name",
    "suffix",
    "salutation_first_name",
    "organization_name",
    "department",
    "address_1",
    "city",
    "state",
    "postal_code",
    "country",
    "phone",
    "email",
    "record_prefix",
    "station_code",
    "batch_code",
    "event_date",
    "event_time",
    "record_status_code",
    "source_code",
    "comment",
    "reserved_24",
]

MONEY_FIELDS = {"gross_amount", "net_amount", "unit_amount", "line_amount", "discount_amount", "tax_amount", "total_amount"}
INT_FIELDS = {"item_count", "quantity"}


@dataclass
class ParseStats:
    total_rows: int = 0
    transaction_rows: int = 0
    short_rows: int = 0
    unknown_rows: int = 0
    item_rows: int = 0
    output_records: int = 0


def strip_value(value: str) -> str:
    return value.strip()


def normalize_text(value: str) -> str | None:
    value = strip_value(value)
    return value or None


def normalize_money(value: str) -> str | None:
    value = strip_value(value)
    if not value:
        return None
    try:
        return format(Decimal(value), ".2f")
    except InvalidOperation:
        return value


def normalize_int(value: str) -> int | str | None:
    value = strip_value(value)
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return value


def build_dict(field_names: list[str], values: list[str]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for name, raw in zip(field_names, values):
        raw = strip_value(raw)
        if name in MONEY_FIELDS:
            data[name] = normalize_money(raw)
        elif name in INT_FIELDS:
            data[name] = normalize_int(raw)
        else:
            data[name] = normalize_text(raw)
    return data


def parse_items(row: list[str]) -> list[dict[str, Any]]:
    item_segment = row[47:]
    while item_segment and strip_value(item_segment[-1]) == "":
        item_segment.pop()
    items: list[dict[str, Any]] = []
    for i in range(0, len(item_segment), 8):
        chunk = item_segment[i:i + 8]
        if len(chunk) < 8:
            chunk += [""] * (8 - len(chunk))
        item = build_dict(ITEM_FIELD_NAMES, chunk)
        if any(v not in (None, "", 0) for v in item.values()):
            items.append(item)
    return items


def classify_record(record: dict[str, Any]) -> str:
    payment_method = (record.get("payment_method") or "").upper()
    source_code = (record.get("source_code") or "").upper()
    status = (record.get("transaction_status") or "").upper()

    if payment_method == "CREDIT CARD":
        return "credit_card"
    if payment_method == "CASH/CHECK":
        return "cash_check"
    if source_code == "IVR":
        return "ivr"
    if status == "CLEARED":
        return "cleared_transaction"
    return "transaction"


def parse_transaction_row(row: list[str]) -> dict[str, Any]:
    fixed = build_dict(FIXED_FIELD_NAMES, row[:47])
    first_item_code = fixed.pop("first_item_code")
    items = parse_items(row)
    if first_item_code and items and not items[0].get("item_code"):
        items[0]["item_code"] = first_item_code
    elif first_item_code and not items:
        items = [{
            "item_code": first_item_code,
            "item_description": None,
            "quantity": None,
            "unit_amount": None,
            "line_amount": None,
            "discount_amount": None,
            "tax_amount": None,
            "total_amount": None,
        }]

    record = {
        "record_kind": "transaction",
        "record_class": classify_record(fixed),
        "fixed_fields": fixed,
        "items": items,
        "raw_column_count": len(row),
        "raw_row": row,
    }
    return record


def parse_short_row(row: list[str]) -> dict[str, Any]:
    data = build_dict(SHORT_RECORD_FIELD_NAMES, row)
    return {
        "record_kind": "short_record",
        "record_class": "event",
        "fixed_fields": data,
        "items": [],
        "raw_column_count": len(row),
        "raw_row": row,
    }


def parse_rows(rows: Iterable[list[str]]) -> tuple[list[dict[str, Any]], ParseStats]:
    records: list[dict[str, Any]] = []
    stats = ParseStats()
    for row_number, row in enumerate(rows, start=1):
        stats.total_rows += 1
        if len(row) >= 56 and (len(row) - 48) % 8 == 0:
            parsed = parse_transaction_row(row)
            stats.transaction_rows += 1
            stats.item_rows += len(parsed["items"])
        elif len(row) == 25:
            parsed = parse_short_row(row)
            stats.short_rows += 1
        else:
            parsed = {
                "record_kind": "unknown",
                "record_class": "unknown",
                "fixed_fields": {f"col_{i}": normalize_text(v) for i, v in enumerate(row)},
                "items": [],
                "raw_column_count": len(row),
                "raw_row": row,
            }
            stats.unknown_rows += 1
        parsed["row_number"] = row_number
        records.append(parsed)
    stats.output_records = len(records)
    return records, stats


def read_rows(path: Path, encoding: str) -> list[list[str]]:
    with path.open("r", encoding=encoding, newline="") as handle:
        return list(csv.reader(handle))


def write_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_transactions_csv(path: Path, records: list[dict[str, Any]]) -> None:
    fieldnames = ["row_number", "record_kind", "record_class", *FIXED_FIELD_NAMES[:-1], "parsed_item_count"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            if record["record_kind"] != "transaction":
                continue
            row = {
                "row_number": record["row_number"],
                "record_kind": record["record_kind"],
                "record_class": record["record_class"],
                "parsed_item_count": len(record["items"]),
            }
            row.update(record["fixed_fields"])
            writer.writerow(row)


def write_items_csv(path: Path, records: list[dict[str, Any]]) -> None:
    fieldnames = ["row_number", "record_id", "item_index", *ITEM_FIELD_NAMES]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            if record["record_kind"] != "transaction":
                continue
            record_id = record["fixed_fields"].get("record_id")
            for item_index, item in enumerate(record["items"], start=1):
                row = {"row_number": record["row_number"], "record_id": record_id, "item_index": item_index}
                row.update(item)
                writer.writerow(row)


def write_short_csv(path: Path, records: list[dict[str, Any]]) -> None:
    fieldnames = ["row_number", "record_kind", "record_class", *SHORT_RECORD_FIELD_NAMES]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            if record["record_kind"] != "short_record":
                continue
            row = {
                "row_number": record["row_number"],
                "record_kind": record["record_kind"],
                "record_class": record["record_class"],
            }
            row.update(record["fixed_fields"])
            writer.writerow(row)


def write_summary(path: Path, stats: ParseStats, encoding: str, input_path: Path) -> None:
    summary = {
        "input_file": str(input_path),
        "encoding": encoding,
        "total_rows": stats.total_rows,
        "transaction_rows": stats.transaction_rows,
        "short_rows": stats.short_rows,
        "unknown_rows": stats.unknown_rows,
        "parsed_item_rows": stats.item_rows,
        "output_records": stats.output_records,
        "transaction_shape": "47 fixed fields + 8*N item fields (+ optional trailing blank)",
        "short_shape": "25 fields",
    }
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def run_self_test(sample_path: Path, encoding: str) -> dict[str, Any]:
    rows = read_rows(sample_path, encoding)
    records, stats = parse_rows(rows)

    assert stats.total_rows == 537, f"expected 537 rows, got {stats.total_rows}"
    assert stats.transaction_rows == 199, f"expected 199 transaction rows, got {stats.transaction_rows}"
    assert stats.short_rows == 338, f"expected 338 short rows, got {stats.short_rows}"
    assert stats.unknown_rows == 0, f"expected 0 unknown rows, got {stats.unknown_rows}"
    assert stats.item_rows == 214, f"expected 214 parsed item rows, got {stats.item_rows}"

    first_tx = next(r for r in records if r["record_kind"] == "transaction")
    assert first_tx["fixed_fields"]["record_id"] == "61537277"
    assert first_tx["fixed_fields"]["payment_method"] == "CASH/CHECK"
    assert len(first_tx["items"]) == 1
    assert first_tx["items"][0]["item_code"] == "GP"

    first_short = next(r for r in records if r["record_kind"] == "short_record")
    assert first_short["fixed_fields"]["record_id"] == "61537016"
    assert first_short["fixed_fields"]["record_status_code"] == "H"
    assert first_short["fixed_fields"]["source_code"] == "IVR"

    long_tx = next(r for r in records if r["fixed_fields"].get("record_id") == "61538104")
    assert len(long_tx["items"]) == 3
    assert long_tx["items"][2]["line_amount"] == "6.98"

    return {
        "status": "passed",
        "assertions": 11,
        "summary": {
            "total_rows": stats.total_rows,
            "transaction_rows": stats.transaction_rows,
            "short_rows": stats.short_rows,
            "unknown_rows": stats.unknown_rows,
            "item_rows": stats.item_rows,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse MicahTek-style .CRD exports into JSONL and CSV files.")
    parser.add_argument("input", type=Path, help="Path to the .CRD file")
    parser.add_argument("--encoding", default="cp1252", help="Input file encoding. Default: cp1252")
    parser.add_argument("--out-dir", type=Path, default=Path("."), help="Directory for generated outputs")
    parser.add_argument("--jsonl", default="parsed_records.jsonl", help="JSONL output filename")
    parser.add_argument("--transactions-csv", default="transactions.csv", help="Transactions CSV output filename")
    parser.add_argument("--items-csv", default="transaction_items.csv", help="Transaction items CSV output filename")
    parser.add_argument("--short-csv", default="non_transaction_records.csv", help="Short/non-transaction CSV output filename")
    parser.add_argument("--summary-json", default="parse_summary.json", help="Summary JSON output filename")
    parser.add_argument("--self-test", action="store_true", help="Run built-in assertions against the input file before writing outputs")
    args = parser.parse_args()

    if args.self_test:
        result = run_self_test(args.input, args.encoding)
        print(json.dumps(result, indent=2))

    rows = read_rows(args.input, args.encoding)
    records, stats = parse_rows(rows)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(args.out_dir / args.jsonl, records)
    write_transactions_csv(args.out_dir / args.transactions_csv, records)
    write_items_csv(args.out_dir / args.items_csv, records)
    write_short_csv(args.out_dir / args.short_csv, records)
    write_summary(args.out_dir / args.summary_json, stats, args.encoding, args.input)

    print(json.dumps({
        "status": "ok",
        "input": str(args.input),
        "out_dir": str(args.out_dir),
        "total_rows": stats.total_rows,
        "transaction_rows": stats.transaction_rows,
        "short_rows": stats.short_rows,
        "unknown_rows": stats.unknown_rows,
        "item_rows": stats.item_rows,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
