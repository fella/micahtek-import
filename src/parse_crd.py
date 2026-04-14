from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
from src.crd_headers import STANDARD_CRD_HEADERS_56, EXTENDED_CRD_HEADERS_64
import csv

ITEM_GROUP_BASE_FIELDS = [
    "item",
    "description",
    "quantity",
    "unit_retail",
    "gross_price_unit_retail_times_quantity",
    "discount",
    "sales_tax",
    "net_price_gross_minus_discount_plus_sales_tax",
]

ITEM_GROUP_FIELDS = [
    "ITEM",
    "DESCRIPTION",
    "QUANTITY",
    "UNIT_RETAIL",
    "GROSS_PRICE_UNIT_RETAIL_TIMES_QUANTITY",
    "DISCOUNT",
    "SALES_TAX",
    "NET_PRICE_GROSS_MINUS_DISCOUNT_PLUS_SALES_TAX",
]

# See push commit is verified


@dataclass(frozen=True)
class RawRecord:
    source_line: int
    raw_text: str

def parse_crd_file(file_path: str) -> List[RawRecord]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CRD file not found: {file_path}")

    records: List[RawRecord] = []

    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for index, line in enumerate(handle, start=1):
            raw = line.rstrip("\r\n")
            if not raw.strip():
                continue
            records.append(RawRecord(source_line=index, raw_text=raw))

    return records

import csv
from collections import Counter


def debug_crd_shapes(raw_records):
    counts = Counter()
    examples = {}

    for record in raw_records:
        values = next(csv.reader([record.raw_text]))
        count = len(values)
        counts[count] += 1
        examples.setdefault(count, (record.source_line, values[:10], record.raw_text))

    print("FIELD COUNT DISTRIBUTION")
    for count in sorted(counts):
        line_no, preview, raw = examples[count]
        print(f"{count}: {counts[count]} rows; first seen at line {line_no}")
        print(f"  preview: {preview}")
        print(f"  raw: {raw[:300]}")
        print()

def _normalize_header(name: str) -> str:
    normalized = name.strip().lower()
    normalized = normalized.replace("#", "number")
    normalized = normalized.replace("/", "_")
    normalized = normalized.replace(" ", "_")

    while "__" in normalized:
        normalized = normalized.replace("__", "_")

    return normalized.strip("_")


def _extract_item_groups(row_dict: Dict[str, str]) -> List[Dict[str, str]]:
    item = {
        "item": row_dict.get("item", ""),
        "description": row_dict.get("description", ""),
        "quantity": row_dict.get("quantity", ""),
        "unit_retail": row_dict.get("unit_retail", ""),
        "gross_price_unit_retail_times_quantity": row_dict.get(
            "gross_price_unit_retail_times_quantity", ""
        ),
        "discount": row_dict.get("discount", ""),
        "sales_tax": row_dict.get("sales_tax", ""),
        "net_price_gross_minus_discount_plus_sales_tax": row_dict.get(
            "net_price_gross_minus_discount_plus_sales_tax", ""
        ),
    }

    if any(value.strip() for value in item.values()):
        return [item]

    return []

import csv
from typing import Any, Dict, List

from src.crd_headers import STANDARD_CRD_HEADERS_56, EXTENDED_CRD_HEADERS_64

def _parse_with_headers(values: List[str], headers: List[str]) -> Dict[str, Any]:
    row: Dict[str, str] = {}

    for header, value in zip(headers, values):
        row[_normalize_header(header)] = value.strip()

    items = []

    item1 = {
        "item": row.get("item", ""),
        "description": row.get("description", ""),
        "quantity": row.get("quantity", ""),
        "unit_retail": row.get("unit_retail", ""),
        "gross_price_unit_retail_times_quantity": row.get(
            "gross_price_unit_retail_times_quantity", ""
        ),
        "discount": row.get("discount", ""),
        "sales_tax": row.get("sales_tax", ""),
        "net_price_gross_minus_discount_plus_sales_tax": row.get(
            "net_price_gross_minus_discount_plus_sales_tax", ""
        ),
    }
    if any(v.strip() for v in item1.values()):
        items.append(item1)

    item2 = {
        "item": row.get("item_2", ""),
        "description": row.get("description_2", ""),
        "quantity": row.get("quantity_2", ""),
        "unit_retail": row.get("unit_retail_2", ""),
        "gross_price_unit_retail_times_quantity": row.get(
            "gross_price_unit_retail_times_quantity_2", ""
        ),
        "discount": row.get("discount_2", ""),
        "sales_tax": row.get("sales_tax_2", ""),
        "net_price_gross_minus_discount_plus_sales_tax": row.get(
            "net_price_gross_minus_discount_plus_sales_tax_2", ""
        ),
    }
    if any(v.strip() for v in item2.values()):
        items.append(item2)

    row["items"] = items
    return row

def parse_crd_rows(raw_records: List[RawRecord]) -> List[Dict[str, Any]]:
    parsed_rows: List[Dict[str, Any]] = []

    for record in raw_records:
        try:
            row = parse_crd_row(record.raw_text)
            row["source_line"] = record.source_line
            parsed_rows.append(row)
        except Exception as exc:
            raise ValueError(
                f"Failed on source_line={record.source_line}: {exc}\n"
                f"RAW: {record.raw_text}"
            ) from exc

    return parsed_rows