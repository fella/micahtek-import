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
        raise FileNotFoundError(f"crd file not found: {file_path}")

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


#Should I remove this as clutter?
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

def _parse_short_row(values: List[str]) -> Dict[str, Any]:
    values = [v.strip() for v in values]
    padded = values + [""] * (25 - len(values))

    return {
        "record_type": "short",
        "control_number": padded[0],
        "status_codes": padded[16],
        "client_number": padded[17],
        "operator_number": padded[18],
        "date_of_call_mmddyy": padded[19],
        "time_of_call_hhmmss_24hour_clock": padded[20],
        "call_type_information_product_referral_custsvc_hangupprank": padded[21],
        "call_letters_media_source": padded[22],
        "comments": padded[23],
        "items": [],
        "raw_values": values,
    }

def parse_crd_row(raw_text: str) -> Dict[str, Any]:
    values = [v.strip() for v in next(csv.reader([raw_text]))]
    field_count = len(values)

    if field_count == 25:
        return _parse_short_row(values)

    if field_count == len(STANDARD_CRD_HEADERS_56):
        row = _parse_with_headers(values, STANDARD_CRD_HEADERS_56)
        row["record_type"] = "standard"
        return row

    if field_count == len(EXTENDED_CRD_HEADERS_64):
        row = _parse_with_headers(values, EXTENDED_CRD_HEADERS_64)
        row["record_type"] = "extended_2_item"
        return row

    if field_count in (72, 88, 112):
        return _parse_multi_item_row(values)

    raise ValueError(f"Unsupported CRD row shape: {field_count} fields")

def _build_item(chunk: List[str]) -> Dict[str, str]:
    padded = [v.strip() for v in chunk] + [""] * (8 - len(chunk))
    return {
        "item": padded[0],
        "description": padded[1],
        "quantity": padded[2],
        "unit_retail": padded[3],
        "gross_price_unit_retail_times_quantity": padded[4],
        "discount": padded[5],
        "sales_tax": padded[6],
        "net_price_gross_minus_discount_plus_sales_tax": padded[7],
    }

def _parse_multi_item_row(values: List[str]) -> Dict[str, Any]:
    values = [v.strip() for v in values]
    row: Dict[str, Any] = {}

    # Base metadata from the 56-field layout, excluding the first item block and trailing field
    for header, value in zip(STANDARD_CRD_HEADERS_56[:47], values[:47]):
        row[_normalize_header(header)] = value

    items: List[Dict[str, str]] = []

    # First item block from the standard layout
    first_item = _build_item(values[47:55])
    if any(v.strip() for v in first_item.values()):
        items.append(first_item)

    # Additional 8-field item groups after the standard trailing slot
    extra_values = values[56:]
    for i in range(0, len(extra_values), 8):
        item = _build_item(extra_values[i:i + 8])
        if any(v.strip() for v in item.values()):
            items.append(item)

def add_item_group(chunk: List[str]) -> None:
    while len(chunk) < 8:
        chunk.append("")
    item = {
        "item": chunk[0],
        "description": chunk[1],
        "quantity": chunk[2],
        "unit_retail": chunk[3],
        "gross_price_unit_retail_times_quantity": chunk[4],
        "discount": chunk[5],
        "sales_tax": chunk[6],
        "net_price_gross_minus_discount_plus_sales_tax": chunk[7],
    }
    if any(v.strip() for v in item.values()):
        items.append(item)

    # first item is already embedded in the base 56 layout
    add_item_group(values[47:55])

    extra = values[56:]
    for i in range(0, len(extra), 8):
        add_item_group(extra[i:i+8])

    row["items"] = items
    row["record_type"] = f"multi_item_{len(items)}"
    return row

def parse_crd_rows(raw_records: List[RawRecord]) -> List[Dict[str, Any]]:
    parsed_rows: List[Dict[str, Any]] = []

    for record in raw_records:
        try:
            row = parse_crd_row(record.raw_text)
            row["source_line"] = record.source_line
            parsed_rows.append(row)
        except Exception as exc:
            print(f"Skipping source_line={record.source_line}: {exc}")

    return parsed_rows