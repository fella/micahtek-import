from decimal import Decimal, InvalidOperation
from src.parse_crd import parse_crd_file, parse_crd_rows

def d(value: str) -> Decimal:
    try:
        return Decimal((value or "").strip() or "0")
    except InvalidOperation:
        return Decimal("0")

raw_records = parse_crd_file("./fixtures/01015642.crd")
rows = parse_crd_rows(raw_records)

assert len(raw_records) == 623
assert len(rows) == 623

problems = []

for row in rows:
    items = row.get("items", [])
    item_count = row.get("item_count", "").strip()

    if item_count.isdigit() and int(item_count) != len(items):
        problems.append(
            (row["source_line"], "item_count_mismatch", item_count, len(items))
        )

    if items:
        item_total = sum(
            d(item.get("net_price_gross_minus_discount_plus_sales_tax", "0"))
            for item in items
        )
        order_total = d(row.get("order_total_0000_dot_00", "0"))

        if order_total != 0 and abs(item_total - order_total) > Decimal("0.05"):
            problems.append(
                (row["source_line"], "money_mismatch", str(order_total), str(item_total))
            )

print(f"raw_records={len(raw_records)} parsed_rows={len(rows)} problems={len(problems)}")
for p in problems[:50]:
    print(p)
