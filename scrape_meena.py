#!/usr/bin/env python3
"""Scrape Meena Bazar category pages via backend API and export CSV + JSON."""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_URL = "https://meenabazaronline.com/category/fish"
DEFAULT_URLS_FILE = "urls_meena.txt"
DEFAULT_OUT_DIR = "outputs"
DEFAULT_CURRENCY = "BDT"
DEFAULT_AREA_ID = "802"
DEFAULT_SUBUNIT_ID = "1075"
DEFAULT_PAGE_SIZE = 40

API_BASE = "https://mbonlineapi.com/api/front"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

AMOUNT_PATTERN = re.compile(
    r"(?i)(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|grams|ml|l|lt|ltr|litre|liter|pcs?|pc|piece|pieces|pack|dozen|unit|bunch|bunches|bundle|bundles)\b"
)
AMOUNT_WORD_ONLY_PATTERN = re.compile(
    r"(?i)\b(each|pack|dozen|unit|bunch|bunches|bundle|bundles|piece|pieces|pc|pcs)\b"
)
RANGE_WITH_UNIT_PATTERN = re.compile(
    r"(?i)(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|grams|ml|l|lt|ltr|litre|liter|pcs?|pc|piece|pieces)\b"
)
SIZE_PATTERN = re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|grams|ml|l|lt|ltr|litre|liter)\b")
SIZE_RANGE_PATTERN = re.compile(
    r"(?i)(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|grams|ml|l|lt|ltr|litre|liter)\b"
)
SIZE_PLUS_PATTERN = re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|grams|ml|l|lt|ltr|litre|liter)\s*\+")
PIECE_COUNT_PATTERN = re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(pcs?|pc|pieces?)\b")

UNIT_ALIASES = {
    "gm": "g",
    "gram": "g",
    "grams": "g",
    "g": "g",
    "kg": "kg",
    "ml": "ml",
    "l": "l",
    "lt": "l",
    "ltr": "l",
    "litre": "l",
    "liter": "l",
    "pc": "pc",
    "pcs": "pc",
    "piece": "pc",
    "pieces": "pc",
    "unit": "unit",
    "pack": "pack",
    "dozen": "dozen",
    "bunch": "bundle",
    "bunches": "bundle",
    "bundle": "bundle",
    "bundles": "bundle",
    "each": "each",
}


@dataclass
class ProductRow:
    product_name: str
    regular_price: float | None
    discounted_price: float | None
    currency: str
    amount_raw: str | None
    quantity: float | None
    unit: str | None
    title_context_quantity: float | None
    title_context_unit: str | None
    title_context_note: str | None
    in_stock: bool
    source_url: str
    scraped_at_utc: str


def random_sleep(min_sec: float = 0.35, max_sec: float = 1.2) -> None:
    time.sleep(random.uniform(min_sec, max_sec))


def parse_amount(amount_raw: str | None, product_name: str | None = None) -> tuple[float | None, str | None]:
    # Meena API usually exposes unit in a dedicated field (e.g., "KG", "2kg").
    # Use amount_raw first, and only fall back to name parsing when needed.
    raw_amount = (amount_raw or "").strip().lower()
    raw_name = (product_name or "").strip().lower()

    # 1) Strongest signal: amount field contains explicit numeric size.
    if raw_amount:
        match = AMOUNT_PATTERN.search(raw_amount)
        if match:
            qty = float(match.group(1))
            unit = UNIT_ALIASES.get(match.group(2).lower(), "unknown")
            if unit == "each":
                return 1.0, "each"
            return qty, unit

    # 2) For generic amount fields like EACH/UNIT, try extracting size from name.
    if raw_name:
        range_match = RANGE_WITH_UNIT_PATTERN.search(raw_name)
        if range_match:
            low = float(range_match.group(1))
            high = float(range_match.group(2))
            unit = UNIT_ALIASES.get(range_match.group(3).lower(), "unknown")
            return (low + high) / 2.0, unit

        match = AMOUNT_PATTERN.search(raw_name)
        if match:
            qty = float(match.group(1))
            unit = UNIT_ALIASES.get(match.group(2).lower(), "unknown")
            if unit == "each":
                return 1.0, "each"
            return qty, unit

    # 3) Unit-only values from amount field (e.g., KG, EACH).
    if raw_amount and raw_amount in UNIT_ALIASES:
        unit = UNIT_ALIASES[raw_amount]
        if unit == "each":
            return 1.0, "each"
        if unit == "kg":
            return 1.0, "kg"
        return None, unit

    if raw_amount:
        word_match = AMOUNT_WORD_ONLY_PATTERN.search(raw_amount)
        if word_match:
            unit = UNIT_ALIASES.get(word_match.group(1).lower(), "unknown")
            if unit == "each":
                return 1.0, "each"
            return None, unit

    if raw_name:
        word_match = AMOUNT_WORD_ONLY_PATTERN.search(raw_name)
        if word_match:
            unit = UNIT_ALIASES.get(word_match.group(1).lower(), "unknown")
            if unit == "each":
                return 1.0, "each"
            return None, unit

    return None, "unknown"


def infer_numeric_amount_for_category(amount_raw: str | None, category_slug: str) -> tuple[float | None, str | None]:
    raw = (amount_raw or "").strip().lower()
    if not raw:
        return None, None
    if not re.fullmatch(r"\d+(?:\.\d+)?", raw):
        return None, None
    # Meena occasionally sends bare numeric amounts in produce categories (e.g., "250").
    # Treat these as grams to avoid losing quantity/unit while keeping scope narrow.
    if category_slug in {"fruits", "vegetables"}:
        return float(raw), "g"
    return None, None


def parse_size_from_title(product_name: str | None) -> tuple[float | None, str | None, str | None]:
    if not product_name:
        return None, None, None
    text = product_name.strip()
    if not text:
        return None, None, None

    range_match = SIZE_RANGE_PATTERN.search(text)
    if range_match:
        low = float(range_match.group(1))
        high = float(range_match.group(2))
        unit = UNIT_ALIASES.get(range_match.group(3).lower(), "unknown")
        return (low + high) / 2.0, unit, f"range:{low}-{high} {unit}"

    plus_match = SIZE_PLUS_PATTERN.search(text)
    if plus_match:
        qty = float(plus_match.group(1))
        unit = UNIT_ALIASES.get(plus_match.group(2).lower(), "unknown")
        return qty, unit, "plus"

    match = SIZE_PATTERN.search(text)
    if match:
        qty = float(match.group(1))
        unit = UNIT_ALIASES.get(match.group(2).lower(), "unknown")
        return qty, unit, None

    return None, None, None


def parse_piece_count_from_title(product_name: str | None) -> tuple[float | None, str | None, str | None]:
    if not product_name:
        return None, None, None
    text = product_name.strip()
    if not text:
        return None, None, None
    match = PIECE_COUNT_PATTERN.search(text)
    if not match:
        return None, None, None
    return float(match.group(1)), "pc", None


def derive_title_context(sell_unit: str | None, product_name: str | None) -> tuple[float | None, str | None, str | None]:
    unit = (sell_unit or "").strip().lower()
    if unit in {"pc", "each"}:
        return parse_size_from_title(product_name)
    if unit in {"g", "kg", "ml", "l"}:
        return parse_piece_count_from_title(product_name)
    return None, None, None


def parse_bool(value: str) -> bool:
    value_norm = value.strip().lower()
    if value_norm in {"1", "true", "t", "yes", "y"}:
        return True
    if value_norm in {"0", "false", "f", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")


def post_json(path: str, payload: dict) -> dict:
    url = f"{API_BASE}{path}"
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
            "Origin": "https://meenabazaronline.com",
            "Referer": "https://meenabazaronline.com/",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw)


def get_json(path: str) -> dict:
    url = f"{API_BASE}{path}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
            "Referer": "https://meenabazaronline.com/",
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw)


def extract_slug_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        raise ValueError(f"Cannot parse category slug from URL: {url}")
    return parts[-1].strip().lower()


def get_category_map() -> dict[str, int]:
    payload = get_json("/nav/categories/list")
    data = payload.get("data") or []
    result: dict[str, int] = {}
    for row in data:
        slug = str(row.get("CategorySlug") or "").strip().lower()
        category_id = row.get("ItemCategoryId")
        if slug and category_id is not None:
            result[slug] = int(category_id)
    return result


def normalize_prices(item: dict) -> tuple[float | None, float | None]:
    unit_price = item.get("UnitSalesPrice")
    discount_price = item.get("DiscountSalesPrice")
    unit_discount = item.get("UnitDiscount")

    regular_price = float(unit_price) if unit_price is not None else None
    discounted_price = None

    if unit_discount and discount_price is not None:
        discounted_price = float(discount_price)
        if regular_price is not None and discounted_price > regular_price:
            discounted_price, regular_price = regular_price, discounted_price
        if regular_price == discounted_price:
            discounted_price = None

    return regular_price, discounted_price


def scrape_category(
    source_url: str,
    category_slug: str,
    category_id: int,
    area_id: str,
    subunit_id: str,
    page_size: int,
) -> list[ProductRow]:
    started_at = datetime.now(timezone.utc).isoformat()
    rows: list[ProductRow] = []
    seen_item_ids: set[int] = set()
    start_sl = 1
    total_items = None

    while True:
        payload = {
            "StartSl": start_sl,
            "NoOfItem": page_size,
            "SearchSlug": category_slug,
            "CategoryId": [category_id],
            "ThumbSize": "lg",
            "SubUnitId": str(subunit_id),
            "AreaId": str(area_id),
            "BrandId": [],
            "SearchType": "C",
            "SubCategoryId": [],
        }
        response = post_json(f"/product/category/{category_slug}", payload)
        data = (response.get("data") or {}).get("Category") or []
        if not data:
            break

        if total_items is None:
            try:
                total_items = int(data[0].get("TotalItem"))
            except Exception:
                total_items = None

        added_this_page = 0
        for item in data:
            item_id = item.get("ItemId")
            if item_id is None:
                continue
            item_id = int(item_id)
            if item_id in seen_item_ids:
                continue
            seen_item_ids.add(item_id)
            added_this_page += 1

            product_name = str(item.get("ItemDisplayName") or item.get("ItemDescription") or "").strip()
            if not product_name:
                continue

            regular_price, discounted_price = normalize_prices(item)
            amount_raw = str(item.get("Unit") or "").strip() or None
            quantity, unit = parse_amount(amount_raw, product_name)
            if unit == "unknown":
                inferred_qty, inferred_unit = infer_numeric_amount_for_category(amount_raw, category_slug)
                if inferred_unit is not None:
                    quantity, unit = inferred_qty, inferred_unit
            title_context_quantity, title_context_unit, title_context_note = derive_title_context(unit, product_name)
            in_stock = float(item.get("StockQuantity") or 0) > 0

            rows.append(
                ProductRow(
                    product_name=product_name,
                    regular_price=regular_price,
                    discounted_price=discounted_price,
                    currency=DEFAULT_CURRENCY,
                    amount_raw=amount_raw,
                    quantity=quantity,
                    unit=unit,
                    title_context_quantity=title_context_quantity,
                    title_context_unit=title_context_unit,
                    title_context_note=title_context_note,
                    in_stock=in_stock,
                    source_url=source_url,
                    scraped_at_utc=started_at,
                )
            )

        if added_this_page == 0:
            break
        start_sl += page_size
        if total_items is not None and start_sl > total_items:
            break
        random_sleep()

    return rows


def read_urls_file(path: str) -> list[str]:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"URLs file not found: {path}")

    urls: list[str] = []
    with file_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            urls.append(raw)

    if not urls:
        raise ValueError(f"No URLs found in file: {path}")
    return urls


def dedupe_rows(rows: list[ProductRow]) -> list[ProductRow]:
    seen: set[tuple[str, str, str | None]] = set()
    deduped: list[ProductRow] = []
    for row in rows:
        key = (row.source_url, row.product_name, row.amount_raw)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def write_outputs(rows: list[ProductRow], out_dir: str) -> tuple[Path, Path]:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"meena_products_{stamp}.json"
    csv_path = output_dir / f"meena_products_{stamp}.csv"

    payload = [asdict(row) for row in rows]
    with json_path.open("w", encoding="utf-8") as jf:
        json.dump(payload, jf, ensure_ascii=False, indent=2)

    fieldnames = list(ProductRow.__dataclass_fields__.keys())
    with csv_path.open("w", encoding="utf-8", newline="") as cf:
        writer = csv.DictWriter(cf, fieldnames=fieldnames)
        writer.writeheader()
        for row in payload:
            writer.writerow(row)

    return csv_path, json_path


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape Meena Bazar category pages")
    parser.add_argument("--url", help="Single category URL to scrape")
    parser.add_argument("--urls-file", help="Path to a .txt file with one URL per line")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output directory for CSV and JSON")
    parser.add_argument("--area-id", default=DEFAULT_AREA_ID, help="Meena area id (default: 802)")
    parser.add_argument("--subunit-id", default=DEFAULT_SUBUNIT_ID, help="Meena subunit id (default: 1075)")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="API batch size per request")
    parser.add_argument(
        "--headless",
        type=parse_bool,
        default=True,
        help="Compatibility arg; unused for API scraper (kept true/false for automation parity)",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    started = time.time()

    if args.urls_file:
        try:
            target_urls = read_urls_file(args.urls_file)
        except (FileNotFoundError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
    elif args.url:
        target_urls = [args.url]
    elif Path(DEFAULT_URLS_FILE).exists():
        try:
            target_urls = read_urls_file(DEFAULT_URLS_FILE)
        except (FileNotFoundError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
    else:
        target_urls = [DEFAULT_URL]

    try:
        category_map = get_category_map()
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError, ValueError) as exc:
        print(f"ERROR: Failed to load category map: {exc}", file=sys.stderr)
        return 1

    rows: list[ProductRow] = []
    failed_urls: list[str] = []

    for idx, url in enumerate(target_urls, start=1):
        print(f"[{idx}/{len(target_urls)}] Scraping {url}")
        try:
            slug = extract_slug_from_url(url)
            category_id = category_map.get(slug)
            if category_id is None:
                raise ValueError(f"No category id mapping found for slug '{slug}'")
            rows.extend(
                scrape_category(
                    source_url=url,
                    category_slug=slug,
                    category_id=category_id,
                    area_id=args.area_id,
                    subunit_id=args.subunit_id,
                    page_size=args.page_size,
                )
            )
        except Exception as exc:  # noqa: BLE001
            failed_urls.append(url)
            print(f"WARNING: Failed to scrape {url}: {exc}", file=sys.stderr)
        random_sleep(0.9, 2.2)

    rows = dedupe_rows(rows)
    if not rows:
        print("ERROR: No products scraped. Exiting non-zero for automation alert.", file=sys.stderr)
        return 1

    csv_path, json_path = write_outputs(rows, args.out_dir)
    duration = time.time() - started

    print(f"Scraped {len(rows)} products from {len(target_urls)} URL(s).")
    if failed_urls:
        print(f"Failed URLs: {len(failed_urls)}")
        for url in failed_urls:
            print(f"  - {url}")
    print(f"CSV:  {csv_path}")
    print(f"JSON: {json_path}")
    print(f"Duration: {duration:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
