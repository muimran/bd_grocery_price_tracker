#!/usr/bin/env python3
"""Scrape Chaldal category pages and export product data to CSV + JSON."""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

DEFAULT_URL = "https://chaldal.com/fresh-vegetable"
DEFAULT_URLS_FILE = "urls_chaldal.txt"
DEFAULT_OUT_DIR = "outputs"
DEFAULT_CURRENCY = "BDT"
DEFAULT_HEADLESS = True
PAGE_RETRIES = 2

# Keep selectors centralized for easy maintenance when markup changes.
CARD_SELECTORS = [
    "div.productV2Catalog",
    "div.product",
]
NAME_SELECTORS = [
    ".pName .nameTextWithEllipsis",
    ".pName",
    "p[class*='nameText']",
]
SUBTEXT_SELECTORS = [
    ".subText",
    "div[class*='subText']",
]
DISCOUNT_PRICE_SELECTORS = [
    ".productV2discountedPrice > .currentry > span",
    ".productV2discountedPrice > .currentry",
    "div[class*='discountedPrice'] > div[class*='currentry'] > span",
    "div[class*='discountedPrice'] > div[class*='currency'] > span",
    "div[class*='discountedPrice'] > div[class*='currentry']",
    "div[class*='discountedPrice'] > div[class*='currency']",
    ".discountedPrice > .currency span",
    ".discountedPrice > .currency",
]
REGULAR_PRICE_IN_DISCOUNT_SELECTORS = [
    ".productV2discountedPrice > .price > .currentry > span",
    ".productV2discountedPrice > .price > .currentry",
    "div[class*='discountedPrice'] div[class*='price'] div[class*='currentry'] > span",
    "div[class*='discountedPrice'] div[class*='price'] div[class*='currency'] > span",
    "div[class*='discountedPrice'] div[class*='price'] div[class*='currentry']",
    "div[class*='discountedPrice'] div[class*='price'] div[class*='currency']",
    ".discountedPrice .price .currency span",
    ".discountedPrice .price .currency",
]
SINGLE_REGULAR_PRICE_SELECTORS = [
    "div[class*='priceWrap'] div[class*='currentry'] > span",
    "div[class*='priceWrap'] div[class*='currency'] > span",
    "div[class*='priceWrap'] div[class*='currentry']",
    "div[class*='priceWrap'] div[class*='currency']",
    "div[class*='price'] div[class*='currentry'] > span",
    "div[class*='price'] div[class*='currency'] > span",
    "div[class*='price'] div[class*='currentry']",
    "div[class*='price'] div[class*='currency']",
    ".priceWrap .currency span",
    ".priceWrap .currency",
    ".price .currency span",
    ".price .currency",
    ".discountedPrice > .currency span",
]

AMOUNT_PATTERN = re.compile(
    r"(?i)\b(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|ml|l|ltr|litre|liter|lt|cc|pcs?|pc|each|bundles?|packs?|pack|boxes?|box|sachets?|sachet|tablets?|tablet)\b"
)
AMOUNT_WORD_ONLY_PATTERN = re.compile(r"(?i)\b(each|bundles?)\b")
SIZE_PATTERN = re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|ml|l|ltr|litre|liter|lt|cc)\b")
SIZE_RANGE_PATTERN = re.compile(
    r"(?i)(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|ml|l|ltr|litre|liter|lt|cc)\b"
)
SIZE_PLUS_PATTERN = re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|ml|l|ltr|litre|liter|lt|cc)\s*\+")
PIECE_COUNT_PATTERN = re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(pcs?|pc|pieces?)\b")
DIGIT_PATTERN = re.compile(r"\d+(?:\.\d+)?")
PRICE_LINE_PATTERN = re.compile(r"^[^\d]*\d+(?:[.,]\d+)?[^\d]*$")

UNIT_ALIASES = {
    "gm": "g",
    "gram": "g",
    "g": "g",
    "kg": "kg",
    "ml": "ml",
    "cc": "ml",
    "l": "l",
    "ltr": "l",
    "litre": "l",
    "liter": "l",
    "lt": "l",
    "pc": "pc",
    "pcs": "pc",
    "each": "each",
    "bundle": "bundle",
    "bundles": "bundle",
    "pack": "pack",
    "packs": "pack",
    "box": "box",
    "boxes": "box",
    "sachet": "sachet",
    "sachets": "sachet",
    "tablet": "tablet",
    "tablets": "tablet",
}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

# Randomized pacing helps avoid fixed, bot-like timing.
LOAD_WAIT_MIN_MS = 2300
LOAD_WAIT_MAX_MS = 4200
PRE_NAV_WAIT_MIN_MS = 350
PRE_NAV_WAIT_MAX_MS = 1000
RETRY_WAIT_MIN_MS = 1200
RETRY_WAIT_MAX_MS = 2400
URL_GAP_MIN_SEC = 2.0
URL_GAP_MAX_SEC = 5.0


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


def parse_bool(value: str) -> bool:
    value_norm = value.strip().lower()
    if value_norm in {"1", "true", "t", "yes", "y"}:
        return True
    if value_norm in {"0", "false", "f", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")


def random_wait_ms(min_ms: int, max_ms: int) -> int:
    return random.randint(min_ms, max_ms)


def random_sleep_seconds(min_sec: float, max_sec: float) -> None:
    time.sleep(random.uniform(min_sec, max_sec))


def first_non_empty_text(locator, selectors: Iterable[str]) -> str | None:
    for selector in selectors:
        el = locator.locator(selector).first
        if el.count() > 0:
            text = el.inner_text().strip()
            if text:
                return text
    return None


def parse_price(raw: str | None) -> float | None:
    if not raw:
        return None
    match = DIGIT_PATTERN.search(raw.replace(",", ""))
    if not match:
        return None
    return float(match.group(0))


def clean_amount_raw(raw_text: str | None) -> str | None:
    if not raw_text:
        return None

    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    if not lines:
        return None

    for line in lines:
        if AMOUNT_PATTERN.search(line) or AMOUNT_WORD_ONLY_PATTERN.search(line):
            return line

    return lines[0]


def first_price_from_selectors(card, selectors: Iterable[str]) -> float | None:
    for selector in selectors:
        el = card.locator(selector).first
        if el.count() == 0:
            continue
        value = parse_price(el.inner_text().strip())
        if value is not None:
            return value
    return None


def prices_from_lines(lines: list[str]) -> tuple[float | None, float | None]:
    numeric_values: list[float] = []
    for line in lines:
        clean = line.strip()
        if not clean or not PRICE_LINE_PATTERN.match(clean):
            continue
        lower = clean.lower()
        if "hr" in lower or "day" in lower:
            continue
        value = parse_price(clean)
        if value is None:
            continue
        numeric_values.append(value)
        if len(numeric_values) >= 2:
            break

    if len(numeric_values) >= 2:
        discounted_price = min(numeric_values[0], numeric_values[1])
        regular_price = max(numeric_values[0], numeric_values[1])
        if discounted_price == regular_price:
            return regular_price, None
        return regular_price, discounted_price

    if len(numeric_values) == 1:
        return numeric_values[0], None

    return None, None


def extract_prices_from_card(card, product_name: str | None = None) -> tuple[float | None, float | None]:
    """Return (regular_price, discounted_price)."""
    discounted_candidate = first_price_from_selectors(card, DISCOUNT_PRICE_SELECTORS)
    regular_discount_context = first_price_from_selectors(card, REGULAR_PRICE_IN_DISCOUNT_SELECTORS)

    if discounted_candidate is not None and regular_discount_context is not None:
        discounted_price = discounted_candidate
        regular_price = regular_discount_context
        if discounted_price > regular_price:
            discounted_price, regular_price = regular_price, discounted_price
        if discounted_price == regular_price:
            return regular_price, None
        return regular_price, discounted_price

    regular_single = first_price_from_selectors(card, SINGLE_REGULAR_PRICE_SELECTORS)
    if regular_single is not None:
        return regular_single, None

    if discounted_candidate is not None:
        return discounted_candidate, None

    try:
        card_text = card.inner_text()
    except Exception:
        return None, None

    lines = [line.strip() for line in card_text.splitlines() if line.strip()]
    if not lines:
        return None, None

    if product_name:
        try:
            name_index = lines.index(product_name.strip())
        except ValueError:
            name_index = next((i for i, line in enumerate(lines) if product_name.strip() in line), len(lines))
        candidate_lines = lines[:name_index]
    else:
        candidate_lines = lines[:4]

    return prices_from_lines(candidate_lines)


def parse_amount(amount_raw: str | None, product_name: str | None = None) -> tuple[float | None, str | None]:
    candidates: list[str] = []
    if amount_raw:
        candidates.append(amount_raw)

    # If subtext is mostly delivery timing ("1 hr", "Next Day"), fall back to product name.
    amount_lower = (amount_raw or "").lower()
    if (not amount_raw) or ("hr" in amount_lower or "day" in amount_lower):
        if product_name:
            candidates.append(product_name)

    if not candidates:
        return None, "unknown"

    for candidate in candidates:
        raw = candidate.strip().lower()
        if not raw:
            continue

        matches = list(AMOUNT_PATTERN.finditer(raw))
        if matches:
            # Prefer the last amount mention, e.g. "75 sachets 1 box" -> "1 box".
            match = matches[-1]
            quantity = float(match.group(1))
            unit = UNIT_ALIASES.get(match.group(2).lower(), "unknown")
            if unit == "each":
                return 1.0, "each"
            return quantity, unit

        word_match = AMOUNT_WORD_ONLY_PATTERN.search(raw)
        if word_match:
            word = UNIT_ALIASES.get(word_match.group(1).lower(), "unknown")
            if word == "each":
                return 1.0, "each"
            return None, word

    return None, "unknown"


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


def detect_in_stock(card) -> bool:
    """Infer stock status from card text badges/buttons."""
    try:
        text = card.inner_text().lower()
    except Exception:
        return True

    out_of_stock_markers = [
        "out of stock",
        "request stock",
    ]
    return not any(marker in text for marker in out_of_stock_markers)


def scroll_until_stable(page, card_selector: str, max_rounds: int = 8, pause_sec: float = 0.9) -> None:
    previous_count = -1
    stable_rounds = 0

    for _ in range(max_rounds):
        current_count = page.locator(card_selector).count()
        if current_count == previous_count:
            stable_rounds += 1
        else:
            stable_rounds = 0

        if stable_rounds >= 2:
            return

        page.mouse.wheel(0, random.randint(3400, 6200))
        page.wait_for_timeout(int((pause_sec + random.uniform(0.15, 0.95)) * 1000))
        previous_count = current_count


def choose_card_selector(page) -> str:
    for selector in CARD_SELECTORS:
        if page.locator(selector).count() > 0:
            return selector
    return CARD_SELECTORS[0]


def scrape_products(url: str, headless: bool) -> list[ProductRow]:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright

    timestamp = datetime.now(timezone.utc).isoformat()
    rows: list[ProductRow] = []
    seen: set[tuple[str, str | None]] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={"width": 1440, "height": 2200},
            user_agent=USER_AGENT,
            locale="en-US",
        )
        page = context.new_page()

        loaded = False
        for attempt in range(1, PAGE_RETRIES + 1):
            try:
                page.wait_for_timeout(random_wait_ms(PRE_NAV_WAIT_MIN_MS, PRE_NAV_WAIT_MAX_MS))
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                page.wait_for_timeout(random_wait_ms(LOAD_WAIT_MIN_MS, LOAD_WAIT_MAX_MS))
                selector = choose_card_selector(page)
                page.wait_for_selector(selector, timeout=30_000)
                loaded = True
                break
            except PlaywrightTimeoutError:
                if attempt == PAGE_RETRIES:
                    raise
                page.wait_for_timeout(random_wait_ms(RETRY_WAIT_MIN_MS, RETRY_WAIT_MAX_MS))

        if not loaded:
            browser.close()
            return []

        card_selector = choose_card_selector(page)
        scroll_until_stable(page, card_selector)

        cards = page.locator(card_selector)
        for idx in range(cards.count()):
            card = cards.nth(idx)
            product_name = first_non_empty_text(card, NAME_SELECTORS)
            if not product_name:
                continue

            amount_raw = clean_amount_raw(first_non_empty_text(card, SUBTEXT_SELECTORS))
            quantity, unit = parse_amount(amount_raw, product_name=product_name)
            title_context_quantity, title_context_unit, title_context_note = derive_title_context(unit, product_name)
            regular_price, discounted_price = extract_prices_from_card(card, product_name=product_name)
            in_stock = detect_in_stock(card)

            dedupe_key = (product_name.strip(), amount_raw.strip() if amount_raw else None)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            rows.append(
                ProductRow(
                    product_name=product_name.strip(),
                    regular_price=regular_price,
                    discounted_price=discounted_price,
                    currency=DEFAULT_CURRENCY,
                    amount_raw=amount_raw.strip() if amount_raw else None,
                    quantity=quantity,
                    unit=unit,
                    title_context_quantity=title_context_quantity,
                    title_context_unit=title_context_unit,
                    title_context_note=title_context_note,
                    in_stock=in_stock,
                    source_url=url,
                    scraped_at_utc=timestamp,
                )
            )

        browser.close()

    return rows


def write_outputs(rows: list[ProductRow], out_dir: str) -> tuple[Path, Path]:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"chaldal_products_{stamp}.json"
    csv_path = output_dir / f"chaldal_products_{stamp}.csv"

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


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape Chaldal category page")
    parser.add_argument("--url", help="Single category URL to scrape")
    parser.add_argument(
        "--urls-file",
        help="Path to a .txt file with one URL per line. If set, scraper will process all URLs in the file.",
    )
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output directory for CSV and JSON")
    parser.add_argument(
        "--headless",
        type=parse_bool,
        default=DEFAULT_HEADLESS,
        help="Run browser in headless mode: true/false (default: true)",
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
            print(f"Using default URL list: {DEFAULT_URLS_FILE}")
        except (FileNotFoundError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
    else:
        target_urls = [DEFAULT_URL]

    rows: list[ProductRow] = []
    failed_urls: list[str] = []
    for idx, url in enumerate(target_urls):
        if idx > 0:
            random_sleep_seconds(URL_GAP_MIN_SEC, URL_GAP_MAX_SEC)
        print(f"[{idx + 1}/{len(target_urls)}] Scraping {url}")
        try:
            rows.extend(scrape_products(url=url, headless=args.headless))
        except Exception as exc:  # noqa: BLE001
            failed_urls.append(url)
            print(f"WARNING: Failed to scrape {url}: {exc}", file=sys.stderr)

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
