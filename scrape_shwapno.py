#!/usr/bin/env python3
"""Scrape Shwapno category pages and export product data to CSV + JSON."""

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

DEFAULT_URL = "https://www.shwapno.com/eggs"
DEFAULT_URLS_FILE = "urls_shwapno.txt"
DEFAULT_OUT_DIR = "outputs"
DEFAULT_CURRENCY = "BDT"
DEFAULT_HEADLESS = True
PAGE_RETRIES = 2

CARD_SELECTORS = [
    "#product-grid .product-box",
    "div[id='product-grid'] .product-box",
    "div.product-box",
    "div[class*='product-box'][class*='rounded']",
]
NAME_SELECTORS = [
    ".product-box-title a",
    "div[class*='product-box-title'] a",
    "div[class*='product-box-title']",
]
PRICE_CONTAINER_SELECTORS = [
    ".product-price",
    "div[class*='product-price']",
]
ACTIVE_PRICE_SELECTORS = [
    ".product-price .active-price",
    "div[class*='product-price'] span[class*='active-price']",
]
OLD_PRICE_SELECTORS = [
    ".product-price .old-price",
    "div[class*='product-price'] span[class*='old-price']",
]

AMOUNT_PATTERN = re.compile(
    r"(?i)\b(\d+(?:\.\d+)?)\s*(?:\(\s*[~]?\s*±\s*\d+(?:\.\d+)?\s*\))?\s*(kg|gm|g|gram|ml|l|ltr|litre|liter|pcs?|pc|each|pack|dozen|bundles?|bunches?|bundle|bunch|pieces?)\b"
)
AMOUNT_WORD_ONLY_PATTERN = re.compile(r"(?i)\b(each|pack|dozen|bundles?|bunches?|bundle|bunch|pieces?)\b")
DIGIT_PATTERN = re.compile(r"\d+(?:\.\d+)?")
SIZE_PATTERN = re.compile(
    r"(?i)(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|ml|l|ltr|litre|liter)\b"
)
PLUS_MINUS_PATTERN = re.compile(
    r"(?i)\(\s*±\s*(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|ml|l|ltr|litre|liter)\s*\)"
)
RANGE_PATTERN = re.compile(
    r"(?i)(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|ml|l|ltr|litre|liter)\b"
)
PIECE_COUNT_PATTERN = re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(pcs?|pc|pieces?)\b")
MIN_ORDER_PATTERN = re.compile(
    r"(?i)min\.\s*(\d+(?:\.\d+)?)\s*(kg|gm|g|gram|ml|l|ltr|litre|liter|pcs?|pc|pack|dozen|unit)"
)

UNIT_ALIASES = {
    "gm": "g",
    "gram": "g",
    "g": "g",
    "kg": "kg",
    "ml": "ml",
    "l": "l",
    "ltr": "l",
    "litre": "l",
    "liter": "l",
    "pc": "pc",
    "pcs": "pc",
    "piece": "pc",
    "pieces": "pc",
    "each": "each",
    "pack": "pack",
    "dozen": "dozen",
    "unit": "unit",
    "bundle": "bundle",
    "bundles": "bundle",
    "bunch": "bundle",
    "bunches": "bundle",
}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

# Randomized pacing helps avoid very bot-like timing patterns.
LOAD_WAIT_MIN_MS = 2400
LOAD_WAIT_MAX_MS = 4200
PRE_NAV_WAIT_MIN_MS = 400
PRE_NAV_WAIT_MAX_MS = 1100
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


def parse_price(raw: str | None) -> float | None:
    if not raw:
        return None
    match = DIGIT_PATTERN.search(raw.replace(",", ""))
    if not match:
        return None
    return float(match.group(0))


def is_price_like_text(text: str) -> bool:
    lower = text.lower().strip()
    if not lower:
        return False
    if "min" in lower or "per " in lower:
        return False
    if "৳" in lower or "tk" in lower:
        return True
    return bool(re.fullmatch(r"\\d+(?:\\.\\d+)?", lower))


def first_non_empty_text(locator, selectors: Iterable[str]) -> str | None:
    for selector in selectors:
        el = locator.locator(selector).first
        if el.count() == 0:
            continue
        text = el.inner_text().strip()
        if text:
            return text
    return None


def first_price_by_selectors(card, selectors: Iterable[str]) -> float | None:
    for selector in selectors:
        el = card.locator(selector).first
        if el.count() == 0:
            continue
        text = el.inner_text().strip()
        if not is_price_like_text(text):
            continue
        parsed = parse_price(text)
        if parsed is not None:
            return parsed
    return None


def extract_prices_from_card(card) -> tuple[float | None, float | None]:
    # Shwapno uses explicit price classes in product cards:
    # - .active-price: current/offer price
    # - .old-price: regular/strikethrough price (only if discounted)
    active_price = first_price_by_selectors(card, ACTIVE_PRICE_SELECTORS)
    old_price = first_price_by_selectors(card, OLD_PRICE_SELECTORS)

    if active_price is not None and old_price is not None:
        regular_price = max(active_price, old_price)
        discounted_price = min(active_price, old_price)
        if regular_price == discounted_price:
            return regular_price, None
        return regular_price, discounted_price

    if active_price is not None:
        return active_price, None

    if old_price is not None:
        return old_price, None

    # Fallback only if class selectors fail unexpectedly.
    values: list[float] = []
    for selector in PRICE_CONTAINER_SELECTORS:
        container = card.locator(selector).first
        if container.count() == 0:
            continue
        spans = container.locator("span")
        for idx in range(spans.count()):
            text = spans.nth(idx).inner_text().strip()
            if not is_price_like_text(text):
                continue
            parsed = parse_price(text)
            if parsed is not None:
                values.append(parsed)
        if values:
            break

    if not values:
        return None, None

    unique_values = list(dict.fromkeys(values))
    if len(unique_values) >= 2:
        regular = max(unique_values)
        discounted = min(unique_values)
        if regular == discounted:
            return regular, None
        return regular, discounted
    return unique_values[0], None


def extract_amount_raw(card) -> str | None:
    price_container = card.locator(".product-price").first
    if price_container.count() > 0:
        spans = price_container.locator("span")
        for idx in range(spans.count()):
            try:
                text = spans.nth(idx).inner_text(timeout=2500).strip()
            except Exception:
                continue
            if not text:
                continue
            lower = text.lower()
            if "per " in lower:
                return text
            if AMOUNT_PATTERN.search(text) or AMOUNT_WORD_ONLY_PATTERN.search(text):
                return text
        try:
            price_text = price_container.inner_text(timeout=2500)
            for line in [line.strip() for line in price_text.splitlines() if line.strip()]:
                if "per " in line.lower():
                    return line
        except Exception:
            pass

    try:
        card_text = card.inner_text(timeout=2500)
    except Exception:
        return None
    for line in [line.strip() for line in card_text.splitlines() if line.strip()]:
        if line.lower().startswith("delivery"):
            continue
        if AMOUNT_PATTERN.search(line) or AMOUNT_WORD_ONLY_PATTERN.search(line) or "per " in line.lower():
            return line

    return None


def infer_amount_from_card_text(card) -> str | None:
    """Fallback extractor for cards where amount text is not exposed via spans."""
    try:
        raw = (card.text_content() or "").lower().replace("\xa0", " ")
        text = re.sub(r"\s+", " ", raw).strip()
    except Exception:
        return None

    if re.search(r"per\s*piece", text):
        return "Per Piece"
    if re.search(r"per\s*kg", text):
        return "Per Kg"
    if re.search(r"per\s*unit", text):
        return "Per Unit"
    if re.search(r"per\s*pack", text):
        return "Per Pack"
    if re.search(r"per\s*dozen", text):
        return "Per Dozen"
    return None


def parse_amount(amount_raw: str | None, product_name: str | None = None) -> tuple[float | None, str | None]:
    candidates: list[str] = []
    if amount_raw:
        candidates.append(amount_raw)
    if product_name:
        candidates.append(product_name)

    # First pass: prefer numeric quantities (e.g., "12Pcs Pack", "1 kg").
    for candidate in candidates:
        raw = candidate.strip().lower()
        if not raw:
            continue

        # Normalize patterns like "330(±)10ml" -> "330 ml" and
        # "52(~±)1gm" -> "52 gm" so tolerance values are ignored.
        raw = re.sub(
            r"(?i)(\d+(?:\.\d+)?)\s*\(\s*[~]?\s*±\s*\)\s*\d+(?:\.\d+)?\s*(kg|gm|g|gram|ml|l|ltr|litre|liter|pcs?|pc)\b",
            r"\1 \2",
            raw,
        )

        # Remove tolerance chunks so "330(±5)ml" parses as 330 ml.
        raw = re.sub(r"\(\s*[~]?\s*±\s*\d+(?:\.\d+)?\s*(?:kg|gm|g|gram|ml|l|ltr|litre|liter|pcs?|pc)?\s*\)", "", raw)

        match = AMOUNT_PATTERN.search(raw)
        if match:
            quantity = float(match.group(1))
            unit = UNIT_ALIASES.get(match.group(2).lower(), "unknown")
            if unit == "each":
                return 1.0, "each"
            return quantity, unit

    # Second pass: fallback to word-only units (e.g., "Per Pack").
    for candidate in candidates:
        raw = candidate.strip().lower()
        if not raw:
            continue

        word_match = AMOUNT_WORD_ONLY_PATTERN.search(raw)
        if word_match:
            word = UNIT_ALIASES.get(word_match.group(1).lower(), "unknown")
            if word == "each":
                return 1.0, "each"
            if word in {"pc", "bundle"}:
                return 1.0, word
            return None, word

    return None, "unknown"


def normalize_unit(raw_unit: str | None) -> str | None:
    if raw_unit is None:
        return None
    return UNIT_ALIASES.get(raw_unit.lower(), raw_unit.lower())


def parse_price_basis(amount_raw: str | None) -> str | None:
    if not amount_raw:
        return None
    lower = amount_raw.strip().lower()
    if "per kg" in lower or re.search(r"per\s*\d+(?:\.\d+)?\s*kg", lower):
        return "per_kg"
    if "per piece" in lower or "per pc" in lower or "per pcs" in lower:
        return "per_piece"
    if "per pack" in lower:
        return "per_pack"
    if "per unit" in lower:
        return "per_unit"
    if "per dozen" in lower:
        return "per_dozen"
    return None


def parse_min_order(text: str | None) -> tuple[float | None, str | None]:
    if not text:
        return None, None
    match = MIN_ORDER_PATTERN.search(text)
    if not match:
        return None, None
    return float(match.group(1)), normalize_unit(match.group(2))


def parse_size_fields(product_name_raw: str | None, amount_raw: str | None) -> tuple[float | None, str | None, str | None]:
    text = " ".join(x for x in [product_name_raw, amount_raw] if x)
    if not text:
        return None, None, None

    plus_minus_match = PLUS_MINUS_PATTERN.search(text)
    if plus_minus_match:
        note = f"±{plus_minus_match.group(1)} {normalize_unit(plus_minus_match.group(2))}"
    else:
        note = None

    range_match = RANGE_PATTERN.search(text)
    if range_match:
        low = float(range_match.group(1))
        high = float(range_match.group(2))
        unit = normalize_unit(range_match.group(3))
        return (low + high) / 2.0, unit, f"range:{low}-{high} {unit}"

    size_match = SIZE_PATTERN.search(text)
    if size_match:
        return float(size_match.group(1)), normalize_unit(size_match.group(2)), note

    return None, None, note


def parse_piece_count_from_title(product_name_raw: str | None) -> tuple[float | None, str | None, str | None]:
    if not product_name_raw:
        return None, None, None
    text = product_name_raw.strip()
    if not text:
        return None, None, None
    match = PIECE_COUNT_PATTERN.search(text)
    if not match:
        return None, None, None
    return float(match.group(1)), "pc", None


def derive_title_context(
    sell_unit: str | None, product_name_raw: str | None, amount_raw: str | None
) -> tuple[float | None, str | None, str | None]:
    unit = (sell_unit or "").strip().lower()
    if unit in {"pc", "each"}:
        return parse_size_fields(product_name_raw, amount_raw)
    if unit in {"g", "kg", "ml", "l"}:
        return parse_piece_count_from_title(product_name_raw)
    return None, None, None


def clean_product_name(name: str) -> str:
    cleaned = re.sub(r"\s+", " ", name).strip()
    cleaned = re.sub(r"(?i)\(\s*min\.[^)]+\)", "", cleaned)
    cleaned = re.sub(r"(?i)\(\s*±[^)]+\)", "", cleaned)
    cleaned = re.sub(r"(?i)\b\d+(?:\.\d+)?\s*\(±\)\s*\d+(?:\.\d+)?\s*(kg|gm|g|gram|ml|l|ltr|litre|liter|pcs?|pc)\b", "", cleaned)
    cleaned = re.sub(r"(?i)\b\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?\s*(kg|gm|g|gram|ml|l|ltr|litre|liter|pcs?|pc)\b", "", cleaned)
    cleaned = re.sub(r"(?i)\b\d+(?:\.\d+)?\s*(kg|gm|g|gram|ml|l|ltr|litre|liter|pcs?|pc|pack|dozen|unit)\b", "", cleaned)
    cleaned = cleaned.replace("()", "")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip(" -_,")


def detect_in_stock(card) -> bool:
    try:
        text = card.inner_text().lower()
    except Exception:
        return True

    markers = [
        "out of stock",
        "request stock",
    ]
    return not any(marker in text for marker in markers)


def scroll_until_stable(page, card_selector: str, max_rounds: int = 18, pause_sec: float = 1.2) -> None:
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

        page.mouse.wheel(0, random.randint(3500, 6200))
        page.wait_for_timeout(int((pause_sec + random.uniform(0.15, 0.95)) * 1000))
        previous_count = current_count


def choose_card_selector(page) -> str:
    for selector in CARD_SELECTORS:
        if page.locator(selector).count() > 0:
            return selector
    return CARD_SELECTORS[0]


def is_featured_card(card) -> bool:
    # Featured carousel cards often live inside wrappers containing "featured" in
    # class/id. Skip those and keep only normal category listing cards.
    featured_ancestor = card.locator(
        "xpath=ancestor::*[contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'featured') "
        "or contains(translate(@id,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'featured')]"
    )
    return featured_ancestor.count() > 0


def scrape_products(url: str, headless: bool) -> list[ProductRow]:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright

    rows: list[ProductRow] = []
    seen: set[tuple[str, str | None]] = set()
    timestamp = datetime.now(timezone.utc).isoformat()

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
                # Prefer main catalog grid cards, not featured carousel cards.
                try:
                    page.wait_for_selector("#product-grid .product-box", timeout=18_000)
                except Exception:
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
            try:
                if is_featured_card(card):
                    continue

                product_name_raw = first_non_empty_text(card, NAME_SELECTORS)
                if not product_name_raw:
                    continue
                product_name = product_name_raw.strip()

                regular_price, discounted_price = extract_prices_from_card(card)
                amount_raw = extract_amount_raw(card)
                if not amount_raw:
                    amount_raw = infer_amount_from_card_text(card)
                quantity, unit = parse_amount(amount_raw, product_name_raw)
                if unit == "unknown":
                    try:
                        quantity_fallback, unit_fallback = parse_amount(card.inner_text(timeout=2000), product_name_raw)
                        if unit_fallback != "unknown":
                            quantity, unit = quantity_fallback, unit_fallback
                    except Exception:
                        pass
                title_context_quantity, title_context_unit, title_context_note = derive_title_context(
                    unit, product_name_raw, amount_raw
                )
                _price_basis = parse_price_basis(amount_raw)
                if _price_basis is None:
                    try:
                        _price_basis = parse_price_basis(card.inner_text(timeout=2000))
                    except Exception:
                        _price_basis = None
                _min_order_qty, _min_order_unit = parse_min_order(amount_raw or product_name_raw)
                in_stock = detect_in_stock(card)

                dedupe_key = (product_name_raw.strip(), amount_raw.strip() if amount_raw else None)
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
            except Exception:
                # Skip problematic cards rather than stalling the full category run.
                continue

        browser.close()

    return rows


def write_outputs(rows: list[ProductRow], out_dir: str) -> tuple[Path, Path]:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"shwapno_products_{stamp}.json"
    csv_path = output_dir / f"shwapno_products_{stamp}.csv"

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
    # Keep one entry per exact (url, name, amount) and prefer richer rows
    # when the same product appears both with and without amount/unit.
    by_product: dict[tuple[str, str], list[ProductRow]] = {}
    for row in rows:
        by_product.setdefault((row.source_url, row.product_name), []).append(row)

    filtered: list[ProductRow] = []
    for _, group in by_product.items():
        has_specific_amount = any((g.amount_raw or "").strip() and g.unit != "unknown" for g in group)
        if has_specific_amount:
            for item in group:
                amount_text = (item.amount_raw or "").strip()
                if not amount_text and item.unit == "unknown":
                    continue
                filtered.append(item)
        else:
            filtered.extend(group)

    seen: set[tuple[str, str, str | None]] = set()
    deduped: list[ProductRow] = []
    for row in filtered:
        key = (row.source_url, row.product_name, row.amount_raw)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape Shwapno category pages")
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
