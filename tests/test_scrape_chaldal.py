from scrape_chaldal import ProductRow, clean_amount_raw, dedupe_rows, detect_in_stock, parse_amount, parse_price, prices_from_lines, read_urls_file


def test_parse_price_symbol_and_text():
    assert parse_price("৳49") == 49.0
    assert parse_price("BDT 1,299") == 1299.0


def test_parse_price_no_number():
    assert parse_price("N/A") is None


def test_parse_amount_each():
    qty, unit = parse_amount("each")
    assert qty == 1.0
    assert unit == "each"


def test_parse_amount_grams():
    qty, unit = parse_amount("500 gm")
    assert qty == 500.0
    assert unit == "g"


def test_parse_amount_kg():
    qty, unit = parse_amount("1 kg")
    assert qty == 1.0
    assert unit == "kg"


def test_parse_amount_ml():
    qty, unit = parse_amount("100 ml")
    assert qty == 100.0
    assert unit == "ml"


def test_parse_amount_ltr():
    qty, unit = parse_amount("1 ltr")
    assert qty == 1.0
    assert unit == "l"


def test_parse_amount_decimal_ltr():
    qty, unit = parse_amount("1.75 ltr")
    assert qty == 1.75
    assert unit == "l"


def test_parse_amount_pack():
    qty, unit = parse_amount("8 pack")
    assert qty == 8.0
    assert unit == "pack"


def test_parse_amount_bundles_plural():
    qty, unit = parse_amount("3 bundles")
    assert qty == 3.0
    assert unit == "bundle"


def test_parse_amount_box():
    qty, unit = parse_amount("1 box")
    assert qty == 1.0
    assert unit == "box"


def test_parse_amount_fallback_to_product_name_when_subtext_is_delivery_time():
    qty, unit = parse_amount("1 hr", product_name="Teo Tak Seng Silver Pomfret Fish Sauce 750 cc")
    assert qty == 750.0
    assert unit == "ml"


def test_parse_amount_prefers_last_match_for_packaging():
    qty, unit = parse_amount("Zero Cal Box 75 Sachets 1 box")
    assert qty == 1.0
    assert unit == "box"


def test_parse_amount_unknown():
    qty, unit = parse_amount("small pack")
    assert qty is None
    assert unit == "unknown"


def test_clean_amount_raw_strips_delivery_line():
    assert clean_amount_raw("500 gm\n1 hr") == "500 gm"
    assert clean_amount_raw("each\nNext Day") == "each"


def test_clean_amount_raw_fallback_line():
    assert clean_amount_raw("No unit shown\n1 hr") == "No unit shown"


def test_prices_from_lines_discount_and_regular():
    regular, discounted = prices_from_lines(["৳49", "৳59"])
    assert regular == 59.0
    assert discounted == 49.0


def test_prices_from_lines_single_price():
    regular, discounted = prices_from_lines(["৳29"])
    assert regular == 29.0
    assert discounted is None


def test_prices_from_lines_ignores_non_price_lines():
    regular, discounted = prices_from_lines(["+ Add", "1 hr", "Flat Bean"])
    assert regular is None
    assert discounted is None


def test_read_urls_file(tmp_path):
    p = tmp_path / "urls.txt"
    p.write_text(
        "# comment\nhttps://chaldal.com/fresh-vegetable\n\nhttps://chaldal.com/fresh-fruits\n",
        encoding="utf-8",
    )
    assert read_urls_file(str(p)) == [
        "https://chaldal.com/fresh-vegetable",
        "https://chaldal.com/fresh-fruits",
    ]


def test_dedupe_rows():
    row = ProductRow(
        product_name="Item A",
        regular_price=10.0,
        discounted_price=None,
        currency="BDT",
        amount_raw="1 kg",
        quantity=1.0,
        unit="kg",
        in_stock=True,
        source_url="https://chaldal.com/fresh-vegetable",
        scraped_at_utc="2026-02-18T00:00:00+00:00",
    )
    deduped = dedupe_rows([row, row])
    assert len(deduped) == 1


class DummyCard:
    def __init__(self, text: str):
        self._text = text

    def inner_text(self):
        return self._text


def test_detect_in_stock_true():
    assert detect_in_stock(DummyCard("Apple\n৳49\n4 pcs\nNext Day")) is True


def test_detect_in_stock_false():
    assert detect_in_stock(DummyCard("Watermelon\nOut of Stock\nRequest Stock")) is False
