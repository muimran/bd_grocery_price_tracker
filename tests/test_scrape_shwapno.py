from scrape_shwapno import clean_product_name, parse_amount, parse_min_order, parse_price_basis, parse_size_fields


def test_clean_product_name_removes_size_tokens():
    assert clean_product_name('Nestle Nan-3 Optipro Milk Powder 350gm') == 'Nestle Nan-3 Optipro Milk Powder'
    assert clean_product_name('Quaker Oats 900(±)100gm (Jar)') == 'Quaker Oats (Jar)'


def test_parse_size_fields_simple():
    value, unit, note = parse_size_fields('Polar Vanilla Ice Cream 1Ltr.', 'Per Piece')
    assert value == 1.0
    assert unit == 'l'
    assert note is None


def test_parse_size_fields_range_and_min_note():
    value, unit, note = parse_size_fields('Beijing Hansh (Duck) Dressed (2-3 kg)', 'Per 1kg (Min. 3kg)')
    assert value == 2.5
    assert unit == 'kg'
    assert note == 'range:2.0-3.0 kg'


def test_parse_price_basis():
    assert parse_price_basis('Per Piece') == 'per_piece'
    assert parse_price_basis('Per 1kg (Min. 0.5kg)') == 'per_kg'
    assert parse_price_basis('Per Unit') == 'per_unit'


def test_parse_min_order():
    qty, unit = parse_min_order('Per 1kg (Min. 0.5kg)')
    assert qty == 0.5
    assert unit == 'kg'


def test_parse_amount_ltr_from_name():
    qty, unit = parse_amount("", "Borges Sunflower Oil 1Ltr.")
    assert qty == 1.0
    assert unit == "l"


def test_parse_amount_decimal_ltr_from_name():
    qty, unit = parse_amount("", "Ecorganic Sunflower Oil 5Ltr. (Tin)")
    assert qty == 5.0
    assert unit == "l"


def test_parse_amount_per_bunch():
    qty, unit = parse_amount("Lau Shak (Bottle Gourd Spinach) Per Bunch", "Lau Shak (Bottle Gourd Spinach) Per Bunch")
    assert qty == 1.0
    assert unit == "bundle"
