# Grocery Scrapers (Bangladesh)

This repo contains Playwright scrapers for multiple grocery websites.

## Scripts

- `scrape_chaldal.py`
- `scrape_shwapno.py`

Both scripts output the same core schema so you can combine datasets later:

- `product_name`
- `regular_price`
- `discounted_price`
- `currency`
- `amount_raw`
- `quantity`
- `unit`
- `in_stock`
- `source_url`
- `scraped_at_utc`

## Install

```bash
python3 -m pip install -r requirements.txt
python3 -m playwright install --with-deps chromium
```

## Chaldal

Single URL:

```bash
python3 scrape_chaldal.py --url "https://chaldal.com/fresh-vegetable" --headless true --out-dir outputs
```

Multiple URLs (recommended file: `urls_chaldal.txt`):

```bash
python3 scrape_chaldal.py --urls-file urls_chaldal.txt --headless true --out-dir outputs
```

## Shwapno

Single URL:

```bash
python3 scrape_shwapno.py --url "https://www.shwapno.com/eggs" --headless true --out-dir outputs
```

Multiple URLs (recommended file: `urls_shwapno.txt`):

```bash
python3 scrape_shwapno.py --urls-file urls_shwapno.txt --headless true --out-dir outputs
```

If `urls_shwapno.txt` exists in the repo root, running `scrape_shwapno.py` without `--urls-file` will use it automatically.

## URL File Format

Use `.txt`, one URL per line. Keep separate files per website.

```txt
# urls_shwapno.txt
https://www.shwapno.com/eggs
https://www.shwapno.com/fruits
```

## Outputs

Each run writes one CSV + one JSON:

- Chaldal: `outputs/chaldal_products_YYYYMMDD_HHMMSS.{csv,json}`
- Shwapno: `outputs/shwapno_products_YYYYMMDD_HHMMSS.{csv,json}`

If zero products are scraped, the script exits with code `1`.

## GitHub Actions

- `.github/workflows/chaldal_scrape.yml`
- `.github/workflows/shwapno_scrape.yml`
