# AliExpress Product Scraper

Scrapes product data from AliExpress search results, exports to CSV/JSON, and downloads product images.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Scrape products by search query
python main.py --query "wireless earbuds" --pages 2

# Scrape with custom output directory
python main.py --query "phone case" --pages 1 --output ./my_data

# Scrape multiple categories
python main.py --query "laptop stand" --pages 3 --output ./data
```

## Output

- `data/products.csv` — CSV file with all scraped products
- `data/products.json` — JSON file with all scraped products
- `data/images/` — Downloaded product images

## Testing

```bash
pytest tests/ -v
```

## Project Structure

```
aliexpress/
├── main.py              # Entry point
├── scraper.py           # Core scraping logic
├── exporter.py          # CSV and JSON export
├── image_downloader.py  # Image downloading
├── config.py            # Configuration
├── requirements.txt
├── tests/
│   ├── __init__.py
│   ├── test_scraper.py
│   ├── test_exporter.py
│   └── test_image_downloader.py
└── README.md
```
