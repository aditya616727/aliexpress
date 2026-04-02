# AliExpress Product Scraper

Scrapes product data from AliExpress search results, uploads images to Cloudflare, stores in MongoDB, and exports to CSV/JSON.

## Quick Start

### Local

```bash
pip install -e ".[dev]"
playwright install chromium
python run.py --query "wireless earbuds" --pages 2
```

### Docker

```bash
docker compose up --build
```

## Usage

```bash
# Single query mode
python run.py --query "wireless earbuds" --pages 2

# Category mode — reads config/categories.yaml
python run.py

# Custom output directory
python run.py --query "phone case" --pages 1 --output ./my_data

# Skip optional stages
python run.py --query "laptop stand" --pages 3 --no-images --no-db

# Run as module
python -m ali_scraper --query "usb hub" --pages 1

# Verbose logging
python run.py --query "test" --verbose
```

## Configuration

Copy `.env.example` to `.env` and fill in:

| Variable | Description |
|---|---|
| `MONGODB_URI` | MongoDB connection string |
| `CLOUDFLARE_ACCOUNT_ID` | Cloudflare Images account ID |
| `CLOUDFLARE_API_TOKEN` | Cloudflare Images API token |
| `HEADLESS` | Run browser headless (default: false) |
| `PROXY_SERVER` | Optional proxy (e.g. `http://host:port`) |
| `MAX_PAGES` | Max pages to scrape (default: 5) |
| `DELAY_MIN` / `DELAY_MAX` | Random delay range between pages |

Categories are configured in `config/categories.yaml`.

## Pipeline

```
config/categories.yaml → Scrape AliExpress → Download images → Upload to Cloudflare → Store in MongoDB → Export CSV/JSON
```

## Output

- `data/products.csv` — CSV file with all scraped products
- `data/products.json` — JSON file with all scraped products
- `data/images/` — Downloaded product images (deleted after Cloudflare upload)

## Testing

```bash
pytest tests/ -v
```

## Project Structure

```
aliexpress/
├── run.py                       # Entry point (no install needed)
├── pyproject.toml               # Package metadata & dependencies
├── Dockerfile
├── docker-compose.yml
├── requirements.txt             # Pinned deps (legacy)
├── config/
│   └── categories.yaml          # Search categories to scrape
├── src/
│   └── ali_scraper/
│       ├── __init__.py
│       ├── __main__.py          # python -m ali_scraper
│       ├── cli.py               # CLI orchestrator
│       ├── config/
│       │   ├── __init__.py
│       │   ├── settings.py      # Environment-based settings
│       │   └── categories.py    # Category config loader
│       ├── database/
│       │   ├── __init__.py
│       │   └── mongodb.py       # MongoDB storage (Clothing schema)
│       ├── scrapers/
│       │   ├── __init__.py
│       │   ├── base.py          # Base Playwright scraper
│       │   └── aliexpress.py    # AliExpress-specific scraper
│       ├── export/
│       │   ├── __init__.py
│       │   ├── data.py          # CSV & JSON exporter
│       │   └── images.py        # Image downloader
│       ├── cloudflare/
│       │   ├── __init__.py
│       │   └── uploader.py      # Cloudflare Images uploader
│       └── utils/
│           ├── __init__.py
│           └── helpers.py       # Shared utilities
└── tests/
    ├── __init__.py
    ├── test_scraper.py
    ├── test_exporter.py
    └── test_image_downloader.py
```
