# ad-extractor

Scrapes car ads from Blocket.se (Sweden), translates Swedish → English, stores in MongoDB.

## Flow

```
config/dealers.yaml  →  Load dealers
        ↓
For each dealer URL:
  BlocketScraper.get_listing_urls()  →  Collect listing URLs (pagination)
        ↓
  For each listing URL:
    BlocketScraper.scrape_listing()  →  Extract title, price, specs, images, etc.
        ↓
    translate_listings()  →  Swedish → English (optional)
        ↓
    mongo_client.insert_listing()  →  Save to MongoDB
```

## Project Structure

| Path | Purpose |
|------|---------|
| `src/ad_extractor/cli.py` | Entry point – orchestrates scraping |
| `src/ad_extractor/scrapers/blocket.py` | Blocket.se scraper |
| `src/ad_extractor/database/mongodb.py` | MongoDB client |
| `src/ad_extractor/translation/` | Swedish→English translation |
| `src/ad_extractor/config/` | Settings, dealer config |
| `config/dealers.yaml` | Dealer list (URLs, emails, etc.) |

## Quick Start

### Option A: Docker (Python 3.13, Chrome, proxy support)

```bash
# 1. Copy .env.example to .env, set MONGODB_URI and WEBSHARE_API_KEY (required)
cp .env.example .env

# 2. Build and run
docker compose build
docker compose run scraper

# On Apple Silicon (M1/M2): docker compose build --platform linux/amd64
```

### Option B: Local

```bash
# 1. Install (Python 3.12 or 3.13 for proxy support)
pip install -e .

# 2. Configure – copy .env.example to .env, set MONGODB_URI (full URI with user, pass, host, db)

# 3. Run (from project root)
python run.py
# Or:
python -m ad_extractor

# Without translation
python run.py --no-translate
```

## Output

Each doc in MongoDB `listings` has:

- Root: `business_id`, `source_url`, `dealer_name`, `dealer_email`, `dealer_phone`, `dealer_location`, `scraped_at`
- `postAdData`: title, description, price, brand, model, images, specs, etc.

```yaml
dealers:
  - business_id: RIDDERMARK_OSTERSUND
    name: Riddermark Bil - Östersund
    location: Östersund
    url: https://www.blocket.se/mobility/search/car?orgId=5359029
    email: ostersund@riddermarkbil.se
    phone: "010-330 73 99"
```

### Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NODE_ENV` | `development` or `production` – selects MongoDB URI | - |
| `MONGODB_URI` | Fallback MongoDB URI | - |
| `MONGODB_URI_DEV` | MongoDB URI when `NODE_ENV=development` | - |
| `MONGODB_URI_PROD` | MongoDB URI when `NODE_ENV=production` | - |
| `DEEPL_API_KEY` | DeepL API key (translation; recommended) | - |
| `USE_PROXY` | Enable Webshare proxy | `false` |
| `WEBSHARE_API_KEY` | Webshare API key (fetches proxies) | - |
| `LOG_LEVEL` | Logging level | `INFO` |

### Translation (DeepL API)

When `DEEPL_API_KEY` is set, the scraper uses the paid DeepL API (no rate limits). Without it, it falls back to free Google Translate (5 req/s, 200k/day limit).

1. Sign up at [deepl.com/pro-api](https://www.deepl.com/pro-api)
2. Create an API key
3. Set in `.env`: `DEEPL_API_KEY=your_key_here`

### Proxy setup (optional)

When `USE_PROXY=true`, the scraper fetches proxies from [Webshare](https://www.webshare.io/).

1. Create an account at [webshare.io](https://www.webshare.io/)
2. Subscribe to a proxy plan (free tier has limited proxies)
3. Copy your API key from [Proxy Dashboard → API](https://proxy.webshare.io/)
4. Set in `.env`:
   ```
   USE_PROXY=true
   WEBSHARE_API_KEY=your_token_here
   ```

**Troubleshooting:**
- Ensure the API key is the **Token** from Webshare (not password)
- **Residential plans** require `mode=backbone` – the scraper auto-detects this
- **Datacenter plans** use `mode=direct`
- Verify: `curl -H "Authorization: Token YOUR_KEY" "https://proxy.webshare.io/api/v2/proxy/list/?mode=backbone&page=1&page_size=1"`

## Data Extracted

- Title, description, price
- Images (gallery URLs)
- Specifications (brand, model, year, fuel type, etc.)
- Equipment/extras
- Dealer metadata (name, email, phone, location)

## Development

```bash
pip install -e ".[dev]"
black src/
ruff check src/
```

## License

MIT
