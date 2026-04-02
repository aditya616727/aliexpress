"""Application settings loaded from environment variables."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Centralized application settings."""

    base_dir = Path(__file__).resolve().parent.parent.parent.parent
    log_dir = base_dir / "logs"
    config_dir = base_dir / "config"

    # AliExpress
    search_url = "https://www.aliexpress.com/w/wholesale-{query}.html"

    # MongoDB
    @property
    def mongodb_uri(self):
        env = os.getenv("NODE_ENV", "production")
        if env == "development":
            return os.getenv("MONGODB_URI_DEV", os.getenv("MONGODB_URI", ""))
        return os.getenv("MONGODB_URI_PROD", os.getenv("MONGODB_URI", ""))

    # Cloudflare Images
    cloudflare_account_id = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")
    cloudflare_api_token = os.getenv("CLOUDFLARE_API_TOKEN", "")

    # Browser
    headless = os.getenv("HEADLESS", "false").lower() == "true"
    chrome_path = os.getenv("CHROME_PATH", "") or None
    chrome_sandbox = os.getenv("CHROME_SANDBOX", "true").lower() == "true"

    # Proxy
    proxy_server = os.getenv("PROXY_SERVER", "") or None

    # Scraping
    request_timeout = 30
    delay_min = int(os.getenv("DELAY_MIN", 2))
    delay_max = int(os.getenv("DELAY_MAX", 5))
    max_retries = 3
    max_pages = int(os.getenv("MAX_PAGES", 5))
    max_concurrent_tabs = int(os.getenv("MAX_CONCURRENT_TABS", 3))
    max_concurrent_uploads = int(os.getenv("MAX_CONCURRENT_UPLOADS", 5))

    # Output
    output_dir = os.getenv("OUTPUT_DIR", "data")

    @property
    def default_output_dir(self):
        return self.base_dir / self.output_dir

    @property
    def default_image_dir(self):
        return self.default_output_dir / "images"

    # Export
    csv_filename = "products.csv"
    json_filename = "products.json"

    # Logging
    log_level = os.getenv("LOG_LEVEL", "INFO")

    # Default headers
    default_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    # Product fields for CSV export
    product_fields = [
        "title",
        "price",
        "original_price",
        "discount",
        "rating",
        "reviews_count",
        "orders_count",
        "store_name",
        "product_url",
        "image_url",
        "image_path",
    ]

    def ensure_directories(self):
        """Create required directories."""
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.default_output_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
