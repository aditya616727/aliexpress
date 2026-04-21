"""Centralized configuration loaded from environment variables."""

import os
from pathlib import Path
from typing import List

from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Application settings loaded from environment variables."""

    def __init__(self) -> None:
        self._base_dir = Path(__file__).resolve().parent.parent.parent.parent

    # Paths
    @property
    def base_dir(self) -> Path:
        return self._base_dir

    log_dir: Path = Path("logs")
    config_dir: Path = Path("config")

    # MongoDB (NODE_ENV=development → MONGODB_URI_DEV, production → MONGODB_URI_PROD)
    @property
    def mongodb_uri(self) -> str:
        env = os.getenv("NODE_ENV", "").lower()
        if env == "development":
            return os.getenv("MONGODB_URI_DEV", os.getenv("MONGODB_URI", ""))
        if env == "production":
            return os.getenv("MONGODB_URI_PROD", os.getenv("MONGODB_URI", ""))
        return os.getenv("MONGODB_URI", "")

    # Proxy (Webshare API – fetches proxies with credentials)
    @property
    def use_proxy(self) -> bool:
        return os.getenv("USE_PROXY", "false").lower() == "true"

    @property
    def webshare_api_key(self) -> str:
        return os.getenv("WEBSHARE_API_KEY", "")

    # Translation (DeepL API – paid, no rate limits like free Google)
    @property
    def deepl_api_key(self) -> str:
        return os.getenv("DEEPL_API_KEY", "")

    # Parallelism (tune for your machine)
    @property
    def num_scraper_workers(self) -> int:
        return int(os.getenv("NUM_SCRAPER_WORKERS", "4"))

    @property
    def num_consumer_workers(self) -> int:
        return int(os.getenv("NUM_CONSUMER_WORKERS", "3"))

    @property
    def consumer_batch_size(self) -> int:
        return int(os.getenv("CONSUMER_BATCH_SIZE", "4"))

    # Logging
    @property
    def log_level(self) -> str:
        return os.getenv("LOG_LEVEL", "INFO")

    def ensure_directories(self) -> None:
        """Create required directories if they don't exist."""
        (self.base_dir / self.log_dir).mkdir(parents=True, exist_ok=True)


# Singleton instance
settings = Settings()


# Blocket CSS selectors
BLOCKET_SELECTORS = {
    "listing_cards": "article.sf-search-ad",
    "listing_link": "a.sf-search-ad-link",
    "title": "h1.t1",
    "subtitle": "p.s-text-subtle",
    "price": "span.t2",
    "description": "div.whitespace-pre-wrap",
    "images": "img[id^='gallery-image-']",
    "specifications": "section.key-info-section dl",
    "equipment": "section:has(h2:contains('Equipment')) ul li",
}

# User agents for rotation
USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]
