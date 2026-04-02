import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT_DIR = os.path.join(BASE_DIR, os.getenv("OUTPUT_DIR", "data"))
DEFAULT_IMAGE_DIR = os.path.join(DEFAULT_OUTPUT_DIR, "images")

ALIEXPRESS_SEARCH_URL = "https://www.aliexpress.com/w/wholesale-{query}.html"

REQUEST_TIMEOUT = 30
REQUEST_DELAY_MIN = int(os.getenv("DELAY_MIN", 2))
REQUEST_DELAY_MAX = int(os.getenv("DELAY_MAX", 5))
MAX_RETRIES = 3
MAX_PAGES = int(os.getenv("MAX_PAGES", 5))
MAX_CONCURRENT_TABS = int(os.getenv("MAX_CONCURRENT_TABS", 3))
MAX_CONCURRENT_UPLOADS = int(os.getenv("MAX_CONCURRENT_UPLOADS", 5))

HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"
CHROME_PATH = os.getenv("CHROME_PATH", "") or None
CHROME_SANDBOX = os.getenv("CHROME_SANDBOX", "true").lower() == "true"

DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

CSV_FILENAME = "products.csv"
JSON_FILENAME = "products.json"

# MongoDB
MONGODB_URI = os.getenv("MONGODB_URI", "")

# Cloudflare Images
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")
CLOUDFLARE_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN", "")

PRODUCT_FIELDS = [
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
