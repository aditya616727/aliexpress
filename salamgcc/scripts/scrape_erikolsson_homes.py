#!/usr/bin/env python3
"""
Scrape ALL home listings from erikolsson.se and store in MongoDB.

Pipeline:
  1. Load brokers from MongoDB (or fallback to local JSON)
  2. Visit each broker's profile page → collect listing URLs (rent + sold)
  3. Visit each listing URL → scrape property data
  4. Map to target schema (similar to Blocket) → upsert into MongoDB

Usage:
  python scripts/scrape_erikolsson_homes.py
  python scripts/scrape_erikolsson_homes.py --limit 5          # limit brokers
  python scripts/scrape_erikolsson_homes.py --listing-limit 10  # limit listings per broker
"""

import json
import os
import re
import sys
import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import requests
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Add src to path so ad_extractor can be imported
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ad_extractor.database.mongodb import mongo_client

# Load .env
_project_root = Path(__file__).resolve().parents[1]
load_dotenv(_project_root / ".env")
load_dotenv()

# ────────────────────────── Constants ──────────────────────────
CURRENT_YEAR = datetime.now().year
BASE_URL = "https://www.erikolsson.se"
SOURCE_SITE = "erikolsson"
DEFAULT_COUNTRY = "Sweden"

CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID")
CLOUDFLARE_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN")
CLOUDFLARE_IMAGES_URL = (
    f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/images/v1"
)
CLOUDFLARE_UPLOAD_WORKERS = 4
NUM_SCRAPER_WORKERS = int(os.getenv("NUM_SCRAPER_WORKERS", "1"))

IMAGE_TEMP_DIR = Path.cwd() / "config" / "images_temp"
IMAGE_TEMP_DIR.mkdir(parents=True, exist_ok=True)

BROKERS_JSON_PATH = Path.cwd() / "config" / "erikolsson_brokers.json"

# ────────────────────────── Mappings ──────────────────────────
PER_MAP = {
    "mån": "monthly",
    "månad": "monthly",
    "dag": "day",
    "dygn": "day",
    "vecka": "weekly",
    "år": "yearly",
    "kvartal": "bi-monthly",
    "halvår": "half-yearly",
}

TYPE_MAP = {
    "lägenhet": "Apartment",
    "radhus": "Townhouse",
    "villa": "Villa",
    "fritidshus": "Villa",
    "tomt": "Villa",
    "kontor": "Office",
    "kontorslokal": "Office",
    "kontorslokaler": "Office",
    "butik": "Shop",
    "affärslokal": "Shop",
    "lager": "Warehouse",
    "garage": "Warehouse",
    "industri": "Factory",
    "fabrik": "Factory",
    "personalboende": "Staff Accommodation",
    "kommersiell": "Commercial Building",
    "kommersiell lokal": "Commercial Building",
    "affärshus": "Commercial Building",
    "kontorshus": "Commercial Floor",
}


def map_per(raw_per: Optional[str]) -> Optional[str]:
    if raw_per is None:
        return None
    return PER_MAP.get(raw_per.lower().strip(), raw_per)


def map_type(raw_type: Optional[str]) -> Optional[str]:
    if raw_type is None:
        return None
    return TYPE_MAP.get(raw_type.lower().strip(), raw_type)


# ────────────────────────── Helpers ──────────────────────────
def slugify(text: str) -> str:
    replacements = {"å": "a", "ä": "a", "ö": "o", "Å": "A", "Ä": "A", "Ö": "O"}
    for k, v in replacements.items():
        text = text.replace(k, v)
    text = re.sub(r"[^a-zA-Z0-9\s]", "", text)
    return re.sub(r"\s+", "_", text.strip()).upper()


def clean_duplicated_text(text: str) -> str:
    half = len(text) // 2
    if half and text[:half] == text[half:]:
        return text[:half]
    return text


def normalize_phone(phone: Optional[str]) -> Optional[int]:
    """Normalize phone to an integer. Removes +, -, spaces. Adds 46 for Sweden."""
    if not phone:
        return None
    raw = str(phone).strip()
    if not raw:
        return None
    compact = re.sub(r"[\s\-+]+", "", raw)
    if compact.startswith("46") and len(compact) > 6:
        try:
            return int(compact)
        except ValueError:
            return None
    if compact.startswith("0"):
        compact = f"46{compact[1:]}"
    try:
        return int(compact)
    except ValueError:
        return None


def strip_nulls(obj: Any) -> Any:
    """Recursively remove keys with None or empty string values."""
    if isinstance(obj, dict):
        return {k: strip_nulls(v) for k, v in obj.items() if v is not None and v != ""}
    if isinstance(obj, list):
        return [strip_nulls(i) for i in obj]
    return obj


def normalize_source_url(url: str) -> str:
    """Normalize listing URLs for deduplication and DB matching."""
    if not url:
        return url
    parsed = urlparse(url.strip())
    normalized = parsed._replace(
        netloc=parsed.netloc.lower(),
        path=parsed.path.rstrip("/"),
    )
    return urlunparse(normalized)


# ────────────────────────── Browser

def get_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--remote-debugging-port=0")
    options.add_argument("--window-size=1280,900")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    chrome_binary = os.getenv("CHROME_BINARY_PATH")
    if chrome_binary:
        print(f"Using Chrome binary: {chrome_binary}")
        options.binary_location = chrome_binary

    chromedriver_path = os.getenv("CHROMEDRIVER_PATH")
    if chromedriver_path:
        print(f"Using ChromeDriver path: {chromedriver_path}")
        service = Service(chromedriver_path)
        return webdriver.Chrome(service=service, options=options)

    print("CHROMEDRIVER_PATH not set; using webdriver-manager to install ChromeDriver")
    chromedriver_path = ChromeDriverManager().install()
    print(f"Using ChromeDriver installed at: {chromedriver_path}")
    service = Service(chromedriver_path)
    return webdriver.Chrome(service=service, options=options)


def handle_cookie_banner(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    try:
        btn = wait.until(EC.element_to_be_clickable((By.ID, "cc-b-acceptall")))
        btn.click()
        print("  ✓ Accepted cookies")
        time.sleep(1)
    except TimeoutException:
        pass


def handle_sf_popup(driver: webdriver.Chrome) -> None:
    try:
        btn = WebDriverWait(driver, 6).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".sf-button-wrapper.sf-anim"))
        )
        btn.click()
        print("  ✓ Closed sf popup")
        time.sleep(0.5)
    except TimeoutException:
        pass


# ────────────────────── Step 1: Load Brokers ──────────────────────
def load_brokers_from_mongodb() -> List[Dict[str, Any]]:
    """Load broker records from MongoDB brokers collection."""
    try:
        brokers = list(
            mongo_client.db.brokers.find(
                {"source_site": SOURCE_SITE},
                {"_id": 0, "name": 1, "source_url": 1, "email": 1, "phone": 1, "address": 1},
            )
        )
        return brokers
    except Exception as e:
        print(f"  ⚠ Could not load brokers from MongoDB: {e}")
        return []


def load_brokers_from_json() -> List[Dict[str, Any]]:
    """Fallback: load from local JSON."""
    if not BROKERS_JSON_PATH.exists():
        print(f"  ⚠ {BROKERS_JSON_PATH} not found")
        return []
    with open(BROKERS_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def load_brokers() -> List[Dict[str, Any]]:
    """Load brokers from MongoDB first, fallback to JSON."""
    brokers = load_brokers_from_mongodb()
    if brokers:
        print(f"  ✓ Loaded {len(brokers)} brokers from MongoDB")
        return brokers
    brokers = load_brokers_from_json()
    if brokers:
        print(f"  ✓ Loaded {len(brokers)} brokers from JSON")
    return brokers


# ──────────── Step 2: Collect Listing URLs per Broker ────────────
def get_listing_links(driver: webdriver.Chrome, section_label: str = "") -> List[str]:
    """Extract listing URLs from the current page's listings container."""
    links = []
    try:
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".css-z7gdp0"))
        )
        for a in container.find_elements(By.TAG_NAME, "a"):
            href = a.get_attribute("href")
            if href:
                links.append(href)
    except TimeoutException:
        print(f"    ⚠ Listings container not found for {section_label}")

    seen = set()
    unique = []
    for link in links:
        if link not in seen:
            seen.add(link)
            unique.append(link)
    return unique


def click_sold_tab(driver: webdriver.Chrome) -> bool:
    """Click the second tab (sold listings)."""
    try:
        tab_list = WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".chakra-tabs__tablist.css-1xhq01z")
            )
        )
        tabs = tab_list.find_elements(By.TAG_NAME, "button")
        if len(tabs) >= 2:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tabs[1])
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", tabs[1])
            time.sleep(1.5)
            return True
    except TimeoutException:
        pass
    return False


def collect_broker_listing_urls(
    driver: webdriver.Chrome, broker: Dict[str, Any], cookie_accepted: bool
) -> Tuple[Dict[str, List[str]], bool]:
    """
    Visit a broker's profile page and collect rent + sold listing URLs.
    Returns ({rent: [...], sold: [...]}, cookie_accepted).
    """
    url = broker.get("source_url", "")
    name = broker.get("name", "Unknown")
    result = {"rent": [], "sold": []}

    if not url:
        return result, cookie_accepted

    try:
        driver.get(url)
        time.sleep(2)

        if not cookie_accepted:
            handle_cookie_banner(driver, WebDriverWait(driver, 15))
            cookie_accepted = True
        handle_sf_popup(driver)

        result["rent"] = get_listing_links(driver, section_label="rent")
        print(f"    sell: {len(result['rent'])} listings")

    except Exception as e:
        print(f"    ✗ Failed: {type(e).__name__}: {e}")

    return result, cookie_accepted


# ──────────── Step 3: Scrape Individual Listing Pages ────────────

def scrape_title(driver: webdriver.Chrome) -> Optional[str]:
    try:
        el = driver.find_element(By.CSS_SELECTOR, ".chakra-stack.css-1uodvt1")
        return el.text.strip() or None
    except NoSuchElementException:
        return None


def scrape_description(driver: webdriver.Chrome) -> Optional[str]:
    try:
        read_more = driver.find_element(By.CSS_SELECTOR, ".chakra-button.css-1tq7k7n")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", read_more)
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", read_more)
        time.sleep(0.5)
    except NoSuchElementException:
        pass
    try:
        el = driver.find_element(By.CSS_SELECTOR, ".chakra-text.css-zw05wl")
        return el.text.strip() or None
    except NoSuchElementException:
        return None


def scrape_address(driver: webdriver.Chrome) -> Optional[str]:
    try:
        el = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".chakra-text.css-1923m1y"))
        )
        text = el.text.strip() or driver.execute_script("return arguments[0].innerText;", el).strip()
        return clean_duplicated_text(text) or None
    except TimeoutException:
        return None


def _get_label_value_pairs(driver: webdriver.Chrome) -> List[Tuple[str, str]]:
    """Extract all label/value pairs from property details section."""
    pairs = []
    try:
        labels = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".chakra-text.css-xqs6kp"))
        )
        for label in labels:
            label_text = label.text.strip() or driver.execute_script(
                "return arguments[0].innerText;", label
            ).strip()
            label_text = clean_duplicated_text(label_text).strip()
            try:
                parent = label.find_element(By.XPATH, "..")
                value_el = parent.find_element(By.CSS_SELECTOR, ".chakra-text.css-wguytq")
                value = value_el.text.strip() or driver.execute_script(
                    "return arguments[0].innerText;", value_el
                ).strip()
                value = clean_duplicated_text(value).strip()
                if label_text and value:
                    pairs.append((label_text, value))
            except NoSuchElementException:
                continue
    except TimeoutException:
        pass
    return pairs


def scrape_age(pairs: List[Tuple[str, str]]) -> Optional[int]:
    for label, value in pairs:
        if "byggår" in label.lower():
            try:
                return CURRENT_YEAR - int(value)
            except ValueError:
                pass
    return None


def parse_room_number(text: str) -> int:
    match = re.search(r"\d+", text)
    return int(match.group()) if match else 0


def parse_beds(text: str) -> int:
    range_match = re.search(r"(\d+)-(\d+)", text)
    if range_match:
        return int(range_match.group(1))
    match = re.search(r"\d+", text)
    return int(match.group()) if match else 1


def scrape_beds_and_baths(pairs: List[Tuple[str, str]]) -> Tuple[int, int]:
    beds = 1
    baths = 0
    total_rooms = 0
    found_beds = False

    for label, value in pairs:
        low = label.lower()
        if low == "antal rum":
            total_rooms = parse_room_number(value)
        elif low == "varav sovrum":
            beds = parse_beds(value)
            found_beds = True

    if total_rooms > 0:
        baths = max(0, total_rooms - beds)
    if beds == 1:
        baths = 1

    return beds, baths


def scrape_price(pairs: List[Tuple[str, str]], driver: webdriver.Chrome) -> Tuple[Optional[int], Optional[str], Optional[int]]:
    monthly_fee = None
    price = None
    raw_per = None

    for label, value in pairs:
        if label.lower() == "avgift":
            value = value.replace("\xa0", " ")
            digits = re.sub(r"[^\d]", "", value)
            per_match = re.search(r"/\s*(\S+)", value)
            if digits:
                monthly_fee = int(digits)
            raw_per = per_match.group(1) if per_match else None
            break

    # Standalone price element (asking/sale price overrides avgift as price)
    try:
        price_el = driver.find_element(By.CSS_SELECTOR, ".chakra-text.css-zrequa")
        value = price_el.text.strip() or driver.execute_script(
            "return arguments[0].innerText;", price_el
        ).strip()
        value = clean_duplicated_text(value).strip()
        digits = re.sub(r"[^\d]", "", value)
        if digits:
            price = int(digits)
            raw_per = "one-time"
    except NoSuchElementException:
        pass

    return price, raw_per, monthly_fee


def scrape_type(pairs: List[Tuple[str, str]]) -> Optional[str]:
    for label, value in pairs:
        if label.lower() == "boendetyp":
            return value or None
    return None


def scrape_size(pairs: List[Tuple[str, str]]) -> Optional[int]:
    for label, value in pairs:
        if label.lower() == "boarea":
            numbers = re.findall(r"\d+", value)
            if numbers:
                return sum(int(n) for n in numbers)
    return None


def scrape_seller(driver: webdriver.Chrome) -> Tuple[Optional[str], Optional[int]]:
    seller_name = None
    seller_number = None
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".chakra-text.css-uje8k6"))
        )
        try:
            name_el = driver.find_element(By.CSS_SELECTOR, "h2.chakra-heading.css-g2gvnk")
            text = name_el.text.strip() or driver.execute_script(
                "return arguments[0].innerText;", name_el
            ).strip()
            seller_name = re.sub(r"\s+", " ", clean_duplicated_text(text)) or None
        except NoSuchElementException:
            pass
        try:
            contact_section = driver.find_element(By.CSS_SELECTOR, ".chakra-stack.css-jze4kf")
            phone_link = contact_section.find_element(By.CSS_SELECTOR, "a[href^='tel:']")
            href = phone_link.get_attribute("href") or ""
            raw = href.replace("tel:", "").strip()
            seller_number = normalize_phone(raw)
        except NoSuchElementException:
            pass
    except TimeoutException:
        pass
    return seller_name, seller_number


def scrape_amenities(driver: webdriver.Chrome) -> Optional[str]:
    try:
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".css-138u0vj"))
        )
        items = container.find_elements(By.CSS_SELECTOR, ".chakra-text.css-1jqr5c8")
        amenities = []
        for item in items:
            text = item.text.strip() or driver.execute_script(
                "return arguments[0].innerText;", item
            ).strip()
            text = clean_duplicated_text(text)
            if text:
                amenities.append(text)
        return ", ".join(amenities) if amenities else None
    except TimeoutException:
        return None


def scrape_additional_fields(pairs: List[Tuple[str, str]]) -> Optional[Dict[str, Any]]:
    SKIP = {"boarea", "antal rum", "varav sovrum", "byggår", "avgift", "boendetyp"}
    fields = {}
    for label, value in pairs:
        if label.lower() not in SKIP:
            fields[label] = value
    return fields if fields else None


# ─────────────────── Image Upload ───────────────────
def _download_image(img_url: str, index: int) -> Optional[Path]:
    try:
        resp = requests.get(img_url, timeout=10)
        if not resp.ok:
            return None
        local = IMAGE_TEMP_DIR / f"{uuid.uuid4().hex}.jpg"
        local.write_bytes(resp.content)
        return local
    except Exception:
        return None


def _upload_to_cloudflare(local_path: Path, index: int) -> Optional[str]:
    try:
        with open(local_path, "rb") as f:
            resp = requests.post(
                CLOUDFLARE_IMAGES_URL,
                headers={"Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}"},
                files={"file": (local_path.name, f, "image/jpeg")},
                timeout=30,
            )
        resp.raise_for_status()
        data = resp.json()
        if data.get("success"):
            variants = data["result"]["variants"]
            return variants[0] if variants else None
        return None
    except Exception:
        return None
    finally:
        try:
            local_path.unlink()
        except Exception:
            pass


def _process_image(args: Tuple[str, int]) -> Optional[str]:
    img_url, index = args
    local = _download_image(img_url, index)
    if local is None:
        return None
    return _upload_to_cloudflare(local, index)


def scrape_images(driver: webdriver.Chrome) -> List[str]:
    # Click "All images" button
    try:
        btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, ".css-u7ydbr .chakra-stack.css-1xq1he2")
            )
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(1.5)
    except TimeoutException:
        try:
            btn = driver.find_element(
                By.XPATH,
                "//*[contains(@class, 'chakra-stack') and "
                ".//p[contains(text(), 'All images') or contains(text(), 'Alla bilder')]]",
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(1.5)
        except NoSuchElementException:
            return []

    # Wait for gallery
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".chakra-stack.css-3ueif"))
        )
    except TimeoutException:
        return []

    container = driver.find_element(By.CSS_SELECTOR, ".chakra-stack.css-3ueif")

    # Scroll through gallery to lazy-load
    ActionChains(driver).move_to_element(container).click().perform()
    time.sleep(0.5)
    for _ in range(20):
        ActionChains(driver).send_keys(Keys.PAGE_DOWN).perform()
        time.sleep(0.5)
    time.sleep(1)

    img_elements = container.find_elements(By.CSS_SELECTOR, "img.chakra-image.css-1phd9a0")
    raw_urls = list(dict.fromkeys(
        img.get_attribute("src")
        for img in img_elements
        if img.get_attribute("src") and img.get_attribute("src").startswith("http")
    ))

    if not raw_urls:
        return []

    print(f"    Found {len(raw_urls)} images — uploading to Cloudflare...")

    cf_urls: List[Optional[str]] = [None] * len(raw_urls)
    with ThreadPoolExecutor(max_workers=CLOUDFLARE_UPLOAD_WORKERS) as executor:
        future_to_idx = {
            executor.submit(_process_image, (url, i)): i
            for i, url in enumerate(raw_urls)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                cf_urls[idx] = future.result()
            except Exception:
                pass

    result = [u for u in cf_urls if u is not None]
    print(f"    ✓ Uploaded {len(result)}/{len(raw_urls)} images")
    return result


# ──────────── Scrape a Single Listing Page ────────────
def scrape_listing(
    driver: webdriver.Chrome,
    url: str,
    ad_type: str,
    broker: Dict[str, Any],
    cookie_accepted: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Visit a listing page, extract data, and return a document matching the
    target schema (similar to the Blocket car listing format, adapted for real estate).
    """
    try:
        driver.get(url)
        time.sleep(2)

        if not cookie_accepted:
            handle_cookie_banner(driver, WebDriverWait(driver, 15))
        handle_sf_popup(driver)

        title = scrape_title(driver)
        description = scrape_description(driver)
        amenities = scrape_amenities(driver)
        address = scrape_address(driver)
        pairs = _get_label_value_pairs(driver)
        age = scrape_age(pairs)
        beds, baths = scrape_beds_and_baths(pairs)
        price, raw_per, monthly_fee = scrape_price(pairs, driver)
        raw_type = scrape_type(pairs)
        size = scrape_size(pairs)
        seller_name, seller_number = scrape_seller(driver)
        additional_fields = scrape_additional_fields(pairs)
        images = scrape_images(driver)

        # Build business_id from broker name + address
        broker_name = broker.get("name", "")
        broker_addr = broker.get("address", "")
        business_id = slugify(f"{broker_name} {broker_addr}") if broker_name else None

        now = datetime.utcnow()

        return {
            "business_id": business_id,
            "source_url": url,
            "source_site": SOURCE_SITE,
            "isPosted": False,
            "postAdData": {
                "title": title,
                "description": description,
                "adType": "sell" if ad_type in ("sold", None, "") else ad_type,
                "price": price if price is not None else 0,
                "per": map_per(raw_per),
                "type": map_type(raw_type),
                "beds": beds,
                "baths": baths,
                "size": size,
                "sizeType": "m²",
                "age": age,
                "amenities": amenities,
                "images": images,
                "country": DEFAULT_COUNTRY,
                "city": address.split(",")[0].strip() if address and "," in address else address,
                "address": address,
                "state": address.split(",")[-1].strip() if address and "," in address else None,
                "sellerName": seller_name,
                "sellerNumber": str(seller_number) if seller_number else None,
                "completionStatus": None,
                "furnishing": None,
                "monthlyFee": monthly_fee,
                "additionalFields": additional_fields,
            },
            "subtitle": None,
            "agent_name": broker.get("name"),
            "agent_email": broker.get("email"),
            "agent_phone": normalize_phone(str(broker.get("phone", ""))) if broker.get("phone") else None,
            "agent_location": broker.get("address"),
            "scraped_at": now,
            "updatedAt": now,
        }

    except Exception as e:
        print(f"    ✗ Error scraping {url}: {type(e).__name__}: {e}")
        return None


# ──────────────────── Main Pipeline ────────────────────
def main() -> None:
    # Parse CLI args
    broker_limit = None
    listing_limit = None

    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        if idx + 1 < len(sys.argv):
            try:
                broker_limit = int(sys.argv[idx + 1])
            except ValueError:
                pass

    if "--listing-limit" in sys.argv:
        idx = sys.argv.index("--listing-limit")
        if idx + 1 < len(sys.argv):
            try:
                listing_limit = int(sys.argv[idx + 1])
            except ValueError:
                pass

    # Connect to MongoDB
    connected = mongo_client.connect()
    if not connected:
        print("✗ Failed to connect to MongoDB")
        sys.exit(1)

    try:
        # Step 1: Load brokers
        print("\n── Step 1: Loading brokers ──")
        brokers = load_brokers()
        if not brokers:
            print("✗ No brokers found. Run scrape_and_store_brokers.py first.")
            sys.exit(1)

        if broker_limit:
            brokers = brokers[:broker_limit]
            print(f"  Limited to {broker_limit} brokers")

        # Step 2: Collect and scrape listings broker-by-broker
        print(f"\n── Step 2: Collecting and scraping listings broker-by-broker ──")
        driver = get_driver()
        cookie_accepted = False
        collection = mongo_client.db["listings"]
        counter_lock = threading.Lock()
        counters = {"inserted": 0, "failed": 0, "done": 0}
        global_seen_urls: set[str] = set()

        def _worker(items: List[Tuple[Dict[str, Any], str, str]], worker_id: int) -> None:
            drv = get_driver()
            cookie_ok = False
            try:
                for broker, url, ad_type in items:
                    with counter_lock:
                        counters["done"] += 1
                        seq = counters["done"]
                    print(f"\n  [W{worker_id}] [{seq}] {url}")

                    # Restart driver if session is dead
                    try:
                        _ = drv.current_url
                    except Exception:
                        print(f"    [W{worker_id}] ⚠ Restarting driver...")
                        try:
                            drv.quit()
                        except Exception:
                            pass
                        drv = get_driver()
                        cookie_ok = False

                    listing = scrape_listing(drv, url, ad_type, broker, cookie_ok)
                    if not listing:
                        with counter_lock:
                            counters["failed"] += 1
                        continue

                    cookie_ok = True
                    listing = strip_nulls(listing)

                    try:
                        result = collection.update_one(
                            {"source_url": url},
                            {"$set": listing},
                            upsert=True,
                        )
                        with counter_lock:
                            if result.upserted_id:
                                counters["inserted"] += 1
                                title = listing.get("postAdData", {}).get("title", "?")
                                print(f"    [W{worker_id}] ✓ Inserted: {title}")
                            elif result.modified_count:
                                counters["inserted"] += 1
                                print(f"    [W{worker_id}] ✓ Updated")
                            else:
                                print(f"    [W{worker_id}] ~ No changes")
                    except Exception as e:
                        print(f"    [W{worker_id}] ✗ DB error: {e}")
                        with counter_lock:
                            counters["failed"] += 1
            finally:
                drv.quit()

        def _scrape_batch(listings: List[Tuple[Dict[str, Any], str, str]], broker_name: str) -> None:
            batch_total = len(listings)
            if batch_total == 0:
                return
            workers = min(NUM_SCRAPER_WORKERS, batch_total)
            print(f"  Using {workers} scraper worker(s) for {broker_name}")
            chunks: List[List[Tuple[Dict[str, Any], str, str]]] = [[] for _ in range(workers)]
            for idx, item in enumerate(listings):
                chunks[idx % workers].append(item)

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [
                    executor.submit(_worker, chunks[i], i + 1)
                    for i in range(workers)
                ]
                for f in as_completed(futures):
                    f.result()  # propagate exceptions

        for i, broker in enumerate(brokers):
            name = broker.get("name", "Unknown")
            print(f"\n  [{i + 1}/{len(brokers)}] {name}")

            # Restart driver if session is dead
            try:
                _ = driver.current_url
            except Exception:
                print("    ⚠ Driver session lost, restarting...")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = get_driver()
                cookie_accepted = False

            urls, cookie_accepted = collect_broker_listing_urls(driver, broker, cookie_accepted)
            broker_urls = [normalize_source_url(url) for url in urls.get("rent", []) if url]

            if not broker_urls:
                print("  No listing URLs found for this broker")
                continue

            broker_urls = [url for url in broker_urls if url not in global_seen_urls]
            if not broker_urls:
                print("  No new listing URLs after global dedupe")
                continue

            seen_local = set()
            broker_unique_urls: List[str] = []
            for url in broker_urls:
                if url not in seen_local:
                    seen_local.add(url)
                    broker_unique_urls.append(url)

            existing_urls = set()
            try:
                cursor = collection.find(
                    {"source_url": {"$in": broker_unique_urls}},
                    {"source_url": 1, "_id": 0},
                )
                existing_urls = {normalize_source_url(doc["source_url"]) for doc in cursor if doc.get("source_url")}
            except Exception as e:
                print(f"  ⚠ Failed to load existing listings from DB for broker {name}: {e}")

            new_urls = [url for url in broker_unique_urls if url not in existing_urls]
            skipped = len(broker_unique_urls) - len(new_urls)
            if skipped:
                print(f"  Skipping {skipped} already scraped listings for {name}")

            if not new_urls:
                print(f"  No new listings to scrape for {name}")
                continue

            if listing_limit is not None:
                if listing_limit <= 0:
                    print("  Listing limit reached, stopping")
                    break
                if len(new_urls) > listing_limit:
                    new_urls = new_urls[:listing_limit]
                    print(f"  Limited to {len(new_urls)} listings for {name}")
                    listing_limit -= len(new_urls)

            broker_listings = [(broker, url, "sell") for url in new_urls]
            print(f"  Scraping {len(broker_listings)} listings for {name}")
            _scrape_batch(broker_listings, name)
            global_seen_urls.update(new_urls)

            if listing_limit is not None and listing_limit <= 0:
                break

        driver.quit()

        inserted = counters["inserted"]
        failed = counters["failed"]

        print(f"\n{'='*50}")
        print(f"✓ Done — {inserted} listings saved, {failed} failed")

    finally:
        mongo_client.close()


if __name__ == "__main__":
    main()

