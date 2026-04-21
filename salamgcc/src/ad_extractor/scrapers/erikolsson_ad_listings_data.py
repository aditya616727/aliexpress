"""
Scrape apartment listing data from Erik Olsson listing pages.
"""

import json
import os
import sys
import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import re
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from pathlib import Path
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))
from ad_extractor.database.mongodb import mongo_client

# Load .env file before reading any env variables
load_dotenv()

CURRENT_YEAR = datetime.now().year
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID")
CLOUDFLARE_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN")
CLOUDFLARE_IMAGES_URL = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/images/v1"

IMAGE_TEMP_DIR = Path.cwd() / "config" / "images_temp"
IMAGE_TEMP_DIR.mkdir(parents=True, exist_ok=True)

CLOUDFLARE_UPLOAD_WORKERS = 4  # parallel upload threads

LISTING_URLS_FILE = Path.cwd() / "config/broker_listing_urls.json"

with open(LISTING_URLS_FILE, "r", encoding="utf-8") as f:
    BROKER_RESULTS = json.load(f)

def get_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,900")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(options=options)



def handle_cookie_banner(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    """Accept cookie banner if present. Only needs to be done once per session."""
    try:
        btn = wait.until(EC.element_to_be_clickable((By.ID, "cc-b-acceptall")))
        btn.click()
        print("  ✓ Accepted cookies")
        time.sleep(1)
    except TimeoutException:
        pass  # Already accepted or not shown


def handle_sf_popup(driver: webdriver.Chrome) -> None:
    """Click the sf-button-wrapper popup if it appears."""
    try:
        btn = WebDriverWait(driver, 6).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, ".sf-button-wrapper.sf-anim")
            )
        )
        btn.click()
        print("  ✓ Closed sf popup")
        time.sleep(0.5)
    except TimeoutException:
        pass  # Popup didn't appear, that's fine


def safe_find_text(driver: webdriver.Chrome, css_selector: str) -> Optional[str]:
    """Find element by CSS selector and return its text, or None if not found."""
    try:
        el = driver.find_element(By.CSS_SELECTOR, css_selector)
        return el.text.strip() or None
    except NoSuchElementException:
        return None


def scrape_title(driver: webdriver.Chrome) -> Optional[str]:
    """Extract listing title."""
    try:
        el = driver.find_element(By.CSS_SELECTOR, ".chakra-stack.css-1uodvt1")
        return el.text.strip() or None
    except NoSuchElementException:
        return None


def scrape_description(driver: webdriver.Chrome) -> Optional[str]:
    """Click 'read more' to expand description, then extract full text."""
    # Click the read more button if present
    try:
        read_more = driver.find_element(By.CSS_SELECTOR, ".chakra-button.css-1tq7k7n")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", read_more)
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", read_more)
        time.sleep(0.5)
    except NoSuchElementException:
        pass  # No read more button, description may already be fully visible

    # Extract the description text
    try:
        el = driver.find_element(By.CSS_SELECTOR, ".chakra-text.css-zw05wl")
        return el.text.strip() or None
    except NoSuchElementException:
        return None


def scrape_address(driver: webdriver.Chrome) -> Optional[str]:
    """Extract address from paragraph element."""
    try:
        el = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".chakra-text.css-1923m1y"))
        )
        # Try .text first, fall back to JS innerText
        text = el.text.strip() or driver.execute_script("return arguments[0].innerText;", el).strip()
        return clean_duplicated_text(text) or None
    except TimeoutException:
        print("    ⚠ Address element not found")
        return None

def clean_duplicated_text(text: str) -> str:
    """Fix text duplicated by nested <font> tags."""
    half = len(text) // 2
    if half and text[:half] == text[half:]:
        return text[:half]
    return text

def scrape_age(driver: webdriver.Chrome) -> Optional[int]:
    """Find 'Year of construction' label and extract its sibling value."""
    try:
        # Find all label elements
        labels = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".chakra-text.css-xqs6kp"))
        )
        for label in labels:
            text = label.text.strip() or driver.execute_script("return arguments[0].innerText;", label).strip()
            text = clean_duplicated_text(text)
            if "byggår" in text.lower():
                # The value is the next sibling <p> with class css-wguytq
                parent = label.find_element(By.XPATH, "..")
                value_el = parent.find_element(By.CSS_SELECTOR, ".chakra-text.css-wguytq")
                value = value_el.text.strip() or driver.execute_script("return arguments[0].innerText;", value_el).strip()
                value = clean_duplicated_text(value).strip()
                construction_year = int(value)
                return CURRENT_YEAR - construction_year
    except TimeoutException:
        print("    ⚠ Property details container not found")
    except ValueError:
        print(f"    ⚠ Could not parse construction year from: '{value}'")
    return None

def parse_room_number(text: str) -> int:
    """Extract the first number from a rooms string like '3 rum och kök'."""
    match = re.search(r"\d+", text)
    return int(match.group()) if match else 0


def parse_beds(text: str) -> int:
    """
    Parse bedroom count from 'Varav sovrum' value.
    Handles exact values like '2 st' and ranges like '3-4 st' (takes lower bound).
    """
    # Range like "3-4 st" → take lower bound
    range_match = re.search(r"(\d+)-(\d+)", text)
    if range_match:
        return int(range_match.group(1))
    # Exact like "2 st"
    match = re.search(r"\d+", text)
    return int(match.group()) if match else 1


def scrape_beds_and_baths(driver: webdriver.Chrome):
    """
    Extract beds and baths from the property details container.
    Returns (beds, baths) tuple.
    """
    beds = 1   # default
    baths = 0
    total_rooms = 0

    try:
        labels = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".chakra-text.css-xqs6kp"))
        )

        for label in labels:
            text = label.text.strip() or driver.execute_script("return arguments[0].innerText;", label).strip()
            text = clean_duplicated_text(text).strip().lower()

            parent = label.find_element(By.XPATH, "..")
            value_el = parent.find_element(By.CSS_SELECTOR, ".chakra-text.css-wguytq")
            value = value_el.text.strip() or driver.execute_script("return arguments[0].innerText;", value_el).strip()
            value = clean_duplicated_text(value).strip()

            if text == "antal rum":
                total_rooms = parse_room_number(value)

            elif text == "varav sovrum":
                beds = parse_beds(value)

        # If "Varav sovrum" was not present, default beds to 1
        # baths = total_rooms - beds (remaining rooms assumed to be bathrooms etc.)
        if total_rooms > 0:
            baths = max(0, total_rooms - beds)
            
        # If beds defaulted to 1, baths also default to 1
        if beds == 1:
            baths = 1

    except TimeoutException:
        print("    ⚠ Could not find property details for beds/baths")

    return beds, baths

def _download_image(img_url: str, index: int) -> Optional[Path]:
    """
    Download a single image to a temp file on disk.
    Returns the local Path on success, None on failure.
    """
    try:
        response = requests.get(img_url, timeout=10)
        if not response.ok:
            print(f"    ⚠ Could not download image {index + 1}: HTTP {response.status_code}")
            return None
        local_path = IMAGE_TEMP_DIR / f"{uuid.uuid4().hex}.jpg"
        local_path.write_bytes(response.content)
        return local_path
    except Exception as e:
        print(f"    ⚠ Download error for image {index + 1}: {e}")
        return None


def _upload_to_cloudflare(local_path: Path, index: int) -> Optional[str]:
    """
    Upload a locally saved image to Cloudflare Images.
    Returns the Cloudflare variant URL on success, None on failure.
    Deletes the local file regardless of outcome.
    """
    try:
        with open(local_path, "rb") as f:
            cf_response = requests.post(
                CLOUDFLARE_IMAGES_URL,
                headers={"Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}"},
                files={"file": (local_path.name, f, "image/jpeg")},
                timeout=30,
            )
        cf_response.raise_for_status()
        data = cf_response.json()

        if data.get("success"):
            variants = data["result"]["variants"]
            return variants[0] if variants else None
        else:
            print(f"    ⚠ Cloudflare upload failed for image {index + 1}: {data.get('errors')}")
            return None
    except Exception as e:
        print(f"    ⚠ Upload error for image {index + 1}: {e}")
        return None
    finally:
        # Always clean up local file
        try:
            local_path.unlink()
        except Exception:
            pass


def _process_image_worker(args: Tuple[str, int]) -> Optional[str]:
    """
    Worker: download image to disk → upload to Cloudflare → delete local file.
    Returns Cloudflare URL or None.
    """
    img_url, index = args
    local_path = _download_image(img_url, index)
    if local_path is None:
        return None
    return _upload_to_cloudflare(local_path, index)


def scrape_images(driver: webdriver.Chrome) -> List[str]:
    """
    Click 'All images', scroll to trigger lazy loading, collect image URLs,
    then use a thread pool to: save each image locally → upload to Cloudflare
    → delete local file. Returns list of Cloudflare URLs.
    """
    # Click the "All images" button
    try:
        all_images_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".css-u7ydbr .chakra-stack.css-1xq1he2"))
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", all_images_btn)
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", all_images_btn)
        time.sleep(1.5)
    except TimeoutException:
        try:
            all_images_btn = driver.find_element(
                By.XPATH, "//*[contains(@class, 'chakra-stack') and .//p[contains(text(), 'All images') or contains(text(), 'Alla bilder')]]"
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", all_images_btn)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", all_images_btn)
            time.sleep(1.5)
        except NoSuchElementException:
            print("    ⚠ 'All images' button not found")
            return []

    # Wait for gallery container
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".chakra-stack.css-3ueif"))
        )
    except TimeoutException:
        print("    ⚠ Image gallery container not found")
        return []

    container = driver.find_element(By.CSS_SELECTOR, ".chakra-stack.css-3ueif")

    # Scroll through gallery to trigger lazy loading
    ActionChains(driver).move_to_element(container).click().perform()
    time.sleep(0.5)
    for _ in range(20):
        ActionChains(driver).send_keys(Keys.PAGE_DOWN).perform()
        time.sleep(0.5)
    time.sleep(1)

    # Collect image URLs
    img_elements = container.find_elements(By.CSS_SELECTOR, "img.chakra-image.css-1phd9a0")
    raw_urls = list(dict.fromkeys(
        img.get_attribute("src")
        for img in img_elements
        if img.get_attribute("src") and img.get_attribute("src").startswith("http")
    ))

    if not raw_urls:
        print("    ⚠ No images found")
        return []

    print(f"    Found {len(raw_urls)} images — downloading, uploading to Cloudflare...")

    # Worker pool: download → save locally → upload → delete local
    cloudflare_urls: List[Optional[str]] = [None] * len(raw_urls)
    with ThreadPoolExecutor(max_workers=CLOUDFLARE_UPLOAD_WORKERS) as executor:
        future_to_index = {
            executor.submit(_process_image_worker, (url, i)): i
            for i, url in enumerate(raw_urls)
        }
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                cloudflare_urls[idx] = future.result()
            except Exception as e:
                print(f"    ⚠ Worker failed for image {idx + 1}: {e}")

    # Filter out failed uploads, preserve order
    result = [url for url in cloudflare_urls if url is not None]
    print(f"    ✓ Uploaded {len(result)}/{len(raw_urls)} images to Cloudflare")
    return result


def scrape_price(driver: webdriver.Chrome):
    """
    Extract price and per fields.
    - If 'Avgift' label exists: split value into price (number) and per (period)
      e.g. "3 952 kr / mån" → price=3952, per="mån"
    - If not: price from css-zrequa element, per="one-time"
    Returns (price, per) tuple.
    """
    try:
        labels = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".chakra-text.css-xqs6kp"))
        )

        for label in labels:
            text = label.text.strip() or driver.execute_script("return arguments[0].innerText;", label).strip()
            text = clean_duplicated_text(text).strip().lower()

            if text == "avgift":
                parent = label.find_element(By.XPATH, "..")
                value_el = parent.find_element(By.CSS_SELECTOR, ".chakra-text.css-wguytq")
                value = value_el.text.strip() or driver.execute_script("return arguments[0].innerText;", value_el).strip()
                value = clean_duplicated_text(value).strip()

                # e.g. "3 952 kr / mån" → price=3952, per="mån"
                # Remove non-breaking spaces and extract number
                value = value.replace("\xa0", " ")
                price_match = re.search(r"[\d\s]+", value)
                per_match = re.search(r"/\s*(\S+)", value)

                price = int(re.sub(r"\s+", "", price_match.group())) if price_match else None
                per = per_match.group(1) if per_match else None
                return price, per

    except TimeoutException:
        pass

    # Fallback: no Avgift label — get price from css-zrequa
    try:
        price_el = driver.find_element(By.CSS_SELECTOR, ".chakra-text.css-zrequa")
        value = price_el.text.strip() or driver.execute_script("return arguments[0].innerText;", price_el).strip()
        value = clean_duplicated_text(value).strip()

        # e.g. "3,250,000 SEK" → price=3250000
        price_match = re.search(r"[\d,\s]+", value)
        price = int(re.sub(r"[,\s]", "", price_match.group())) if price_match else None
        return price, "one-time"

    except NoSuchElementException:
        pass

    return None, None



def scrape_type(driver: webdriver.Chrome) -> Optional[str]:
    """Extract property type from 'Boendetyp' label in property details."""
    try:
        labels = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".chakra-text.css-xqs6kp"))
        )
        for label in labels:
            text = label.text.strip() or driver.execute_script("return arguments[0].innerText;", label).strip()
            text = clean_duplicated_text(text).strip()
            if text.lower() == "boendetyp":
                parent = label.find_element(By.XPATH, "..")
                value_el = parent.find_element(By.CSS_SELECTOR, ".chakra-text.css-wguytq")
                value = value_el.text.strip() or driver.execute_script("return arguments[0].innerText;", value_el).strip()
                return clean_duplicated_text(value).strip() or None
    except TimeoutException:
        print("    ⚠ Type element not found")
    return None


def scrape_seller(driver: webdriver.Chrome):
    """
    Extract seller name and phone number from broker card.
    Returns (sellerName, sellerNumber) tuple.
    """
    seller_name = None
    seller_number = None

    try:
        # Find the broker card by locating the "Responsible broker" label
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".chakra-text.css-uje8k6"))
        )

        # Find the h2 heading directly — it's unique to the broker card
        try:
            name_el = driver.find_element(By.CSS_SELECTOR, "h2.chakra-heading.css-g2gvnk")
            text = name_el.text.strip() or driver.execute_script("return arguments[0].innerText;", name_el).strip()
            seller_name = re.sub(r"\s+", " ", clean_duplicated_text(text)) or None
        except NoSuchElementException:
            pass

        # Find phone link scoped inside the same section
        try:
            # The contact section is a sibling of the profile link, both inside css-11nrrcx
            contact_section = driver.find_element(By.CSS_SELECTOR, ".chakra-stack.css-jze4kf")
            phone_link = contact_section.find_element(By.CSS_SELECTOR, "a[href^='tel:']")
            href = phone_link.get_attribute("href") or ""
            raw = href.replace("tel:", "").strip()
            # Remove '-' and whitespace, then add +46 if missing (Sweden)
            compact = re.sub(r"[\s-]+", "", raw)
            if compact.startswith("0"):
                seller_number = f"+46{compact[1:]}"
            elif compact.startswith("46"):
                seller_number = f"+{compact}"
            else:
                seller_number = compact or None
        except NoSuchElementException:
            pass

    except TimeoutException:
        print("    ⚠ Broker card not found")

    return seller_name, seller_number



# Mapping from Swedish "per" values scraped from the site to allowed enum values
PER_MAP = {
    "mån": "monthly",
    "månad": "monthly",
    "dag": "day",
    "dygn": "day",
    "vecka": "weekly",
    "år": "yearly",
    "kvartal": "bi-monthly",   # closest match
    "halvår": "half-yearly",
}

# Mapping from scraped Swedish property type to allowed enum values
TYPE_MAP = {
    # Boendetyp values from Erik Olsson
    "lägenhet": "Apartment",
    "radhus": "Townhouse",
    "villa": "Villa",
    "fritidshus": "Villa",
    "tomt": "Villa",
    # Commercial
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
    """Map scraped Swedish per value to allowed enum."""
    if raw_per is None:
        return None
    return PER_MAP.get(raw_per.lower().strip(), raw_per)


def map_type(raw_type: Optional[str]) -> Optional[str]:
    """Map scraped Swedish property type to allowed enum."""
    if raw_type is None:
        return None
    return TYPE_MAP.get(raw_type.lower().strip(), raw_type)


def scrape_size(driver: webdriver.Chrome) -> Optional[int]:
    """
    Extract living area (Boarea) and return as integer.
    Handles simple values like "35 m²" and combined like "115 + 20 m²".
    """
    try:
        labels = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".chakra-text.css-xqs6kp"))
        )

        for label in labels:
            text = label.text.strip() or driver.execute_script("return arguments[0].innerText;", label).strip()
            text = clean_duplicated_text(text).strip().lower()

            if text == "boarea":
                parent = label.find_element(By.XPATH, "..")
                value_el = parent.find_element(By.CSS_SELECTOR, ".chakra-text.css-wguytq")
                value = value_el.text.strip() or driver.execute_script("return arguments[0].innerText;", value_el).strip()
                value = clean_duplicated_text(value).strip()

                # Extract all numbers and sum them
                # Handles "35 m²" → 35 and "115 + 20 m²" → 135
                numbers = re.findall(r"\d+", value)
                if numbers:
                    return sum(int(n) for n in numbers)

    except TimeoutException:
        print("    ⚠ Could not find property details for size")

    return None



def scrape_amenities(driver: webdriver.Chrome) -> Optional[str]:
    """Extract all amenity items and join them as a comma-separated string."""
    try:
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".css-138u0vj"))
        )
        items = container.find_elements(By.CSS_SELECTOR, ".chakra-text.css-1jqr5c8")
        amenities = []
        for item in items:
            text = item.text.strip() or driver.execute_script("return arguments[0].innerText;", item).strip()
            text = clean_duplicated_text(text)
            if text:
                amenities.append(text)
        return ", ".join(amenities) if amenities else None
    except TimeoutException:
        return None


def scrape_additional_fields(driver: webdriver.Chrome) -> Optional[Dict[str, Any]]:
    """
    Collect all label/value pairs from the property details section that are
    not already mapped to top-level schema fields. Stored as additionalFields.
    """
    # Fields already captured at the top level — skip them
    SKIP_LABELS = {"boarea", "antal rum", "varav sovrum", "byggår", "avgift", "boendetyp"}

    fields: Dict[str, Any] = {}
    try:
        labels = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".chakra-text.css-xqs6kp"))
        )
        for label in labels:
            label_text = label.text.strip() or driver.execute_script("return arguments[0].innerText;", label).strip()
            label_text = clean_duplicated_text(label_text).strip()
            if label_text.lower() in SKIP_LABELS:
                continue
            try:
                parent = label.find_element(By.XPATH, "..")
                value_el = parent.find_element(By.CSS_SELECTOR, ".chakra-text.css-wguytq")
                value = value_el.text.strip() or driver.execute_script("return arguments[0].innerText;", value_el).strip()
                value = clean_duplicated_text(value).strip()
                if label_text and value:
                    fields[label_text] = value
            except NoSuchElementException:
                continue
    except TimeoutException:
        pass

    return fields if fields else None


def scrape_listing(driver: webdriver.Chrome, url: str, ad_type: str, cookie_accepted: bool = False) -> Optional[Dict[str, Any]]:
    """
    Visit a single listing page and extract all available fields.

    Args:
        driver:  Selenium WebDriver instance
        url:     Full URL of the listing page
        ad_type: "rent" or "sold"

    Returns:
        Dict matching the PropertyAd schema, or None on failure
    """
    try:
        driver.get(url)
        time.sleep(2)

        if not cookie_accepted:
            handle_cookie_banner(driver, WebDriverWait(driver, 15))
        handle_sf_popup(driver)

        title       = scrape_title(driver)
        description = scrape_description(driver)
        amenities = scrape_amenities(driver)
        address     = scrape_address(driver)
        age         = scrape_age(driver)
        beds, baths = scrape_beds_and_baths(driver)
        price, raw_per = scrape_price(driver)
        raw_type = scrape_type(driver)
        seller_name, seller_number = scrape_seller(driver)
        size = scrape_size(driver)
        images = scrape_images(driver)

        # Collect all extra fields from the page for additionalFields
        additional_fields = scrape_additional_fields(driver)

        return {
            "category": "real estate",
            "postAdData": {
                "title":            title,
                "description":      description,
                "adType":           ad_type,
                "additionalFields": additional_fields if additional_fields else None,
                "address":          address,
                "age":              age,
                "amenities":        amenities,
                "baths":            baths,
                "beds":             beds,
                "city":             address.split(",")[0].strip() if address and "," in address else None,
                "completionStatus": None,
                "country":          "Sweden",
                "furnishing":       None,
                "images":           images,
                "per":              map_per(raw_per),
                "price":            price,
                "sellerName":       seller_name,
                "sellerNumber":     seller_number,
                "size":             size,
                "sizeType":         "m²",
                "state":            address.split(",")[-1].strip() if address and "," in address else None,
                "type":             map_type(raw_type),
            },
        }

    except Exception as e:
        print(f"    ✗ Error scraping listing {url}: {e}")
        return None




def strip_nulls(obj: Any) -> Any:
    """Recursively remove keys with None or empty string values."""
    if isinstance(obj, dict):
        return {k: strip_nulls(v) for k, v in obj.items() if v is not None and v != ""}
    if isinstance(obj, list):
        return [strip_nulls(i) for i in obj]
    return obj


def scrape_all_listings(
    broker_results: Dict[str, Dict[str, List[str]]]
) -> int:
    """
    Iterate all brokers and their rent/sold listing URLs, scrape each one
    and insert directly into MongoDB. Returns total inserted count.
    """
    collection = mongo_client.db["listings"]
    driver = get_driver()
    cookie_accepted = False
    inserted = 0
    failed = 0

    try:
        for business_id, sections in broker_results.items():
            print(f"\n── Broker: {business_id} ──")

            for ad_type in ("rent", "sold"):
                urls = sections.get(ad_type, [])
                print(f"  [{ad_type.upper()}] {len(urls)} listing(s)")

                for i, url in enumerate(urls):
                    print(f"    [{i + 1}/{len(urls)}] {url}")
                    listing = scrape_listing(driver, url, ad_type, cookie_accepted)
                    if not listing:
                        failed += 1
                        continue

                    cookie_accepted = True

                    post = listing.get("postAdData", {})
                    source_url = f"{post.get('title', '')}_{post.get('address', '')}"
                    listing["source_url"] = source_url
                    listing["source_site"] = "erikolsson"
                    listing["scraped_at"] = datetime.utcnow()
                    listing = strip_nulls(listing)

                    try:
                        result = collection.update_one(
                            {"source_url": source_url},
                            {"$set": listing},
                            upsert=True
                        )
                        if result.upserted_id or result.modified_count:
                            inserted += 1
                            print(f"    ✓ Saved to DB: {source_url}")
                        else:
                            print(f"    ~ Already exists: {source_url}")
                    except Exception as e:
                        print(f"    ✗ DB insert failed: {e}")
                        failed += 1
    finally:
        driver.quit()

    return inserted


# ------------------------- Entry Point -------------------------
if __name__ == "__main__":
    if not mongo_client.connect():
        print("✗ Failed to connect to MongoDB")
        sys.exit(1)

    try:
        total = scrape_all_listings(BROKER_RESULTS)
        print(f"\n✓ Done — {total} listings saved to MongoDB")
    finally:
        mongo_client.close()