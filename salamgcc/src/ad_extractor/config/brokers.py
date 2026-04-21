"""Scraper for Erik Olsson brokers - https://www.erikolsson.se/brokers (Selenium)"""

import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import os
import requests
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ad_extractor.database.mongodb import mongo_client

# Project root = parent of src/ (so .env is found when CWD is not the repo root)
_project_root = Path(__file__).resolve().parents[3]
load_dotenv(_project_root / ".env")
load_dotenv()


BASE_URL = "https://www.erikolsson.se"
BROKERS_URL = f"{BASE_URL}/brokers"
OUTPUT_DIR = Path.cwd() / "config"
IMAGES_DIR = OUTPUT_DIR / "broker_images"
SOURCE_SITE = "erikolsson"
DEFAULT_COUNTRY = "Sweden"
DEFAULT_COUNTRY_CALLING_CODE = "+46"
DEFAULT_AGENCY_NAME = "Erik Olsson"


CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID")
CLOUDFLARE_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN")
CLOUDFLARE_IMAGES_URL = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/images/v1"


def upload_to_cloudflare(filepath: str) -> Optional[str]:
    """Upload a local image to Cloudflare Images. Returns the CDN URL or None."""
    if not filepath or not Path(filepath).exists():
        return None
    if not CLOUDFLARE_ACCOUNT_ID or not CLOUDFLARE_API_TOKEN:
        raise ValueError("CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN must be set in environment")

    try:
        with open(filepath, "rb") as f:
            response = requests.post(
                CLOUDFLARE_IMAGES_URL,
                headers={"Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}"},
                files={"file": (Path(filepath).name, f, "image/jpeg")},
                timeout=30,
            )
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            # Cloudflare returns a list of variants — grab the first one
            variants = data["result"]["variants"]
            return variants[0] if variants else None
        else:
            print(f"    Cloudflare upload failed: {data.get('errors')}")
            return None
    except Exception as e:
        print(f"    Error uploading to Cloudflare: {e}")
        return None


def slugify(text: str) -> str:
    """Convert text to uppercase underscore slug, handling Swedish characters."""
    replacements = {"å": "a", "ä": "a", "ö": "o", "Å": "A", "Ä": "A", "Ö": "O"}
    for k, v in replacements.items():
        text = text.replace(k, v)
    text = re.sub(r"[^a-zA-Z0-9\s]", "", text)
    return re.sub(r"\s+", "_", text.strip()).upper()


def normalize_phone(phone: Optional[str], country: str = DEFAULT_COUNTRY) -> Optional[int]:
    """
    Normalize phone to an integer.
    - Removes whitespace, '-', and '+' characters
    - Adds country code for Sweden when missing
    """
    if not phone:
        return None

    raw = str(phone).strip()
    if not raw:
        return None

    # Remove whitespace, hyphens, and plus sign
    compact = re.sub(r"[\s\-+]+", "", raw)

    # If it already starts with country code, return as int
    if compact.startswith("46") and len(compact) > 6:
        try:
            return int(compact)
        except ValueError:
            return None

    # Sweden-specific rules
    if country.lower() == "sweden":
        # 0702731861 -> 46702731861
        if compact.startswith("0"):
            compact = f"46{compact[1:]}"

    try:
        return int(compact)
    except ValueError:
        return None


def scrape_agency_from_profile(driver: webdriver.Chrome, profile_url: str) -> Dict[str, Any]:
    """
    Best-effort scrape of agency/office info from a broker profile page.
    The site markup changes often, so we prefer resilient heuristics.
    """
    agency: Dict[str, Any] = {
        "name": DEFAULT_AGENCY_NAME,
        "source_url": None,
        "phone": None,
        "email": None,
        "address": None,
        "country": DEFAULT_COUNTRY,
    }

    if not profile_url:
        return agency

    try:
        driver.get(profile_url)
        time.sleep(1.5)

        # Collect any link that looks like an office/agency page
        try:
            anchors = driver.find_elements(By.TAG_NAME, "a")
            office_links = []
            for a in anchors:
                href = (a.get_attribute("href") or "").strip()
                if not href:
                    continue
                if any(token in href.lower() for token in ("/kontor", "/office", "/offices")):
                    text = (a.text or "").strip()
                    office_links.append((href, text))
            if office_links:
                href, text = office_links[0]
                agency["source_url"] = href
                if text and len(text) >= 3:
                    agency["name"] = re.sub(r"\s+", " ", text)
        except Exception:
            pass

        # Pull contact details if present on the page (tel/mailto)
        try:
            tel_links = driver.find_elements(By.CSS_SELECTOR, "a[href^='tel:']")
            if tel_links:
                href = (tel_links[0].get_attribute("href") or "").replace("tel:", "").strip()
                agency["phone"] = normalize_phone(href, DEFAULT_COUNTRY)
        except Exception:
            pass

        try:
            mailto_links = driver.find_elements(By.CSS_SELECTOR, "a[href^='mailto:']")
            if mailto_links:
                href = (mailto_links[0].get_attribute("href") or "").strip()
                email = href.split("mailto:")[1].split("?")[0] if "mailto:" in href else href
                agency["email"] = (email or "").strip() or None
        except Exception:
            pass

        # Best-effort address: first element that looks like a street address block
        try:
            candidates = driver.find_elements(By.CSS_SELECTOR, "address, [itemprop='address']")
            if candidates:
                txt = re.sub(r"\s+", " ", (candidates[0].text or "").strip())
                agency["address"] = txt or None
        except Exception:
            pass

    except Exception as e:
        print(f"    Warning: agency scrape failed: {type(e).__name__}: {e}")

    return agency


def get_driver() -> webdriver.Chrome:
    """Initialize headless Chrome driver."""
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


def dismiss_overlays(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    """Accept cookie banner and close popup."""
    # Accept cookies
    try:
        btn = wait.until(EC.element_to_be_clickable((By.ID, "cc-b-acceptall")))
        btn.click()
        print("✓ Accepted cookies")
        time.sleep(1)
    except TimeoutException:
        print("  Cookie banner not found, skipping")

    # Close popup
    try:
        close_btn = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '[aria-label="Close popup"]'))
        )
        close_btn.click()
        print("✓ Closed popup")
        time.sleep(0.5)
    except TimeoutException:
        print("  Popup not found, skipping")


def scroll_to_load_all(driver: webdriver.Chrome, target_count: Optional[int] = None) -> None:
    """Scroll to bottom repeatedly to trigger lazy loading."""
    print("  Scrolling to load all brokers...")
    prev_count = 0
    for _ in range(100):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(3)
        cards = driver.find_elements(By.CSS_SELECTOR, ".chakra-stack.css-11nrrcx")
        count = len(cards)
        if target_count is not None and count >= target_count:
            prev_count = count
            break
        if count == prev_count:
            break
        prev_count = count
    print(f"  Found {prev_count} broker cards after scrolling")


def download_and_upload_image(img_url: str, filename: str) -> Optional[str]:
    """Download broker image locally, upload to Cloudflare, return CDN URL."""
    if not img_url:
        return None
    try:
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        filepath = IMAGES_DIR / filename
        resp = requests.get(img_url, timeout=10)
        if resp.ok:
            filepath.write_bytes(resp.content)
            # Upload to Cloudflare and return CDN URL
            cloudflare_url = upload_to_cloudflare(str(filepath))
            return cloudflare_url
    except Exception as e:
        print(f"    Warning: Could not download/upload image: {e}")
    return None


def clean_duplicated_text(text: str) -> str:
    """
    Nested <font> tags can cause Selenium .text to duplicate content.
    e.g. "070-273 18 61070-273 18 61" → "070-273 18 61"
    """
    half = len(text) // 2
    if half and text[:half] == text[half:]:
        return text[:half]
    return text


def extract_broker_card(card) -> Optional[Dict[str, Any]]:
    """Extract all broker data from a single card WebElement."""
    try:
        # --- Name & profile URL ---
        try:
            profile_link = card.find_element(By.CSS_SELECTOR, "a.chakra-link.css-4a6x12")
        except NoSuchElementException:
            return None
        href = profile_link.get_attribute("href") or ""
        profile_url = urljoin(BASE_URL, href) if href else ""

        name_el = profile_link.find_element(By.CSS_SELECTOR, "h2.chakra-heading")
        name = re.sub(r"\s+", " ", name_el.text.strip())

        # --- Location ---
        try:
            location_el = card.find_element(By.CSS_SELECTOR, ".chakra-text.css-7cbk0p")
            location = clean_duplicated_text(location_el.text.strip())
        except NoSuchElementException:
            location = ""

        # --- Phone & Email ---
        phone = ""
        email = ""
        try:
            contact_container = card.find_element(By.CSS_SELECTOR, ".chakra-stack.css-1qusn61")
            contact_links = contact_container.find_elements(By.CSS_SELECTOR, "a.chakra-link.css-spn4bz")

            for link in contact_links:
                href_contact = link.get_attribute("href") or ""
                try:
                    text_el = link.find_element(By.CSS_SELECTOR, ".chakra-text.css-13892uw")
                    raw_text = clean_duplicated_text(text_el.text.strip())
                except NoSuchElementException:
                    raw_text = ""

                if href_contact.startswith("tel:"):
                    phone = raw_text or href_contact.replace("tel:", "")
                elif href_contact.startswith("mailto:"):
                    email = raw_text or href_contact.split("mailto:")[1].split("?")[0]
        except NoSuchElementException:
            pass

        # --- Image ---
        display_pic = ""
        try:
            img_el = card.find_element(By.CSS_SELECTOR, "img.chakra-image")
            # Prefer srcset (higher quality), fall back to src
            srcset = img_el.get_attribute("srcset") or ""
            src = img_el.get_attribute("src") or ""
            img_url = srcset.split(" ")[0] if srcset else src

            if img_url:
                safe_name = re.sub(r"[^a-z0-9_]", "_", name.lower()) + ".jpg"
                display_pic = download_and_upload_image(img_url, safe_name) or img_url

        except NoSuchElementException:
            pass

        phone = normalize_phone(phone, DEFAULT_COUNTRY)

        return {
            "source_site": SOURCE_SITE,
            "source_url": profile_url,
            "name": name,
            "address": location or None,
            "country": DEFAULT_COUNTRY,
            "email": email,
            "phone": phone,
            "profileImage": display_pic or None,
            "agencyName": DEFAULT_AGENCY_NAME,
        }

    except Exception as e:
        # Avoid huge selenium stacktraces for transient DOM changes
        print(f"    Error extracting card: {type(e).__name__}: {e}")
        return None


def scrape_brokers(store_in_mongodb: bool = True, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Main scraping orchestrator. Optionally stores each broker one-by-one into MongoDB."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    driver = get_driver()
    wait = WebDriverWait(driver, 15)
    connected = False

    if store_in_mongodb:
        connected = mongo_client.connect()
        if not connected:
            print("✗ Failed to connect to MongoDB (brokers will only be saved to JSON)")

    try:
        print(f"Navigating to {BROKERS_URL} ...")
        driver.get(BROKERS_URL)
        time.sleep(2)

        dismiss_overlays(driver, wait)
        scroll_to_load_all(driver, target_count=limit)

        # Re-fetch cards after all scrolling is done
        cards = driver.find_elements(By.CSS_SELECTOR, ".chakra-stack.css-11nrrcx")
        total = len(cards)
        print(f"\nExtracting data from {total} broker cards...")

        brokers: List[Dict[str, Any]] = []
        for i, card in enumerate(cards):
            if limit is not None and len(brokers) >= limit:
                break
            broker = extract_broker_card(card)
            if broker:
                brokers.append(broker)
                if store_in_mongodb and connected:
                    mongo_client.upsert_broker(broker)
            if (i + 1) % 10 == 0:
                print(f"  Processed {i + 1}/{total}")

        # Enrich each broker with linked agency data (agent accounts are tied to agency accounts)
        print("\nEnriching brokers with agency data...")
        for i, broker in enumerate(brokers):
            agency = scrape_agency_from_profile(driver, broker.get("source_url", ""))
            broker["agency"] = agency
            broker["agencyName"] = agency.get("name") or broker.get("agencyName")
            if store_in_mongodb and connected:
                mongo_client.upsert_broker(broker)
            if (i + 1) % 10 == 0:
                print(f"  Enriched {i + 1}/{len(brokers)}")

    finally:
        driver.quit()
        if store_in_mongodb and connected:
            mongo_client.close()

    print(f"\n✓ Scraped {len(brokers)} brokers")
    return brokers


def main():
    brokers = scrape_brokers()

    json_path = OUTPUT_DIR / "erikolsson_brokers.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(brokers, f, ensure_ascii=False, indent=2)
    print(f"✓ Saved to {json_path}")

    if brokers:
        print("\nSample broker:")
        print(json.dumps(brokers[0], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()