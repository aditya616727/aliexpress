"""
Scrape apartment listing URLs from each Erik Olsson broker's profile page.
"""

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException


# Load brokers from JSON file in project root
BROKERS_FILE = Path.cwd() / "config/erikolsson_brokers.json"
print(f"Loading brokers from {BROKERS_FILE}...")

with open(BROKERS_FILE, "r", encoding="utf-8") as f:
    BROKERS: List[Dict[str, Any]] = json.load(f)


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
    except Exception:
        pass  # Popup didn't appear or session issue, that's fine


def get_apartment_links(driver: webdriver.Chrome, section_label: str = "") -> List[str]:
    """
    Extract all apartment listing URLs from the listings container.
    Returns a deduplicated list of URLs.
    """
    links = []
    try:
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".css-z7gdp0"))
        )
        anchor_tags = container.find_elements(By.TAG_NAME, "a")
        for a in anchor_tags:
            href = a.get_attribute("href")
            if href:
                links.append(href)
    except TimeoutException:
        print(f"  ⚠ Listings container not found for {section_label}")

    # Deduplicate while preserving order
    seen = set()
    unique_links = []
    for link in links:
        if link not in seen:
            seen.add(link)
            unique_links.append(link)

    return unique_links

def click_second_tab(driver: webdriver.Chrome) -> bool:
    """
    Click the second button in the tabs container.
    Returns True if successful, False otherwise.
    """
    try:
        tab_list = WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".chakra-tabs__tablist.css-1xhq01z")
            )
        )
        tabs = tab_list.find_elements(By.TAG_NAME, "button")
        if len(tabs) >= 2:
            # Scroll the tab into view first, then JS click to bypass any overlay
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tabs[1])
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", tabs[1])
            time.sleep(1.5)
            print("  ✓ Clicked second tab")
            return True
        else:
            print("  ⚠ Less than 2 tabs found")
            return False
    except TimeoutException:
        print("  ⚠ Tab list not found")
        return False


def scrape_broker_listings(brokers: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """
    Visit each broker's page and collect apartment listing URLs.

    Returns:
        A dict mapping broker source_url -> list of apartment URLs
        (we no longer rely on business_id)
    """
    results: Dict[str, List[str]] = {}
    driver = get_driver()
    wait = WebDriverWait(driver, 15)
    cookie_accepted = False

    try:
        for i, broker in enumerate(brokers):
            name = broker["name"]
            url = broker.get("source_url") or broker.get("url") or ""
            broker_key = url or name

            print(f"\n[{i + 1}/{len(brokers)}] {name}")
            print(f"  URL: {url}")

            # Restart driver if session is dead
            try:
                _ = driver.current_url
            except Exception:
                print("  ⚠ Driver session lost, restarting...")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = get_driver()
                wait = WebDriverWait(driver, 15)
                cookie_accepted = False

            try:
                driver.get(url)
                time.sleep(2)

                if not cookie_accepted:
                    handle_cookie_banner(driver, wait)
                    cookie_accepted = True

                handle_sf_popup(driver)

                # Scrape initial listings → rent
                rent_links = get_apartment_links(driver, section_label="rent")
                print(f"  ✓ Rent listings : {len(rent_links)}")

                # Click second tab and scrape → sold
                sold_links = []
                if click_second_tab(driver):
                    sold_links = get_apartment_links(driver, section_label="sold")
                    print(f"  ✓ Sold listings : {len(sold_links)}")

                results[broker_key] = {
                    "rent": rent_links,
                    "sold": sold_links,
                }
            except Exception as e:
                print(f"  ✗ Failed to scrape {name}: {e}")
                results[broker_key] = {"rent": [], "sold": []}

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return results

# ---------------------------- Scarape listings -----------------------------

def main():
    print(f"Starting broker listing scraper for {len(BROKERS)} brokers...\n")
    results = scrape_broker_listings(BROKERS)

    # Save to JSON so the listing scraper can pick it up
    output_path = Path.cwd() / "config/broker_listing_urls.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"✓ Saved listing URLs to {output_path}")


if __name__ == "__main__":
    main()