#!/usr/bin/env python3
"""Quick test script for BlocketScraper - scrapes a few listings and prints results."""

import json
import os
import sys

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _project_root)
sys.path.insert(0, os.path.join(_project_root, "src"))

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

from ad_extractor.scrapers.blocket import BlocketScraper

# --- Config ---
TEST_DEALER_URL = "https://www.blocket.se/mobility/search/car?orgId=188150"  # Riddermark Järfälla
TEST_BUSINESS_ID = "RIDDERMARK_JARFALLA"
MAX_LISTINGS = 3      # how many individual listings to scrape
HEADLESS = True       # set False to watch the browser
# --------------

def main():
    logger.info(f"Starting Blocket test | dealer: {TEST_BUSINESS_ID} | max listings: {MAX_LISTINGS}")

    with BlocketScraper(business_id=TEST_BUSINESS_ID, headless=HEADLESS) as scraper:
        scraper.init_driver()

        # Step 1: get listing URLs
        logger.info("Step 1: fetching listing URLs...")
        urls = scraper.get_listing_urls(TEST_DEALER_URL)
        logger.success(f"Found {len(urls)} listing URLs")

        if not urls:
            logger.error("No URLs found - check selector or site structure")
            return

        # Step 2: scrape individual listings
        logger.info(f"Step 2: scraping first {MAX_LISTINGS} listings...")
        results = []
        for i, url in enumerate(urls[:MAX_LISTINGS]):
            logger.info(f"[{i+1}/{MAX_LISTINGS}] {url}")
            try:
                data = scraper.scrape_listing(url)
                results.append(data)
                title = data.get("postAdData", {}).get("title", "N/A")
                price = data.get("postAdData", {}).get("price", "N/A")
                images = data.get("postAdData", {}).get("images", [])
                logger.success(f"  title: {title} | price: {price} | images: {len(images)}")
            except Exception as e:
                logger.error(f"  Failed: {e}")

    # Step 3: dump results to JSON
    out_path = os.path.join(_project_root, "scripts", "test_blocket_output.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.success(f"Results saved to {out_path}")


if __name__ == "__main__":
    main()
