#!/usr/bin/env python3
"""Scrape Bytbil showroom dealer details and save to CSV or MongoDB.

This script scrapes dealer detail pages from https://www.bytbil.com/handlare
and extracts company name, email, phone, and address.

By default it writes results to a CSV file in the repository root.
Use --mongo to also upsert results into the existing MongoDB broker collection.
"""

import csv
import json
import re
import sys
import time
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

try:
    from ad_extractor.database.mongodb import mongo_client
except ImportError:  # pragma: no cover
    mongo_client = None

BASE_URL = "https://www.bytbil.com"
LIST_URL = "https://www.bytbil.com/handlare"
PAGE_URL = "https://www.bytbil.com/handlare?page={}"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

DEALER_URL_PATTERN = re.compile(r"^/(?:handlare|handlare/)[^/]+-\d+$")

SKIP_URL_SUBSTRINGS = [
    "-lan",
    "auktoriserad-for",
    "kontakt",
    "om-bytbil",
    "logga-in",
    "handlarlogin",
    "advertisementinfo",
    "admin.bytbil.com",
    "facebook.com",
    "vend.com",
    "privacy",
    "anvandarvillkor",
    "personuppgiftshantering",
]

CSV_FIELDS = [
    "source_site",
    "source_url",
    "dealer_name",
    "email",
    "phone",
    "address",
    "scraped_at",
]


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def get_soup(session: requests.Session, url: str) -> BeautifulSoup:
    resp = session.get(url, timeout=20)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


def normalize_url(href: str) -> str:
    if href.startswith("//"):
        return f"https:{href}"
    if href.startswith("/"):
        return f"{BASE_URL}{href}"
    return href


def is_dealer_link(href: str, text: str) -> bool:
    if not href:
        return False
    if href.startswith(BASE_URL):
        href = href[len(BASE_URL) :]
    if any(skip in href for skip in SKIP_URL_SUBSTRINGS):
        return False
    if DEALER_URL_PATTERN.match(href):
        normalized_text = text.strip().lower()
        if not normalized_text or "upph" in normalized_text or "auktor" in normalized_text:
            return False
        return True
    return False


def extract_listing_links(soup: BeautifulSoup) -> List[str]:
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(" ", strip=True)
        if is_dealer_link(href, text):
            candidate = normalize_url(href)
            if candidate not in links:
                links.append(candidate)
    return links


def extract_jsonld_data(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string
        if not raw:
            continue
        try:
            data = json.loads(raw.strip())
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("@type") in {"Organization", "LocalBusiness", "AutoDealer", "AutoRental"}:
                    return item
        if isinstance(data, dict) and data.get("@type") in {"Organization", "LocalBusiness", "AutoDealer", "AutoRental"}:
            return data
    return None


def extract_address_from_text(full_text: str) -> Optional[str]:
    match = re.search(r"Adress[:\s]*([^\n\r\[]+)", full_text, re.IGNORECASE)
    if match:
        return re.sub(r"\s+", " ", match.group(1)).strip()
    return None


def extract_dealer_info(soup: BeautifulSoup, source_url: str) -> Dict[str, Optional[str]]:
    raw_text = " ".join(soup.stripped_strings)
    jsonld = extract_jsonld_data(soup)

    name = None
    if soup.h1:
        name = soup.h1.get_text(strip=True)
    if not name and jsonld:
        name = jsonld.get("name")
    if not name:
        name = ""

    phone = None
    email = None
    address = None

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("tel:") and not phone:
            phone = href.replace("tel:", "").strip()
        elif href.startswith("mailto:") and not email:
            email = href.replace("mailto:", "").strip()
        if phone and email:
            break

    if jsonld:
        if not email:
            email = jsonld.get("email")
        if not phone:
            phone = jsonld.get("telephone")
        address_data = jsonld.get("address")
        if address_data and isinstance(address_data, dict) and not address:
            parts = []
            for key in ("streetAddress", "postalCode", "addressLocality", "addressRegion", "addressCountry"):
                value = address_data.get(key)
                if value:
                    parts.append(str(value).strip())
            if parts:
                address = ", ".join(parts)

    if not address:
        address = extract_address_from_text(raw_text)

    if not address:
        # Try a fallback on a component that looks like street + city
        match = re.search(r"(\d{2,5} [A-Za-zÅÄÖåäö\s\-]+)", raw_text)
        if match:
            address = match.group(1).strip()

    return {
        "dealer_name": name or "",
        "email": email or "",
        "phone": phone or "",
        "address": address or "",
        "source_site": "bytbil",
        "source_url": source_url,
        "scraped_at": datetime.utcnow().isoformat(),
    }


def save_csv(rows: List[Dict[str, Optional[str]]], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})
    print(f"Saved {len(rows)} rows to {output_file}")


def upsert_to_mongo(rows: List[Dict[str, Optional[str]]]) -> int:
    if mongo_client is None:
        raise RuntimeError("MongoDB client import failed; run with the project dependencies installed.")

    if not mongo_client.connect():
        raise RuntimeError("Failed to connect to MongoDB. Check MONGODB_URI in environment.")

    collection = mongo_client.db["bytbil_dealers"]
    # Ensure a unique index on source_url to support upsert behaviour
    collection.create_index([("source_url", 1)], unique=True)

    upserted = 0
    for row in rows:
        now = datetime.utcnow()
        doc = {
            "source_site": row["source_site"],
            "source_url": row["source_url"],
            "name": row["dealer_name"],
            "email": row["email"],
            "phone": row["phone"],
            "address": row["address"],
        }
        result = collection.update_one(
            {"source_url": row["source_url"]},
            {
                "$set": {**doc, "updated_at": now},
                "$setOnInsert": {"created_at": now, "scraped_at": now},
            },
            upsert=True,
        )
        if result.upserted_id or result.modified_count:
            upserted += 1
    mongo_client.close()
    return upserted


def scrape_pages(max_pages: int, delay: float) -> List[str]:
    session = get_session()
    found_links: List[str] = []

    for page in range(1, max_pages + 1):
        url = LIST_URL if page == 1 else PAGE_URL.format(page)
        print(f"Loading dealer list page {page}: {url}")
        try:
            soup = get_soup(session, url)
        except Exception as exc:
            print(f"Failed to load page {page}: {exc}")
            break

        links = extract_listing_links(soup)
        if not links:
            print(f"No dealer links found on page {page}; stopping.")
            break

        new_links = [link for link in links if link not in found_links]
        if not new_links:
            print(f"No new dealer links found on page {page}; stopping.")
            break

        found_links.extend(new_links)
        print(f"Found {len(new_links)} new dealer links (total {len(found_links)})")
        time.sleep(delay)

    return found_links


def scrape_dealers(dealer_urls: List[str], delay: float) -> List[Dict[str, Optional[str]]]:
    session = get_session()
    rows: List[Dict[str, Optional[str]]] = []
    for index, url in enumerate(dealer_urls, start=1):
        print(f"Scraping dealer {index}/{len(dealer_urls)}: {url}")
        try:
            soup = get_soup(session, url)
            row = extract_dealer_info(soup, url)
            rows.append(row)
        except Exception as exc:
            print(f"  Failed to scrape {url}: {exc}")
        time.sleep(delay)
    return rows


def parse_args() -> ArgumentParser:
    parser = ArgumentParser(description="Scrape Bytbil dealer showroom details")
    parser.add_argument("--max-pages", type=int, default=10, help="Maximum listing pages to crawl")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests in seconds")
    parser.add_argument("--output-file", type=str, default="bytbil_dealers.csv", help="CSV file to write")
    parser.add_argument("--no-mongo", action="store_true", help="Skip upserting results into MongoDB")
    return parser


def main() -> None:
    parser = parse_args()
    args = parser.parse_args()

    print("Starting Bytbil dealer scraper")
    dealer_urls = scrape_pages(max_pages=args.max_pages, delay=args.delay)
    if not dealer_urls:
        print("No dealer URLs found. Exiting.")
        return

    rows = scrape_dealers(dealer_urls, delay=args.delay)
    if not rows:
        print("No dealer details extracted. Exiting.")
        return

    save_csv(rows, Path(args.output_file))

    if not args.no_mongo:
        count = upsert_to_mongo(rows)
        print(f"MongoDB upsert completed: {count} documents upserted into bytbil_dealers collection.")
    else:
        print("Skipping MongoDB upsert (--no-mongo flag set).")


if __name__ == "__main__":
    main()
