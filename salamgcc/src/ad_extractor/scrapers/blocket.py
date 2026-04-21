"""Blocket.se scraper implementation."""

import re
import time
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from loguru import logger
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from ..translation.dictionaries import (
    SWEDISH_TO_ENGLISH_SPECS,
    VALUE_TRANSLATIONS,
    EQUIPMENT_TRANSLATIONS,
    FUEL_TYPE_MAP,
)
from ..utils.helpers import upload_images_to_cloudflare
from .base import BaseScraper

load_dotenv()


class BlocketScraper(BaseScraper):
    """Scraper specifically for Blocket.se."""

    def __init__(
        self,
        business_id: str,
        headless: bool = True,
        translate_to_english: bool = True,
    ) -> None:
        super().__init__(headless=headless)
        self.business_id = business_id
        self.translate_to_english = translate_to_english
        self.translator = None  # Optional: GoogleTranslator for fallback

        self.SELECTORS = {
            "listing_cards": "article.sf-search-ad",
            "listing_link": "a.sf-search-ad-link",
            "pagination_container": "w-pagination nav",
            "pagination_links": "w-pagination nav a",
            "next_page_button": 'w-pagination nav a[aria-label*="next"]',
            "title": "h1.t1",
            "subtitle": "p.s-text-subtle.mb-0.mt-8",
            "price": "span.t2",
            "description": "div.whitespace-pre-wrap",
            "image_gallery": 'ul[aria-label*="Image"]',
            "image_items": 'li img[id^="gallery-image-"]',
            "specifications_section": "section.key-info-section",
            "spec_items": "dl div",
            "equipment_section": "section:has(h2)",
            "equipment_items": "ul li",
        }

    def accept_cookies(self) -> bool:
        """Try to accept cookie popup if it appears."""
        try:
            time.sleep(1)
            strategies = [
                ("css selector", 'div[id*="sp_message_container"] iframe'),
                ("xpath", "/html/body/div[2]/div/div/div/div[3]/button[1]"),
                ("css selector", 'div[role="dialog"] button'),
                ("css selector", 'div[id*="message_container"] button'),
                ("css selector", 'button[class*="accept"]'),
                ("css selector", 'button[id*="accept"]'),
                ('xpath', '//button[contains(text(), "Acceptera")]'),
                ('xpath', '//button[contains(text(), "Accept")]'),
                ('xpath', '//button[contains(text(), "Godkänn")]'),
            ]

            for method, selector in strategies:
                try:
                    if "iframe" in selector:
                        iframes = self.driver.find_elements("css selector", selector)
                        for iframe in iframes:
                            try:
                                if iframe.is_displayed():
                                    self.driver.switch_to.frame(iframe)
                                    time.sleep(0.5)
                                    buttons = self.driver.find_elements("css selector", "button")
                                    for btn in buttons:
                                        text = btn.text.lower()
                                        if any(w in text for w in ["accept", "godkänn", "ok", "samtycke"]):
                                            btn.click()
                                            # logger.debug("Accepted cookies (iframe)")
                                            time.sleep(0.5)
                                            self.driver.switch_to.default_content()
                                            return True
                                    self.driver.switch_to.default_content()
                            except Exception:
                                self.driver.switch_to.default_content()
                                continue
                    else:
                        buttons = (
                            self.driver.find_elements("css selector", selector)
                            if method == "css selector"
                            else self.driver.find_elements("xpath", selector)
                        )
                        for button in buttons:
                            try:
                                if button.is_displayed() and button.is_enabled():
                                    self.driver.execute_script(
                                        "arguments[0].scrollIntoView(true);", button
                                    )
                                    time.sleep(0.2)
                                    button.click()
                                    # logger.debug("Accepted cookies")
                                    time.sleep(0.5)
                                    return True
                            except Exception:
                                continue
                except Exception:
                    continue
            return False
        except Exception as e:
            # logger.debug(f"Cookie handling error: {e}")
            return False

    def get_listing_urls(self, dealer_url: str) -> List[str]:
        """Extract all listing URLs from dealer search page. Paginates until no more pages."""
        all_urls: List[str] = []
        page_num = 1

        try:
            # logger.debug(f"Loading first page for cookie acceptance...")
            self.driver.get(dealer_url)
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, self.SELECTORS["listing_cards"])))
            self.accept_cookies()
            time.sleep(0.5)

            while True:
                try:
                    page_url = f"{dealer_url}&page={page_num}" if "?" in dealer_url else f"{dealer_url}?page={page_num}"
                    logger.info(f"Scraping page {page_num}: {page_url}")

                    self.driver.get(page_url)
                    self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, self.SELECTORS["listing_cards"])))
                    self.scroll_page(times=2)
                    time.sleep(0.5)

                    soup = BeautifulSoup(self.driver.page_source, "lxml")
                    listing_cards = soup.select(self.SELECTORS["listing_cards"])
                    logger.info(f"Found {len(listing_cards)} listing cards on page {page_num}")

                    if len(listing_cards) == 0:
                        break

                    page_urls = []
                    for card in listing_cards:
                        try:
                            link = card.select_one(self.SELECTORS["listing_link"])
                            if link and link.get("href"):
                                url = link["href"]
                                if not url.startswith("http"):
                                    url = f"https://www.blocket.se{url}"
                                page_urls.append(url)
                        except Exception:
                            continue

                    all_urls.extend(page_urls)
                    if len(listing_cards) < 50:
                        break
                    page_num += 1
                except Exception as e:
                    logger.error(f"Error on page {page_num}: {e}")
                    break

            unique_urls = list(set(all_urls))
            logger.success(f"Extracted {len(unique_urls)} unique listing URLs")
            return unique_urls
        except Exception as e:
            logger.error(f"Failed to get listing URLs: {e}")
            return list(set(all_urls))

    def check_driver_alive(self) -> bool:
        """Check if driver session is still alive, reinitialize if needed."""
        try:
            _ = self.driver.current_url
            return True
        except Exception:
            logger.warning("Driver session lost, reinitializing...")
            try:
                self.close()
            except Exception:
                pass
            self.init_driver()
            return True

    def scrape_listing(self, listing_url: str) -> Optional[Dict[str, Any]]:
        """Scrape single listing detail page and map to CarAd schema."""
        try:
            self.check_driver_alive()
            logger.debug(f"Loading: {listing_url}")
            self.driver.get(listing_url)
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, self.SELECTORS["title"])))
            self.accept_cookies()
            time.sleep(0.3)

            soup = BeautifulSoup(self.driver.page_source, "lxml")

            raw_title = self._extract_title(soup)
            raw_subtitle = self._extract_subtitle(soup)
            raw_price = self._extract_price(soup)
            raw_description = self._extract_description(soup)
            raw_seller_name = self._extract_seller_name(soup)
            raw_images = self._extract_images(soup)
            raw_specifications = self._extract_specifications(soup)
            raw_equipment = self._extract_equipment(soup)

            data = self._map_to_carad_schema(
                title=raw_title,
                subtitle=raw_subtitle,
                price=raw_price,
                description=raw_description,
                seller_name=raw_seller_name,
                images=raw_images,
                specifications=raw_specifications,
                equipment=raw_equipment,
                source_url=listing_url,
            )

            logger.debug(f"Extracted: {data.get('postAdData', {}).get('title')} - {data.get('postAdData', {}).get('price')}")
            return data
        except Exception as e:
            msg = getattr(e, "msg", None) or str(e) or repr(e)
            # Selenium may have "Message:\n\nStacktrace" – extract first line
            if isinstance(msg, str) and "\n" in msg:
                msg = msg.split("\n")[0].strip() or msg[:200]
            if not msg or msg == "Message:":
                msg = "browser session crashed or invalid (try reducing NUM_SCRAPER_WORKERS or USE_PROXY=false)"
            logger.error(f"Failed to scrape listing {listing_url}: {type(e).__name__}: {msg}")
            raise

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        elem = soup.select_one(self.SELECTORS["title"])
        return elem.get_text(strip=True) if elem else None

    def _extract_subtitle(self, soup: BeautifulSoup) -> Optional[str]:
        elem = soup.select_one(self.SELECTORS["subtitle"])
        return elem.get_text(strip=True) if elem else None

    def _extract_price(self, soup: BeautifulSoup) -> Optional[str]:
        elem = soup.select_one(self.SELECTORS["price"])
        return elem.get_text(strip=True) if elem else None

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        elem = soup.select_one(self.SELECTORS["description"])
        return elem.get_text(strip=True) if elem else None

    def _extract_seller_name(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            seller_elem = soup.find(string=re.compile(r"Företag|Company"))
            if seller_elem:
                parent = seller_elem.find_parent()
                if parent and parent.find_previous_sibling():
                    return parent.find_previous_sibling().get_text(strip=True)
        except Exception:
            pass
        return None

    def _extract_images(self, soup: BeautifulSoup) -> List[str]:
        raw_urls = []
        for img in soup.select(self.SELECTORS["image_items"]):
            try:
                srcset = img.get("srcset", "")
                if srcset:
                    urls = srcset.split(",")
                    for url_part in urls:
                        if "1600w" in url_part:
                            raw_urls.append(url_part.strip().split(" ")[0])
                            break
                    else:
                        raw_urls.append(urls[0].strip().split(" ")[0])
            except Exception:
                continue
        return upload_images_to_cloudflare(raw_urls)

    def _extract_specifications(self, soup: BeautifulSoup) -> Dict[str, str]:
        specs = {}
        try:
            for selector in [
                self.SELECTORS["specifications_section"],
                "section[class*='key-info']",
            ]:
                spec_section = soup.select_one(selector)
                if spec_section:
                    dts = spec_section.find_all("dt")
                    dds = spec_section.find_all("dd")
                    for dt, dd in zip(dts, dds):
                        key = dt.get_text(strip=True)
                        value = dd.get_text(strip=True)
                        if key and value:
                            specs[key] = value
                    if specs:
                        break
            if not specs:
                dl = soup.find("dl")
                if dl:
                    dts, dds = dl.find_all("dt"), dl.find_all("dd")
                    for dt, dd in zip(dts, dds):
                        key = dt.get_text(strip=True)
                        value = dd.get_text(strip=True)
                        if key and value:
                            specs[key] = value
            specs = self._translate_specifications(specs)
        except Exception as e:
            logger.error(f"Failed to extract specifications: {e}")
        return specs

    def _translate_specifications(self, specs: Dict[str, str]) -> Dict[str, str]:
        translated = {}
        for swedish_key, value in specs.items():
            english_key = SWEDISH_TO_ENGLISH_SPECS.get(swedish_key, swedish_key)
            translated_value = VALUE_TRANSLATIONS.get(value, value)
            translated[english_key] = translated_value
        return translated

    def _extract_equipment(self, soup: BeautifulSoup) -> List[str]:
        equipment = []
        try:
            heading = soup.find("h2", string=re.compile(r"Equipment|Utrustning|equipment|utrustning"))
            if heading:
                ul = heading.find_next("ul")
                if ul:
                    equipment = [
                        li.get_text(strip=True)
                        for li in ul.find_all("li")
                        if li.get_text(strip=True)
                    ]
        except Exception as e:
            logger.error(f"Failed to extract equipment: {e}")
        return equipment

    @staticmethod
    def _strip_spaces_in_numbers(text: Optional[str]) -> Optional[str]:
        """Remove spaces between digits, e.g. '1 200 kg' -> '1200 kg'."""
        if not text or not isinstance(text, str):
            return text
        return re.sub(r"(?<=\d)\s+(?=\d)", "", text)

    @staticmethod
    def _extract_number(text: Optional[str]) -> Optional[int]:
        if not text:
            return None
        numbers = re.findall(r"\d+", str(text).replace(" ", ""))
        return int("".join(numbers)) if numbers else None

    @staticmethod
    def _translate_drive_terrain(drive_wheels: Optional[str]) -> Optional[str]:
        """Map to 4WD, 2WD, FWD, RWD only."""
        if not drive_wheels or not isinstance(drive_wheels, str):
            return None
        s = drive_wheels.strip()
        if s.upper() in ("4WD", "AWD"):
            return "4WD"
        if s.upper() == "2WD":
            return "2WD"
        if s.upper() == "FWD":
            return "FWD"
        if s.upper() == "RWD":
            return "RWD"
        lower = s.lower()
        if "fyr" in lower or "four" in lower or "4wd" in lower or "awd" in lower:
            return "4WD"
        if "två" in lower or "two" in lower or "2wd" in lower:
            return "2WD"
        if "fram" in lower or "front" in lower or "fwd" in lower:
            return "FWD"
        if "bak" in lower or "rear" in lower or "rwd" in lower:
            return "RWD"
        return None

    @staticmethod
    def _get_engine_capacity_range(engine_size_text: Optional[str]) -> Optional[str]:
        if not engine_size_text or not isinstance(engine_size_text, str):
            return None
        s = str(engine_size_text).strip()
        if not re.search(r"\d", s):
            return None
        match = re.search(r"(\d+\.?\d*)", s)
        if not match:
            return None
        cc = int(float(match.group(1)) * 1000)
        ranges = [
            (1000, "0 - 999 cc"), (1500, "1000 - 1499 cc"), (2000, "1500 - 1999 cc"),
            (2500, "2000 - 2499 cc"), (3000, "2500 - 2999 cc"), (3500, "3000 - 3499 cc"),
            (4000, "3500 - 3999 cc"),
        ]
        for limit, label in ranges:
            if cc < limit:
                return label
        return "4000+ cc"

    @staticmethod
    def _get_horsepower_range(hp_num: Optional[int]) -> Optional[str]:
        if hp_num is None:
            return None
        try:
            hp = int(hp_num)
            ranges = [
                (100, "0 - 99 HP"), (200, "100 - 199 HP"), (300, "200 - 299 HP"),
                (400, "300 - 399 HP"), (500, "400 - 499 HP"), (600, "500 - 599 HP"),
                (700, "600 - 699 HP"), (800, "700 - 799 HP"), (900, "800 - 899 HP"),
            ]
            for limit, label in ranges:
                if hp < limit:
                    return label
            return "900+ HP"
        except Exception:
            return None

    @staticmethod
    def _normalize_fuel_type(fuel_type: Optional[str]) -> Optional[str]:
        """Map fuel type to one of: Diesel, Electric, Petrol, Hybrid, Other."""
        if not fuel_type or not isinstance(fuel_type, str):
            return None
        normalized = fuel_type.strip()
        result = FUEL_TYPE_MAP.get(normalized)
        if result:
            return result
        lower = normalized.lower()
        for key, value in sorted(FUEL_TYPE_MAP.items(), key=lambda x: -len(x[0])):
            if key.lower() == lower or key.lower() in lower:
                return value
        return "Other"

    @staticmethod
    def _format_transmission_type(transmission: Optional[str]) -> Optional[str]:
        if not transmission:
            return None
        if "transmission" in transmission.lower():
            return transmission
        return f"{transmission} Transmission"

    def _translate_equipment(self, equipment_list: List[str]) -> List[str]:
        if not self.translate_to_english or not equipment_list:
            return equipment_list
        return [EQUIPMENT_TRANSLATIONS.get(item, item) for item in equipment_list]

    def _map_to_carad_schema(
        self,
        title: Optional[str],
        subtitle: Optional[str],
        price: Optional[str],
        description: Optional[str],
        seller_name: Optional[str],
        images: List[str],
        specifications: Dict[str, str],
        equipment: List[str],
        source_url: str,
    ) -> Dict[str, Any]:
        price_num = self._extract_number(price) or 0
        year_text = specifications.get("Model Year", "0")
        year = self._extract_number(year_text) or 0
        kilometers_text = specifications.get("Mileage", "0") or ""
        kilometers_num = self._extract_number(kilometers_text)
        if kilometers_num is not None:
            if "mil" in str(kilometers_text).lower():
                kilometers = kilometers_num * 10
            else:
                kilometers = kilometers_num
        else:
            kilometers = None
        horsepower_text = specifications.get("Power", "")
        horsepower_num = self._extract_number(horsepower_text)
        horsepower = self._get_horsepower_range(horsepower_num)
        battery_capacity = self._extract_number(specifications.get("Battery Capacity", ""))
        battery_health = self._extract_number(specifications.get("Battery Health", ""))
        electric_range = self._extract_number(specifications.get("Electric Range (WLTP)", ""))
        tyres_condition = self._extract_number(specifications.get("Tyres Condition", ""))

        translated_equipment = self._translate_equipment(equipment)
        extras = ", ".join(translated_equipment) if translated_equipment else None

        auto_pilot = None
        autopilot_type = None
        if equipment:
            for item in equipment:
                if "autopilot" in item.lower():
                    auto_pilot = "Yes"
                    autopilot_type = item
                    break

        post_ad_data = {
            "title": title or "",
            "description": description or "",
            "price": price_num,
            "brand": specifications.get("Brand", ""),
            "model": specifications.get("Model", ""),
            "year": year,
            "images": images or [],
            "bodyType": specifications.get("Car Type"),
            "fuelType": self._normalize_fuel_type(specifications.get("Fuel Type")),
            "transmissionType": self._format_transmission_type(specifications.get("Transmission")),
            "horsepower": horsepower,
            "engineCapacityCc": self._get_engine_capacity_range(specifications.get("Engine Size")),
            "kilometers": kilometers,
            "seatingCapacity": specifications.get("Seats"),
            "doors": specifications.get("Doors"),
            "numberOfCylinders": specifications.get("Number of Cylinders"),
            "exteriorColor": specifications.get("Color"),
            "interiorColor": specifications.get("Interior Color"),
            "trim": specifications.get("Trim"),
            "batteryCapacity": battery_capacity,
            "batteryHealth": battery_health,
            "electricRange": electric_range,
            "range": specifications.get("Range"),
            "chargingTime": specifications.get("Charging Time"),
            "registrationNumber": specifications.get("Registration Number"),
            "chassisNumber": specifications.get("Chassis Number (VIN)"),
            "engineNumber": specifications.get("Engine Number"),
            "interiorCondition": specifications.get("Interior Condition"),
            "exteriorCondition": specifications.get("Exterior Condition"),
            "tyresConditionPercent": tyres_condition,
            "warranty": specifications.get("Warranty"),
            "fuelConsumption": specifications.get("Fuel Consumption"),
            "driveTerrain": self._translate_drive_terrain(specifications.get("Drive Wheels")),
            "city": None,
            "address": None,
            "country": "Sweden",
            "sellerAssists": seller_name is not None,
            "extras": extras,
            "autoPilot": auto_pilot,
            "autopilotType": autopilot_type,
            "per": None,
            "regionalSpecs": "European Specs",
            "targetMarket": None,
            "steeringSide": specifications.get("Steering Side"),
            "additionalFields": {
                "vehicleClass": specifications.get("Vehicle Class"),
                "registrationDate": specifications.get("Registration Date"),
                "maxTrailerWeight": self._strip_spaces_in_numbers(specifications.get("Max Trailer Weight")),
            },
        }

        additional_fields = {
            k: v for k, v in post_ad_data["additionalFields"].items()
            if v is not None and v != ""
        }

        cleaned_post_ad_data = {}
        for key, value in post_ad_data.items():
            if key == "additionalFields":
                if additional_fields:
                    cleaned_post_ad_data[key] = additional_fields
            elif value is None or value == "":
                continue
            else:
                cleaned_post_ad_data[key] = value

        result = {
            "business_id": self.business_id,
            "source_url": source_url,
            "source_site": "blocket",
            "isPosted": False,
            "postAdData": cleaned_post_ad_data,
        }
        if subtitle is not None and subtitle != "":
            result["subtitle"] = subtitle
        return result
