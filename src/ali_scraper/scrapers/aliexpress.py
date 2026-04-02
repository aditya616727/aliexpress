"""AliExpress product scraper using Playwright."""

import json
import re
import time
import random
import logging
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

from .base import BaseScraper
from ..config import settings

logger = logging.getLogger(__name__)

EMPTY_PRODUCT = {
    "title": "",
    "price": "",
    "original_price": "",
    "discount": "",
    "rating": "",
    "reviews_count": "",
    "orders_count": "",
    "store_name": "",
    "product_url": "",
    "image_url": "",
    "image_path": "",
}


class AliExpressScraper(BaseScraper):
    """Scrapes product listings from AliExpress search results using Playwright."""

    _CHALLENGE_MARKERS = ["_____tmd_____", "punish", "x5secdata", "baxia-dialog", "captcha"]

    def _is_challenge_page(self, page):
        """Detect if AliExpress served an anti-bot challenge instead of results."""
        url = page.url
        if "_____tmd_____" in url or "punish" in url:
            logger.warning(f"Challenge detected in URL: {url[:120]}")
            return True
        html = page.content()
        for marker in self._CHALLENGE_MARKERS:
            if marker in html:
                logger.warning(f"Challenge marker '{marker}' found in page HTML")
                return True
        return False

    def _warm_up(self, context):
        """Visit the AliExpress homepage first to establish cookies/session."""
        page = context.new_page()
        try:
            logger.info("Warming up: visiting AliExpress homepage...")
            page.goto("https://www.aliexpress.com", wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(random.randint(2000, 4000))
            # Dismiss any popups
            for sel in ["button[class*='close']", "[class*='popup'] button", ".next-dialog-close"]:
                try:
                    page.click(sel, timeout=1000)
                except Exception:
                    pass
            page.wait_for_timeout(1000)
        except Exception as e:
            logger.debug(f"Warm-up navigation failed (non-fatal): {e}")
        finally:
            page.close()

    def _build_search_url(self, query, page=1):
        encoded_query = quote_plus(query)
        url = settings.search_url.format(query=encoded_query)
        if page > 1:
            url += f"?page={page}&SearchText={encoded_query}"
        return url

    def _fetch_page(self, url, retries=3):
        """Fetch a page using Playwright browser with stealth and retry.

        Detects challenge/captcha pages and retries with a fresh context.
        """
        for attempt in range(1, retries + 1):
            context = self._create_context()

            try:
                # Warm up on first attempt to establish cookies
                if attempt == 1:
                    self._warm_up(context)

                page = context.new_page()
                logger.info(f"Navigating to: {url} (attempt {attempt}/{retries})")

                try:
                    page.goto(url, wait_until="networkidle", timeout=60000)
                except Exception:
                    logger.info("networkidle timed out, falling back to domcontentloaded")
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)

                # Check for challenge page early
                if self._is_challenge_page(page):
                    logger.warning(f"Challenge page on attempt {attempt}/{retries}")
                    context.close()
                    if attempt < retries:
                        # Close browser entirely to get fresh fingerprint
                        self._close_browser()
                        backoff = 10 * attempt + random.uniform(3, 8)
                        logger.info(f"Restarting browser, retrying in {backoff:.1f}s...")
                        time.sleep(backoff)
                        continue
                    else:
                        logger.error("All attempts hit challenge page")
                        return None

                # Wait for JS to render product cards
                page.wait_for_timeout(random.randint(5000, 8000))

                # Wait for product cards to appear (try multiple selectors)
                selectors_to_try = [
                    "div[class*='search-item-card-wrapper-gallery']",
                    "a.search-card-item",
                    "a[href*='/item/']",
                    "div[class*='gallery']",
                ]

                loaded = False
                for sel in selectors_to_try:
                    try:
                        page.wait_for_selector(sel, timeout=5000)
                        loaded = True
                        logger.info(f"Products loaded with selector: {sel}")
                        break
                    except Exception:
                        continue

                if not loaded:
                    # Check again — maybe it turned into a challenge after JS ran
                    if self._is_challenge_page(page):
                        logger.warning(f"Late challenge detected on attempt {attempt}/{retries}")
                        context.close()
                        if attempt < retries:
                            self._close_browser()
                            backoff = 10 * attempt + random.uniform(3, 8)
                            logger.info(f"Restarting browser, retrying in {backoff:.1f}s...")
                            time.sleep(backoff)
                            continue
                        else:
                            logger.error("All attempts hit challenge page")
                            return None
                    logger.info("No product selector matched, waiting extra time for JS...")
                    page.wait_for_timeout(5000)

                # Scroll down to trigger lazy loading of more products
                for _ in range(4):
                    page.evaluate("window.scrollBy(0, window.innerHeight)")
                    page.wait_for_timeout(random.randint(1500, 2500))

                html = page.content()
                return html

            except Exception as e:
                logger.error(f"Attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    self._close_browser()
                    backoff = 5 * attempt + random.uniform(1, 5)
                    logger.info(f"Retrying in {backoff:.1f}s...")
                    time.sleep(backoff)
                else:
                    logger.error(f"All {retries} attempts failed for: {url}")
                    return None
            finally:
                try:
                    context.close()
                except Exception:
                    pass

    def _extract_products_from_html(self, html):
        """Extract products from the rendered HTML using multiple strategies."""
        # Strategy 1: Extract from embedded JSON data in script tags
        products = self._extract_from_script_data(html)
        # Only trust script data if products have real content (price or image)
        valid = [p for p in products if p.get("price") or p.get("image_url")]
        if valid:
            return valid

        # Strategy 2: Parse rendered HTML DOM
        products = self._extract_from_html_structure(html)
        return products

    def _extract_from_script_data(self, html):
        """Extract product data from embedded JSON in script tags."""
        products = []
        try:
            patterns = [
                r'_init_data_\s*=\s*{\s*data:\s*({.+?})\s*}',
                r'"itemList":\s*(\[.+?\])',
                r'"items":\s*(\[.+?\])',
                r'window\.__INIT_DATA__\s*=\s*({.+?});',
                r'"resultList":\s*(\[.+?\])',
            ]

            for pattern in patterns:
                matches = re.findall(pattern, html, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match)
                        items = self._find_items_in_data(data)
                        for item in items:
                            product = self._parse_json_item(item)
                            if product and product.get("title"):
                                products.append(product)
                    except (json.JSONDecodeError, TypeError):
                        continue

            if not products:
                soup = BeautifulSoup(html, "lxml")
                scripts = soup.find_all("script")
                for script in scripts:
                    text = script.string or ""
                    if "itemList" in text or "productId" in text:
                        json_matches = re.findall(r'({[^{}]*"title"[^{}]*})', text)
                        for jm in json_matches:
                            try:
                                item = json.loads(jm)
                                product = self._parse_json_item(item)
                                if product and product.get("title"):
                                    products.append(product)
                            except (json.JSONDecodeError, TypeError):
                                continue

        except Exception as e:
            logger.debug(f"Script data extraction failed: {e}")

        return products

    def _find_items_in_data(self, data):
        """Recursively find item lists in nested data structures."""
        items = []
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and ("title" in item or "productTitle" in item):
                    items.append(item)
                else:
                    items.extend(self._find_items_in_data(item))
        elif isinstance(data, dict):
            if "title" in data and ("productId" in data or "price" in data or "image" in data):
                items.append(data)
            for value in data.values():
                if isinstance(value, (dict, list)):
                    items.extend(self._find_items_in_data(value))
        return items

    def _parse_json_item(self, item):
        """Parse a JSON item dict into a standardized product dict."""
        product = dict(EMPTY_PRODUCT)

        # Title
        for key in ["title", "productTitle", "name", "subject"]:
            if key in item and item[key]:
                product["title"] = str(item[key]).strip()
                break

        # Price
        for key in ["price", "salePrice", "minPrice", "currentPrice"]:
            if key in item and item[key]:
                val = item[key]
                if isinstance(val, dict):
                    product["price"] = str(val.get("minPrice", val.get("value", "")))
                else:
                    product["price"] = str(val).strip()
                break

        # Original price
        for key in ["originalPrice", "oriMinPrice", "retailPrice"]:
            if key in item and item[key]:
                val = item[key]
                if isinstance(val, dict):
                    product["original_price"] = str(val.get("value", ""))
                else:
                    product["original_price"] = str(val).strip()
                break

        # Discount
        for key in ["discount", "salePercentage"]:
            if key in item and item[key]:
                product["discount"] = str(item[key]).strip()
                break

        # Rating
        for key in ["starRating", "averageStar", "rating", "evaluateScore"]:
            if key in item and item[key]:
                product["rating"] = str(item[key]).strip()
                break

        # Reviews count
        for key in ["reviewCount", "totalReview", "reviews"]:
            if key in item and item[key]:
                product["reviews_count"] = str(item[key]).strip()
                break

        # Orders
        for key in ["orders", "tradeCount", "totalOrder", "soldCount"]:
            if key in item and item[key]:
                product["orders_count"] = str(item[key]).strip()
                break

        # Store name
        for key in ["storeName", "store", "sellerName"]:
            if key in item and item[key]:
                val = item[key]
                if isinstance(val, dict):
                    product["store_name"] = str(val.get("storeName", val.get("name", "")))
                else:
                    product["store_name"] = str(val).strip()
                break

        # Product URL
        for key in ["productDetailUrl", "detailUrl", "itemUrl", "url"]:
            if key in item and item[key]:
                url = str(item[key]).strip()
                if not url.startswith("http"):
                    url = "https:" + url if url.startswith("//") else "https://www.aliexpress.com" + url
                product["product_url"] = url
                break

        # Image URL
        for key in ["image", "imageUrl", "imgUrl", "productImage", "pic"]:
            if key in item and item[key]:
                img = str(item[key]).strip()
                if not img.startswith("http"):
                    img = "https:" + img if img.startswith("//") else "https:" + img
                product["image_url"] = img
                break

        return product

    def _extract_from_html_structure(self, html):
        """Extract products by parsing the rendered HTML DOM."""
        products = []
        soup = BeautifulSoup(html, "lxml")

        # Strategy A: Find gallery wrapper cards (real AliExpress DOM structure)
        gallery_cards = soup.find_all("div", class_=re.compile(r"search-item-card-wrapper-gallery"))
        if gallery_cards:
            logger.info(f"Found {len(gallery_cards)} gallery card wrappers")
            for card in gallery_cards:
                link = card.find("a", href=re.compile(r"/item/\d+"))
                if not link:
                    continue
                href = link.get("href", "")
                if not href.startswith("http"):
                    href = "https:" + href if href.startswith("//") else "https://www.aliexpress.com" + href
                product = self._parse_card_element(card, product_url=href)
                if product.get("title"):
                    products.append(product)
            if products:
                logger.info(f"Extracted {len(products)} products from gallery cards")
                return products

        # Strategy B: Find product links and walk up to card containers
        product_links = soup.find_all("a", href=re.compile(r"/item/\d+\.html"))
        if not product_links:
            product_links = soup.find_all("a", href=re.compile(r"/item/\d+"))

        logger.info(f"Found {len(product_links)} product links in HTML")

        seen_urls = set()
        for link in product_links:
            href = link.get("href", "")

            if not href.startswith("http"):
                href = "https:" + href if href.startswith("//") else "https://www.aliexpress.com" + href

            id_match = re.search(r"/item/(\d+)", href)
            item_id = id_match.group(1) if id_match else href

            if item_id in seen_urls:
                continue
            seen_urls.add(item_id)

            # Walk up to find the product card container
            card = link
            for _ in range(5):
                parent = card.parent
                if parent and parent.name in ("div", "li"):
                    text = parent.get_text(strip=True)
                    if len(text) > len(card.get_text(strip=True)):
                        card = parent
                    else:
                        break
                else:
                    break

            product = self._parse_card_element(card, product_url=href)
            if product.get("title"):
                products.append(product)

        return products

    def _parse_card_element(self, card, product_url=""):
        """Parse a product card HTML element into a product dict."""
        product = dict(EMPTY_PRODUCT)
        product["product_url"] = product_url

        # Title
        title_el = (
            card.find("h3")
            or card.find("h1")
            or card.find("h2")
            or card.find(class_=re.compile(r"title", re.I))
        )
        if title_el:
            product["title"] = title_el.get_text(strip=True)
        if not product["title"]:
            a_tag = card if card.name == "a" else card.find("a", title=True)
            if a_tag and a_tag.get("title"):
                product["title"] = a_tag["title"].strip()
        if not product["title"]:
            for el in card.find_all(["span", "div", "p"]):
                text = el.get_text(strip=True)
                if len(text) > 10 and not re.match(r'^[\d\s$.,€£¥₹%Rs]+$', text):
                    product["title"] = text
                    break

        # Price
        all_text = card.get_text(strip=True)
        price_matches = re.findall(r'(?:Rs\.|US\s*\$|€|£|¥|₹)[\d,]+(?:\.\d{1,2})?', all_text)

        if price_matches:
            product["price"] = price_matches[0].strip()
        if not product["price"]:
            price_el = card.find(class_=re.compile(r"price", re.I))
            if price_el:
                raw = price_el.get_text(strip=True)
                m = re.match(r'((?:Rs\.|US\s*\$|€|£|¥|₹)\s*[\d,]+\.?\d*)', raw)
                product["price"] = m.group(1) if m else raw

        # Original price
        if len(price_matches) >= 2:
            for pm in price_matches[1:]:
                if pm.strip() != product["price"].strip():
                    product["original_price"] = pm.strip()
                    break

        # Discount
        discount_match = re.search(r'-?\d+%', all_text)
        if discount_match:
            product["discount"] = discount_match.group(0)

        # Image
        for img_el in card.find_all("img"):
            img_src = (
                img_el.get("src")
                or img_el.get("data-src")
                or img_el.get("data-lazy-src")
                or ""
            )
            if not img_src:
                srcset = img_el.get("srcset", "")
                if srcset:
                    img_src = srcset.split(",")[0].split(" ")[0]
            if img_src and ("alicdn" in img_src or "aliexpress-media" in img_src):
                if not img_src.startswith("http"):
                    img_src = "https:" + img_src if img_src.startswith("//") else "https:" + img_src
                img_src = re.sub(r'_\.avif$', '', img_src)
                product["image_url"] = img_src
                break
        if not product["image_url"]:
            img_el = card.find("img")
            if img_el:
                img_src = img_el.get("src") or img_el.get("data-src") or ""
                if img_src and not img_src.startswith("data:"):
                    if not img_src.startswith("http"):
                        img_src = "https:" + img_src if img_src.startswith("//") else "https:" + img_src
                    img_src = re.sub(r'_\.avif$', '', img_src)
                    product["image_url"] = img_src

        # Store
        store_el = card.find(class_=re.compile(r"store", re.I))
        if store_el:
            product["store_name"] = store_el.get_text(strip=True)

        # Rating
        rating_el = card.find(class_=re.compile(r"star|rating", re.I))
        if rating_el:
            product["rating"] = rating_el.get_text(strip=True)

        # Orders / sold count
        spaced_text = card.get_text(" ", strip=True)
        sold_match = re.search(r'(\d[\d,]*\+?\s*sold)', spaced_text, re.I)
        if sold_match:
            product["orders_count"] = sold_match.group(1).strip()
        if not product["orders_count"]:
            orders_el = card.find(class_=re.compile(r"order|sold", re.I))
            if orders_el:
                product["orders_count"] = orders_el.get_text(strip=True)

        return product

    def scrape(self, query, pages=1):
        """Scrape AliExpress search results for the given query.

        Args:
            query: Search term
            pages: Number of result pages to scrape

        Returns:
            List of product dicts
        """
        all_products = []

        try:
            for page in range(1, pages + 1):
                url = self._build_search_url(query, page)
                logger.info(f"Scraping page {page}/{pages}: {url}")

                html = self._fetch_page(url)
                if not html:
                    logger.warning(f"Failed to fetch page {page}")
                    continue

                products = self._extract_products_from_html(html)
                logger.info(f"Found {len(products)} products on page {page}")
                all_products.extend(products)

                if page < pages:
                    delay = random.uniform(settings.delay_min, settings.delay_max)
                    logger.info(f"Waiting {delay:.1f}s before next page...")
                    time.sleep(delay)
        finally:
            self._close_browser()

        # Deduplicate by title
        seen = set()
        unique_products = []
        for p in all_products:
            key = p.get("title", "").lower().strip()
            if key and key not in seen:
                seen.add(key)
                unique_products.append(p)

        logger.info(f"Total unique products scraped: {len(unique_products)}")
        return unique_products
