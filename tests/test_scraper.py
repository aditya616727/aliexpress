import json
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper import AliExpressScraper


# --- Sample HTML and data fixtures ---

SAMPLE_HTML_WITH_CARDS = """
<html>
<body>
<div class="search-item-card">
    <a href="//www.aliexpress.com/item/123456.html" title="Wireless Bluetooth Earbuds">
        <img src="//ae01.alicdn.com/kf/image1.jpg" />
    </a>
    <h3>Wireless Bluetooth Earbuds</h3>
    <div class="price">US $12.99</div>
    <div class="store">TechStore Official</div>
    <div class="rating">4.8</div>
    <div class="orders">500+ sold</div>
</div>
<div class="search-item-card">
    <a href="//www.aliexpress.com/item/789012.html" title="USB C Hub Adapter">
        <img src="//ae01.alicdn.com/kf/image2.jpg" />
    </a>
    <h3>USB C Hub Adapter</h3>
    <div class="price">US $25.50</div>
    <div class="store">GadgetWorld</div>
    <div class="rating">4.5</div>
    <div class="orders">1200+ sold</div>
</div>
</body>
</html>
"""

SAMPLE_HTML_WITH_LINKS = """
<html>
<body>
<a href="//www.aliexpress.com/item/111111.html" title="Smart Watch Band">
    <img src="//ae01.alicdn.com/kf/watch.jpg" />
    <h3>Smart Watch Band</h3>
    <div class="price">US $8.99</div>
</a>
<a href="//www.aliexpress.com/item/222222.html" title="Phone Case Cover">
    <img src="//ae01.alicdn.com/kf/case.jpg" />
    <h3>Phone Case Cover</h3>
    <div class="price">US $3.50</div>
</a>
</body>
</html>
"""

SAMPLE_JSON_DATA = [
    {
        "title": "Portable Charger 10000mAh",
        "price": "15.99",
        "originalPrice": "29.99",
        "discount": "47%",
        "starRating": "4.7",
        "reviewCount": "2340",
        "orders": "5000+",
        "storeName": "PowerBank Official",
        "productDetailUrl": "//www.aliexpress.com/item/333333.html",
        "image": "//ae01.alicdn.com/kf/charger.jpg",
    },
    {
        "productTitle": "LED Desk Lamp",
        "salePrice": "22.00",
        "oriMinPrice": "35.00",
        "averageStar": "4.9",
        "totalReview": "890",
        "tradeCount": "3000+",
        "store": {"storeName": "LightWorld"},
        "detailUrl": "//www.aliexpress.com/item/444444.html",
        "imageUrl": "//ae01.alicdn.com/kf/lamp.jpg",
    },
]

SAMPLE_HTML_WITH_SCRIPT = """
<html>
<body>
<script>
var data = {"itemList": %s};
</script>
</body>
</html>
""" % json.dumps(SAMPLE_JSON_DATA)

EMPTY_HTML = "<html><body><div>No products here</div></body></html>"


class TestAliExpressScraper:

    def setup_method(self):
        self.scraper = AliExpressScraper()

    # --- URL Building ---

    def test_build_search_url_basic(self):
        url = self.scraper._build_search_url("wireless earbuds")
        assert "wireless+earbuds" in url or "wireless%20earbuds" in url
        assert "aliexpress.com" in url

    def test_build_search_url_page_2(self):
        url = self.scraper._build_search_url("phone case", page=2)
        assert "page=2" in url

    def test_build_search_url_page_1_no_param(self):
        url = self.scraper._build_search_url("test", page=1)
        assert "page=" not in url

    def test_build_search_url_special_chars(self):
        url = self.scraper._build_search_url("usb-c hub & adapter")
        assert "aliexpress.com" in url

    # --- HTML Card Extraction ---

    def test_extract_from_html_cards(self):
        products = self.scraper._extract_from_html_structure(SAMPLE_HTML_WITH_CARDS)
        assert len(products) == 2
        titles = [p["title"] for p in products]
        assert "Wireless Bluetooth Earbuds" in titles
        assert "USB C Hub Adapter" in titles

    def test_extract_price_from_cards(self):
        products = self.scraper._extract_from_html_structure(SAMPLE_HTML_WITH_CARDS)
        prices = [p["price"] for p in products]
        assert any("12.99" in p for p in prices)
        assert any("25.50" in p for p in prices)

    def test_extract_store_from_cards(self):
        products = self.scraper._extract_from_html_structure(SAMPLE_HTML_WITH_CARDS)
        stores = [p["store_name"] for p in products]
        assert any("TechStore" in s for s in stores)

    def test_extract_image_url_from_cards(self):
        products = self.scraper._extract_from_html_structure(SAMPLE_HTML_WITH_CARDS)
        image_urls = [p["image_url"] for p in products]
        assert any("image1.jpg" in u for u in image_urls)
        assert all(u.startswith("https:") for u in image_urls if u)

    def test_extract_product_url_from_cards(self):
        products = self.scraper._extract_from_html_structure(SAMPLE_HTML_WITH_CARDS)
        urls = [p["product_url"] for p in products]
        assert any("123456" in u for u in urls)
        assert all(u.startswith("https:") for u in urls if u)

    def test_extract_sold_count_from_cards(self):
        products = self.scraper._extract_from_html_structure(SAMPLE_HTML_WITH_CARDS)
        orders = [p["orders_count"] for p in products]
        assert any("sold" in o.lower() for o in orders if o)

    def test_extract_from_link_based_html(self):
        products = self.scraper._extract_from_html_structure(SAMPLE_HTML_WITH_LINKS)
        assert len(products) == 2
        titles = [p["title"] for p in products]
        assert "Smart Watch Band" in titles
        assert "Phone Case Cover" in titles

    def test_extract_from_empty_html(self):
        products = self.scraper._extract_from_html_structure(EMPTY_HTML)
        assert products == []

    # --- JSON Extraction ---

    def test_parse_json_item_standard(self):
        item = SAMPLE_JSON_DATA[0]
        product = self.scraper._parse_json_item(item)
        assert product["title"] == "Portable Charger 10000mAh"
        assert product["price"] == "15.99"
        assert product["original_price"] == "29.99"
        assert product["discount"] == "47%"
        assert product["rating"] == "4.7"
        assert product["reviews_count"] == "2340"
        assert product["orders_count"] == "5000+"
        assert product["store_name"] == "PowerBank Official"
        assert "333333" in product["product_url"]
        assert "charger.jpg" in product["image_url"]

    def test_parse_json_item_alternate_keys(self):
        item = SAMPLE_JSON_DATA[1]
        product = self.scraper._parse_json_item(item)
        assert product["title"] == "LED Desk Lamp"
        assert product["price"] == "22.00"
        assert product["original_price"] == "35.00"
        assert product["rating"] == "4.9"
        assert product["store_name"] == "LightWorld"

    def test_parse_json_item_empty(self):
        product = self.scraper._parse_json_item({})
        assert product["title"] == ""
        assert product["price"] == ""

    def test_find_items_in_nested_data(self):
        nested = {
            "data": {
                "root": {
                    "fields": {
                        "itemList": [
                            {"title": "Product A", "price": "10.00", "productId": "1"},
                            {"title": "Product B", "price": "20.00", "productId": "2"},
                        ]
                    }
                }
            }
        }
        items = self.scraper._find_items_in_data(nested)
        assert len(items) == 2
        assert items[0]["title"] == "Product A"

    def test_find_items_in_flat_list(self):
        flat = [
            {"title": "Item 1", "price": "5.00"},
            {"title": "Item 2", "price": "10.00"},
        ]
        items = self.scraper._find_items_in_data(flat)
        assert len(items) == 2

    def test_extract_from_script_data(self):
        products = self.scraper._extract_from_script_data(SAMPLE_HTML_WITH_SCRIPT)
        assert len(products) >= 1

    # --- Combined Extraction ---

    def test_extract_products_prefers_json(self):
        products = self.scraper._extract_products_from_html(SAMPLE_HTML_WITH_SCRIPT)
        assert len(products) >= 1

    def test_extract_products_falls_back_to_html(self):
        products = self.scraper._extract_products_from_html(SAMPLE_HTML_WITH_CARDS)
        assert len(products) == 2

    # --- URL Fixing ---

    def test_image_url_protocol_fix(self):
        item = {"title": "Test", "image": "//ae01.alicdn.com/kf/test.jpg", "price": "1"}
        product = self.scraper._parse_json_item(item)
        assert product["image_url"].startswith("https://")

    def test_product_url_protocol_fix(self):
        item = {"title": "Test", "productDetailUrl": "//www.aliexpress.com/item/1.html", "price": "1"}
        product = self.scraper._parse_json_item(item)
        assert product["product_url"].startswith("https://")

    # --- Browser lifecycle ---

    def test_close_browser_no_error_when_not_launched(self):
        self.scraper._close_browser()

    # --- Deduplication in scrape (mocked) ---

    def test_scrape_deduplication(self, mocker):
        html = SAMPLE_HTML_WITH_CARDS
        mocker.patch.object(self.scraper, "_fetch_page", return_value=html)
        mocker.patch.object(self.scraper, "_close_browser")
        products = self.scraper.scrape("test", pages=2)
        titles = [p["title"] for p in products]
        assert len(titles) == len(set(t.lower() for t in titles))

    def test_scrape_handles_fetch_failure(self, mocker):
        mocker.patch.object(self.scraper, "_fetch_page", return_value=None)
        mocker.patch.object(self.scraper, "_close_browser")
        products = self.scraper.scrape("test", pages=1)
        assert products == []

    # --- Product field completeness ---

    def test_product_has_all_fields(self):
        products = self.scraper._extract_from_html_structure(SAMPLE_HTML_WITH_CARDS)
        expected_fields = {
            "title", "price", "original_price", "discount", "rating",
            "reviews_count", "orders_count", "store_name",
            "product_url", "image_url", "image_path",
        }
        for p in products:
            assert set(p.keys()) == expected_fields

    # --- Card element parsing edge cases ---

    def test_parse_card_with_no_image(self):
        from bs4 import BeautifulSoup
        html = '<div><a href="//www.aliexpress.com/item/999.html"><h3>No Image Product</h3></a></div>'
        soup = BeautifulSoup(html, "lxml")
        card = soup.find("div")
        product = self.scraper._parse_card_element(card, product_url="https://www.aliexpress.com/item/999.html")
        assert product["title"] == "No Image Product"
        assert product["image_url"] == ""

    def test_parse_card_with_data_src_image(self):
        from bs4 import BeautifulSoup
        html = '<div><a href="//www.aliexpress.com/item/888.html"><h3>Lazy Image</h3><img data-src="//cdn.example.com/img.jpg" /></a></div>'
        soup = BeautifulSoup(html, "lxml")
        card = soup.find("div")
        product = self.scraper._parse_card_element(card, product_url="https://test.com")
        assert "img.jpg" in product["image_url"]

    def test_parse_card_title_from_attribute(self):
        from bs4 import BeautifulSoup
        html = '<a href="//www.aliexpress.com/item/777.html" title="Attr Title Product"><img src="//cdn.example.com/x.jpg" /></a>'
        soup = BeautifulSoup(html, "lxml")
        card = soup.find("a")
        product = self.scraper._parse_card_element(card, product_url="https://test.com")
        assert product["title"] == "Attr Title Product"
