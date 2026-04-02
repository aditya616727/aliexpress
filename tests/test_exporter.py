import csv
import json
import os
import tempfile
import pytest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from ali_scraper.export.data import DataExporter
from ali_scraper.config.settings import settings

PRODUCT_FIELDS = settings.product_fields


SAMPLE_PRODUCTS = [
    {
        "title": "Wireless Earbuds Pro",
        "price": "US $15.99",
        "original_price": "US $29.99",
        "discount": "47%",
        "rating": "4.8",
        "reviews_count": "1520",
        "orders_count": "5000+",
        "store_name": "AudioTech Store",
        "product_url": "https://www.aliexpress.com/item/123456.html",
        "image_url": "https://ae01.alicdn.com/kf/earbuds.jpg",
        "image_path": "/tmp/images/0000_wireless_earbuds.jpg",
    },
    {
        "title": "USB-C Hub 7 in 1",
        "price": "US $22.50",
        "original_price": "US $40.00",
        "discount": "44%",
        "rating": "4.6",
        "reviews_count": "890",
        "orders_count": "3200+",
        "store_name": "GadgetHub",
        "product_url": "https://www.aliexpress.com/item/789012.html",
        "image_url": "https://ae01.alicdn.com/kf/hub.jpg",
        "image_path": "/tmp/images/0001_usb_c_hub.jpg",
    },
    {
        "title": "Portable Phone Charger",
        "price": "US $18.00",
        "original_price": "",
        "discount": "",
        "rating": "4.5",
        "reviews_count": "456",
        "orders_count": "1800+",
        "store_name": "PowerWorld",
        "product_url": "https://www.aliexpress.com/item/345678.html",
        "image_url": "https://ae01.alicdn.com/kf/charger.jpg",
        "image_path": "",
    },
]


class TestDataExporter:

    def setup_method(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.exporter = DataExporter(output_dir=self.tmp_dir)

    def teardown_method(self):
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    # --- CSV Export ---

    def test_csv_export_creates_file(self):
        path = self.exporter.export_to_csv(SAMPLE_PRODUCTS)
        assert os.path.exists(path)
        assert path.endswith(".csv")

    def test_csv_export_correct_row_count(self):
        path = self.exporter.export_to_csv(SAMPLE_PRODUCTS)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert len(rows) == len(SAMPLE_PRODUCTS) + 1  # +1 for header

    def test_csv_export_header_matches_fields(self):
        path = self.exporter.export_to_csv(SAMPLE_PRODUCTS)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
        assert header == PRODUCT_FIELDS

    def test_csv_export_data_integrity(self):
        path = self.exporter.export_to_csv(SAMPLE_PRODUCTS)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert rows[0]["title"] == "Wireless Earbuds Pro"
        assert rows[1]["price"] == "US $22.50"
        assert rows[2]["store_name"] == "PowerWorld"

    def test_csv_export_custom_filename(self):
        path = self.exporter.export_to_csv(SAMPLE_PRODUCTS, filename="custom.csv")
        assert path.endswith("custom.csv")
        assert os.path.exists(path)

    def test_csv_export_empty_products(self):
        path = self.exporter.export_to_csv([])
        assert os.path.exists(path)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert len(rows) == 1  # Header only

    def test_csv_handles_unicode(self):
        products = [
            {**SAMPLE_PRODUCTS[0], "title": "蓝牙耳机 Bluetooth Headset 日本語"},
        ]
        path = self.exporter.export_to_csv(products)
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "蓝牙耳机" in content

    # --- JSON Export ---

    def test_json_export_creates_file(self):
        path = self.exporter.export_to_json(SAMPLE_PRODUCTS)
        assert os.path.exists(path)
        assert path.endswith(".json")

    def test_json_export_valid_json(self):
        path = self.exporter.export_to_json(SAMPLE_PRODUCTS)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert isinstance(data, dict)

    def test_json_export_structure(self):
        path = self.exporter.export_to_json(SAMPLE_PRODUCTS)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "total_products" in data
        assert "products" in data
        assert data["total_products"] == 3
        assert len(data["products"]) == 3

    def test_json_export_data_integrity(self):
        path = self.exporter.export_to_json(SAMPLE_PRODUCTS)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        products = data["products"]
        assert products[0]["title"] == "Wireless Earbuds Pro"
        assert products[1]["rating"] == "4.6"
        assert products[2]["product_url"] == "https://www.aliexpress.com/item/345678.html"

    def test_json_export_custom_filename(self):
        path = self.exporter.export_to_json(SAMPLE_PRODUCTS, filename="custom.json")
        assert path.endswith("custom.json")
        assert os.path.exists(path)

    def test_json_export_empty_products(self):
        path = self.exporter.export_to_json([])
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["total_products"] == 0
        assert data["products"] == []

    def test_json_handles_unicode(self):
        products = [
            {**SAMPLE_PRODUCTS[0], "title": "蓝牙耳机 Bluetooth 日本語"},
        ]
        path = self.exporter.export_to_json(products)
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "蓝牙耳机" in content

    # --- Export All ---

    def test_export_all_creates_both_files(self):
        csv_path, json_path = self.exporter.export_all(SAMPLE_PRODUCTS)
        assert os.path.exists(csv_path)
        assert os.path.exists(json_path)
        assert csv_path.endswith(".csv")
        assert json_path.endswith(".json")

    def test_export_all_consistent_data(self):
        csv_path, json_path = self.exporter.export_all(SAMPLE_PRODUCTS)

        with open(csv_path, "r", encoding="utf-8") as f:
            csv_rows = list(csv.DictReader(f))

        with open(json_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)

        assert len(csv_rows) == len(json_data["products"])
        assert csv_rows[0]["title"] == json_data["products"][0]["title"]

    # --- Output Directory ---

    def test_creates_output_directory(self):
        new_dir = os.path.join(self.tmp_dir, "nested", "output")
        exporter = DataExporter(output_dir=new_dir)
        assert os.path.isdir(new_dir)
