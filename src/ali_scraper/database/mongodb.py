"""MongoDB storage module for scraped clothing products."""

import re
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

from ..config import settings

logger = logging.getLogger(__name__)


def _parse_price(price_str):
    """Extract numeric price from string like 'Rs.3,049.37' or 'US $12.99'."""
    if not price_str:
        return 0.0
    cleaned = re.sub(r'[^\d.]', '', price_str.replace(',', ''))
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


def _parse_discount(discount_str):
    """Extract discount percentage from string like '-20%' or '47%'."""
    if not discount_str:
        return 0
    match = re.search(r'(\d+)', discount_str)
    if match:
        return min(int(match.group(1)), 100)
    return 0


def _guess_clothing_attributes(title):
    """Infer clothing attributes from the product title."""
    title_lower = title.lower()

    # Gender detection
    gender = "Unisex"
    if any(w in title_lower for w in ["women", "woman", "female", "ladies", "girl"]):
        gender = "Female"
    elif any(w in title_lower for w in ["men", "man", "male", "boy"]):
        gender = "Male"
    elif any(w in title_lower for w in ["baby", "infant", "toddler", "kid"]):
        gender = "Baby"
    elif "couple" in title_lower:
        gender = "Couples"

    # Clothing type detection
    clothing_type = "Other"
    type_map = {
        "t-shirt": "T-Shirt", "tshirt": "T-Shirt", "shirt": "Shirt",
        "dress": "Dress", "jacket": "Jacket", "coat": "Coat",
        "sweater": "Sweater", "hoodie": "Hoodie", "pants": "Pants",
        "jeans": "Jeans", "shorts": "Shorts", "skirt": "Skirt",
        "blouse": "Blouse", "suit": "Suit", "vest": "Vest",
        "cardigan": "Cardigan", "legging": "Leggings", "sock": "Socks",
        "underwear": "Underwear", "bra": "Bra", "scarf": "Scarf",
        "hat": "Hat", "cap": "Cap", "glove": "Gloves",
        "earbuds": "Accessories", "headset": "Accessories",
        "headphone": "Accessories", "earphone": "Accessories",
        "watch": "Accessories", "bag": "Bag", "shoe": "Shoes",
        "boot": "Boots", "sandal": "Sandals", "sneaker": "Sneakers",
    }
    for keyword, ctype in type_map.items():
        if keyword in title_lower:
            clothing_type = ctype
            break

    # Season
    season = ""
    if any(w in title_lower for w in ["summer", "lightweight", "thin", "cool"]):
        season = "Summer"
    elif any(w in title_lower for w in ["winter", "warm", "thermal", "fleece", "thick"]):
        season = "Winter"
    elif any(w in title_lower for w in ["spring", "autumn", "fall"]):
        season = "Spring/Autumn"

    return {
        "gender": gender,
        "clothingType": clothing_type,
        "season": season,
    }


def product_to_clothing_doc(product):
    """Convert a scraped product dict to a MongoDB Clothing document."""
    title = product.get("title", "")
    price = _parse_price(product.get("price", ""))
    original_price = _parse_price(product.get("original_price", ""))
    discount = _parse_discount(product.get("discount", ""))
    attrs = _guess_clothing_attributes(title)

    # Use Cloudflare image URLs if available, fallback to original
    images = product.get("images", [])
    if not images:
        img = product.get("image_url", "")
        images = [img] if img else []

    doc = {
        "title": title,
        "description": title,
        "price": price if price > 0 else 1.0,
        "country": "China",
        "state": "",
        "city": "",
        "address": "",
        "images": images,
        "discountPercentage": discount,
        "gender": attrs["gender"],
        "clothingType": attrs["clothingType"],
        "season": attrs.get("season", ""),
        "features": [],
        "variants": [],
    }

    # Add original price as a variant if different from sale price
    if original_price > 0 and original_price != price:
        doc["variants"].append({
            "variantId": "original",
            "name": "Original Price",
            "price": original_price,
            "attributes": {},
            "images": images,
            "discountPercentage": discount,
        })

    return doc


class MongoDBStorage:
    """Store scraped products in MongoDB using the Clothing schema."""

    def __init__(self, uri=None, db_name=None):
        self.uri = uri or settings.mongodb_uri
        if not self.uri:
            raise ValueError(
                "MongoDB URI is required. Set MONGODB_URI in .env"
            )
        self.client = None
        self.db = None
        self.collection = None
        self._db_name = db_name

    def connect(self):
        """Establish connection to MongoDB."""
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=10000)
            self.client.admin.command("ping")

            if self._db_name:
                self.db = self.client[self._db_name]
            else:
                self.db = self.client.get_default_database()

            self.collection = self.db["clothings"]
            logger.info(f"Connected to MongoDB: {self.db.name}")
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def close(self):
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def insert_products(self, products):
        """Convert and insert scraped products into the Clothing collection.

        Returns:
            Number of documents inserted
        """
        if self.collection is None:
            self.connect()

        docs = []
        for product in products:
            doc = product_to_clothing_doc(product)
            if doc["title"]:
                docs.append(doc)

        if not docs:
            logger.warning("No valid documents to insert")
            return 0

        try:
            result = self.collection.insert_many(docs)
            count = len(result.inserted_ids)
            logger.info(f"Inserted {count} documents into MongoDB")
            return count
        except OperationFailure as e:
            logger.error(f"MongoDB insert failed: {e}")
            raise


# Module-level singleton (like ad-extractor's mongo_client)
mongo_storage = None


def get_mongo_storage():
    """Get or create the singleton MongoDBStorage instance."""
    global mongo_storage
    if mongo_storage is None:
        mongo_storage = MongoDBStorage()
    return mongo_storage
