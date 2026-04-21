"""Store scraped erikolsson listings into MongoDB using MongoDBClient."""

import json
from pathlib import Path
from src.ad_extractor.database.mongodb import mongo_client

LISTINGS_FILE = Path.cwd() / "config/scraped_listings.json"


def strip_nulls(obj):
    """Recursively remove keys with None or empty string values."""
    if isinstance(obj, dict):
        return {k: strip_nulls(v) for k, v in obj.items() if v is not None and v != ""}
    if isinstance(obj, list):
        return [strip_nulls(i) for i in obj]
    return obj


def main():
    with open(LISTINGS_FILE, "r", encoding="utf-8") as f:
        listings = json.load(f)
    print(f"✓ Loaded {len(listings)} listings")

    if not mongo_client.connect():
        print("✗ Failed to connect to MongoDB")
        return

    collection = mongo_client.db["listings"]
    inserted = 0
    updated = 0

    for raw in listings:
        # If already wrapped in postAdData, use as-is, otherwise wrap it
        if "postAdData" in raw:
            post = raw["postAdData"]
            doc = raw
        else:
            post = {k: v for k, v in raw.items() if k not in ("category",)}
            doc = {"category": raw.get("category", "real estate"), "postAdData": post}

        title = post.get("title", "")
        address = post.get("address", "")
        source_url = f"{title}_{address}"

        doc["source_url"] = source_url
        doc["source_site"] = "erikolsson"
        doc = strip_nulls(doc)

        result = collection.update_one(
            {"source_url": source_url},
            {"$set": doc},
            upsert=True
        )
        if result.upserted_id:
            inserted += 1
        else:
            updated += 1

    print(f"✓ Inserted: {inserted}, Updated: {updated}")
    mongo_client.close()


if __name__ == "__main__":
    main()
