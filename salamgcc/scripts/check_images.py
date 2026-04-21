"""Check if scraped listings have Cloudflare image URLs stored in MongoDB."""

import sys
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from ad_extractor.database.mongodb import mongo_client


def check_url_reachable(url: str) -> bool:
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def main():
    if not mongo_client.connect():
        print("✗ Failed to connect to MongoDB")
        return

    collection = mongo_client.db["listings"]
    total = collection.count_documents({"source_site": "erikolsson"})
    print(f"Total erikolsson docs in DB: {total}\n")

    if total == 0:
        print("No erikolsson listings found.")
        mongo_client.close()
        return

    with_images = 0
    without_images = 0
    cf_ok = 0
    cf_broken = 0

    cursor = collection.find({"source_site": "erikolsson"}, {"postAdData.images": 1, "source_url": 1})

    for doc in cursor:
        images = doc.get("postAdData", {}).get("images", [])
        source = doc.get("source_url", "unknown")

        if not images:
            without_images += 1
            print(f"  ✗ NO IMAGES  — {source}")
            continue

        with_images += 1
        # Check first image URL is a Cloudflare link and reachable
        first = images[0]
        is_cf = "imagedelivery.net" in first
        reachable = check_url_reachable(first) if is_cf else False

        if is_cf and reachable:
            cf_ok += 1
            print(f"  ✓ {len(images)} images, CF link OK — {source}")
        elif is_cf and not reachable:
            cf_broken += 1
            print(f"  ⚠ {len(images)} images, CF link UNREACHABLE — {source}")
            print(f"      {first}")
        else:
            cf_broken += 1
            print(f"  ✗ {len(images)} images, NOT a Cloudflare URL — {source}")
            print(f"      {first}")

    print("\n── Summary ──")
    print(f"  Docs with images    : {with_images}")
    print(f"  Docs without images : {without_images}")
    print(f"  CF links OK         : {cf_ok}")
    print(f"  CF links broken/missing: {cf_broken}")

    mongo_client.close()


if __name__ == "__main__":
    main()
