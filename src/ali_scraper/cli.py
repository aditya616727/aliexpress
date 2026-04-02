"""CLI orchestrator — analogous to ad-extractor's cli.py.

Manages the full pipeline:
    Categories → Scrape → Download images → Upload to Cloudflare → Store in MongoDB → Export
"""

import argparse
import logging
import os
import sys

from .config import settings, load_categories
from .scrapers import AliExpressScraper
from .export import DataExporter, ImageDownloader
from .cloudflare import CloudflareUploader
from .database import MongoDBStorage

logger = logging.getLogger(__name__)


def setup_logging(verbose=False):
    level = logging.DEBUG if verbose else getattr(logging, settings.log_level, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def scrape_category(query, pages=1, output_dir=None, download_images=True,
                    upload_cloudflare=True, store_db=True):
    """Run the full scraping pipeline for a single search query.

    Pipeline:
        1. Scrape AliExpress search results
        2. Download product images locally
        3. Upload images to Cloudflare Images
        4. Delete local images after upload
        5. Store product data in MongoDB (Clothing collection)
        6. Export data to CSV and JSON

    Returns:
        List of product dicts
    """
    # 1. Scrape products
    logger.info(f"Starting scrape for: '{query}' ({pages} page(s))")
    with AliExpressScraper() as scraper:
        products = scraper.scrape(query, pages=pages)

    if not products:
        logger.warning("No products found. AliExpress may be blocking the request or the page structure changed.")
        return []

    logger.info(f"Scraped {len(products)} products")

    # 2. Download images locally
    if download_images:
        logger.info("Downloading product images...")
        image_dir = os.path.join(output_dir, "images") if output_dir else None
        downloader = ImageDownloader(output_dir=image_dir)
        downloaded = downloader.download_all(products)
        logger.info(f"Downloaded {downloaded} images")

        # 3. Upload to Cloudflare Images and delete local files
        if upload_cloudflare:
            logger.info("Uploading images to Cloudflare...")
            try:
                cf_uploader = CloudflareUploader()
                cf_uploaded = cf_uploader.upload_all(products, delete_local=True)
                logger.info(f"Uploaded {cf_uploaded} images to Cloudflare, local files deleted")
            except ValueError as e:
                logger.error(f"Cloudflare upload skipped: {e}")
            except Exception as e:
                logger.error(f"Cloudflare upload failed: {e}")

    # 4. Store in MongoDB
    if store_db:
        logger.info("Storing products in MongoDB...")
        try:
            mongo = MongoDBStorage()
            mongo.connect()
            inserted = mongo.insert_products(products)
            logger.info(f"Inserted {inserted} products into MongoDB")
            mongo.close()
        except ValueError as e:
            logger.error(f"MongoDB storage skipped: {e}")
        except Exception as e:
            logger.error(f"MongoDB storage failed: {e}")

    # 5. Export data to CSV and JSON
    logger.info("Exporting data to CSV and JSON...")
    exporter = DataExporter(output_dir=output_dir)
    csv_path, json_path = exporter.export_all(products)
    logger.info(f"CSV exported to: {csv_path}")
    logger.info(f"JSON exported to: {json_path}")

    return products


def scrape_all(categories=None, output_dir=None, download_images=True,
               upload_cloudflare=True, store_db=True):
    """Scrape all categories, print summary.

    Analogous to ad-extractor's scrape_all() which iterates all dealers.
    """
    if categories is None:
        categories = load_categories()

    summary = []

    for cat in categories:
        logger.info(f"--- Category: {cat.name} (query: '{cat.query}') ---")
        products = scrape_category(
            query=cat.query,
            pages=cat.pages,
            output_dir=output_dir,
            download_images=download_images,
            upload_cloudflare=upload_cloudflare,
            store_db=store_db,
        )
        summary.append((cat.name, len(products)))

    # Print summary
    print(f"\n{'='*60}")
    print("Scraping complete!")
    print(f"{'='*60}")
    for name, count in summary:
        print(f"  {name}: {count} products")
    total = sum(c for _, c in summary)
    print(f"  Total: {total} products")
    print(f"{'='*60}")

    return summary


def main():
    """Entry point — parse CLI args and run the pipeline."""
    parser = argparse.ArgumentParser(
        description="Scrape products from AliExpress, upload images to Cloudflare, store in MongoDB"
    )
    parser.add_argument(
        "--query", "-q",
        type=str,
        default=None,
        help="Search query (e.g., 'wireless earbuds'). If omitted, uses config/categories.yaml.",
    )
    parser.add_argument(
        "--pages", "-p",
        type=int,
        default=1,
        help="Number of pages to scrape (default: 1)",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output directory (default: from .env OUTPUT_DIR)",
    )
    parser.add_argument(
        "--no-images",
        action="store_true",
        help="Skip image downloading and Cloudflare upload",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip MongoDB storage",
    )
    parser.add_argument(
        "--no-cloudflare",
        action="store_true",
        help="Skip Cloudflare upload (keep local images)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    args = parser.parse_args()

    setup_logging(verbose=args.verbose)
    settings.ensure_directories()

    if args.query:
        # Single query mode (like before)
        products = scrape_category(
            query=args.query,
            pages=args.pages,
            output_dir=args.output,
            download_images=not args.no_images,
            upload_cloudflare=not args.no_cloudflare,
            store_db=not args.no_db,
        )

        print(f"\n{'='*60}")
        print("Scraping complete!")
        print(f"Products found: {len(products)}")
        if products:
            print(f"\nSample products:")
            for i, p in enumerate(products[:5]):
                title = p.get('title', 'N/A')[:70]
                price = p.get('price', 'N/A')
                images = p.get('images', [])
                cf_status = "Cloudflare" if images else "Local/None"
                print(f"  {i+1}. {title}")
                print(f"     Price: {price} | Images: {cf_status}")
        print(f"{'='*60}")

        return 0 if products else 1
    else:
        # Category mode — load from YAML (like ad-extractor's dealer mode)
        scrape_all(
            output_dir=args.output,
            download_images=not args.no_images,
            upload_cloudflare=not args.no_cloudflare,
            store_db=not args.no_db,
        )
        return 0
