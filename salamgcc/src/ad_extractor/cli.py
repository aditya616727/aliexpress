"""Main CLI entry point for ad-extractor."""

import queue
import sys
import threading
from datetime import datetime
from typing import Any, Dict, List, Tuple

from loguru import logger


def _strip_nulls(obj: Any) -> None:
    """Remove keys with None or empty string values from dicts (mutates in place)."""
    if isinstance(obj, dict):
        for k in list(obj.keys()):
            if obj[k] is None or obj[k] == "":
                del obj[k]
            else:
                _strip_nulls(obj[k])
    elif isinstance(obj, list):
        for x in obj:
            _strip_nulls(x)

from .config import load_dealers, settings
from .config.dealers import DealerConfig
from .database import mongo_client
from .scrapers import BlocketScraper
from .translation import translate_listings

SENTINEL = None


def _scraper_worker(
    worker_id: int,
    url_queue: queue.Queue,
    output_queue: queue.Queue,
    dealer: DealerConfig,
    scrape_failures: List[Tuple[str, str]],
    failures_lock: threading.Lock,
) -> None:
    """Scrape URLs from queue and put (data, dealer) in output_queue."""
    with BlocketScraper(
        dealer.business_id,
        translate_to_english=False,
    ) as scraper:
        while True:
            url = url_queue.get()
            if url is SENTINEL:
                url_queue.task_done()
                break
            try:
                data = scraper.scrape_listing(url)
                if data:
                    output_queue.put((data, dealer))
            except Exception as e:
                msg = getattr(e, "msg", None) or str(e) or type(e).__name__
                logger.error(f"[Scraper {worker_id}] Failed {url}: {msg}")
                with failures_lock:
                    scrape_failures.append((url, msg))
            finally:
                url_queue.task_done()


def _consumer_worker(
    worker_id: int,
    output_queue: queue.Queue,
    translate_to_english: bool,
    saved_counter: list,
    counter_lock: threading.Lock,
    batch_size: int,
    insert_failures: List[Tuple[str, str]],
    failures_lock: threading.Lock,
) -> None:
    """Get (data, dealer) from queue, batch translate, batch insert."""
    batch: list = []
    while True:
        item = output_queue.get()
        if item is SENTINEL:
            break
        data, dealer = item
        data["dealer_name"] = dealer.name
        data["dealer_email"] = dealer.email
        data["dealer_phone"] = dealer.phone
        data["dealer_location"] = dealer.location
        data["scraped_at"] = datetime.utcnow()
        batch.append((data, dealer))

        if len(batch) < batch_size:
            continue

        try:
            batch_data = [b[0] for b in batch]
            batch_dealers = [b[1] for b in batch]
            if translate_to_english and batch_data:
                batch_data = translate_listings(batch_data, use_async=True)
            for data, dealer in zip(batch_data, batch_dealers):
                if "postAdData" in data:
                    data["postAdData"]["city"] = dealer.location
                    data["postAdData"]["address"] = dealer.location
                    data["postAdData"]["country"] = "Sweden"
                    data["postAdData"]["regionalSpecs"] = "European Specs"
                _strip_nulls(data)
            count = mongo_client.insert_many_listings(batch_data)
            if count:
                with counter_lock:
                    saved_counter[0] += count
                logger.success(f"[Consumer {worker_id}] Saved batch of {count} listings")
            elif batch_data:
                err = "Insert returned 0"
                with failures_lock:
                    for d in batch_data:
                        url = d.get("source_url", "?")
                        insert_failures.append((url, err))
        except Exception as e:
            logger.error(f"[Consumer {worker_id}] Batch failed: {e}")
            err = str(e)
            with failures_lock:
                for d in [b[0] for b in batch]:
                    url = d.get("source_url", "?")
                    insert_failures.append((url, err))
        finally:
            batch = []
            for _ in range(batch_size):
                output_queue.task_done()

    # Flush remainder (sentinel received)
    if batch:
        try:
            batch_data = [b[0] for b in batch]
            batch_dealers = [b[1] for b in batch]
            if translate_to_english and batch_data:
                batch_data = translate_listings(batch_data, use_async=True)
            for data, dealer in zip(batch_data, batch_dealers):
                if "postAdData" in data:
                    data["postAdData"]["city"] = dealer.location
                    data["postAdData"]["address"] = dealer.location
                    data["postAdData"]["country"] = "Sweden"
                    data["postAdData"]["regionalSpecs"] = "European Specs"
                _strip_nulls(data)
            count = mongo_client.insert_many_listings(batch_data)
            if count:
                with counter_lock:
                    saved_counter[0] += count
                logger.success(f"[Consumer {worker_id}] Saved flush of {count} listings")
            elif batch_data:
                err = "Insert returned 0"
                with failures_lock:
                    for d in batch_data:
                        url = d.get("source_url", "?")
                        insert_failures.append((url, err))
        except Exception as e:
            logger.error(f"[Consumer {worker_id}] Flush failed: {e}")
            err = str(e)
            with failures_lock:
                for d in [b[0] for b in batch]:
                    url = d.get("source_url", "?")
                    insert_failures.append((url, err))
        for _ in batch:
            output_queue.task_done()


def scrape_dealer(dealer: DealerConfig, translate_to_english: bool = True) -> Dict[str, Any]:
    """Scrape a single dealer's listings with parallel scrapers + consumers."""
    logger.info("=" * 80)
    logger.info(f"SCRAPING {dealer.name.upper()} ({dealer.business_id})")
    logger.info(f"Translation: {'ENABLED' if translate_to_english else 'DISABLED'}")
    num_scrapers = settings.num_scraper_workers
    num_consumers = settings.num_consumer_workers
    batch_size = settings.consumer_batch_size
    logger.info(f"Workers: {num_scrapers} scrapers, {num_consumers} consumers (batch={batch_size})")
    logger.info("=" * 80)

    empty_result: Dict[str, Any] = {
        "dealer": dealer.name,
        "business_id": dealer.business_id,
        "total": 0,
        "saved": 0,
        "scrape_failures": [],
        "insert_failures": [],
    }

    try:
        # Phase 1: One scraper discovers all URLs
        with BlocketScraper(
            dealer.business_id,
            translate_to_english=False,
        ) as scraper:
            urls = scraper.get_listing_urls(dealer.url)
            logger.info(f"Found {len(urls)} listings")

        if not urls:
            logger.warning(f"No listings found for {dealer.name}")
            return empty_result

        # Phase 2: Skip already-scraped
        existing = mongo_client.get_existing_source_urls(dealer.business_id)
        new_urls = [u for u in urls if u not in existing]
        skipped = len(urls) - len(new_urls)
        if skipped:
            logger.info(f"Skipping {skipped} already-scraped, {len(new_urls)} new")
        if not new_urls:
            logger.info("All listings already scraped")
            return empty_result

        # Phase 3: Producer-consumer pipeline
        url_queue: queue.Queue = queue.Queue()
        output_queue: queue.Queue = queue.Queue()
        scrape_failures: List[Tuple[str, str]] = []
        insert_failures: List[Tuple[str, str]] = []
        failures_lock = threading.Lock()

        for url in new_urls:
            url_queue.put(url)
        for _ in range(num_scrapers):
            url_queue.put(SENTINEL)

        saved_counter = [0]
        counter_lock = threading.Lock()

        scrapers = [
            threading.Thread(
                target=_scraper_worker,
                args=(i, url_queue, output_queue, dealer, scrape_failures, failures_lock),
            )
            for i in range(num_scrapers)
        ]
        consumers = [
            threading.Thread(
                target=_consumer_worker,
                args=(
                    i,
                    output_queue,
                    translate_to_english,
                    saved_counter,
                    counter_lock,
                    batch_size,
                    insert_failures,
                    failures_lock,
                ),
            )
            for i in range(num_consumers)
        ]

        for t in scrapers + consumers:
            t.start()

        url_queue.join()

        for _ in range(num_consumers):
            output_queue.put(SENTINEL)

        for t in consumers:
            t.join()

        for t in scrapers:
            t.join()

        saved = saved_counter[0]
        logger.success(f"Completed: {len(new_urls)} processed for {dealer.name}")
        return {
            "dealer": dealer.name,
            "business_id": dealer.business_id,
            "total": len(new_urls),
            "saved": saved,
            "scrape_failures": list(scrape_failures),
            "insert_failures": list(insert_failures),
        }

    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        import traceback
        traceback.print_exc()
        return empty_result


def _print_summary(results: List[Dict[str, Any]], initial_count: int, final_count: int) -> None:
    """Print end-of-run summary with per-dealer stats and failed ads."""
    total_saved = sum(r["saved"] for r in results)
    total_attempted = sum(r["total"] for r in results)

    logger.info("")
    logger.info("=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)

    for i, r in enumerate(results, 1):
        dealer_name = r["dealer"]
        total = r["total"]
        saved = r["saved"]
        scrape_failures = r.get("scrape_failures", [])
        insert_failures = r.get("insert_failures", [])

        if total == 0:
            logger.info(f"Dealer {i}: {dealer_name} - 0/0 ads (no new listings)")
            continue

        status = f"{saved}/{total} ads"
        if scrape_failures or insert_failures:
            failed_count = len(scrape_failures) + len(insert_failures)
            status += f" ({failed_count} failed)"
        logger.info(f"Dealer {i}: {dealer_name} - {status}")

        if scrape_failures:
            logger.info(f"  Scrape failures ({len(scrape_failures)}):")
            for url, err in scrape_failures[:10]:  # limit to 10
                short_url = url[:80] + "..." if len(url) > 80 else url
                logger.info(f"    - {short_url}")
                logger.info(f"      Reason: {err}")
            if len(scrape_failures) > 10:
                logger.info(f"    ... and {len(scrape_failures) - 10} more")

        if insert_failures:
            logger.info(f"  Insert failures ({len(insert_failures)}):")
            for url, err in insert_failures[:10]:
                short_url = url[:80] + "..." if len(url) > 80 else url
                logger.info(f"    - {short_url}")
                logger.info(f"      Reason: {err}")
            if len(insert_failures) > 10:
                logger.info(f"    ... and {len(insert_failures) - 10} more")

    logger.info("")
    logger.info(f"Total: {total_saved}/{total_attempted} ads saved")
    logger.info(f"MongoDB: {initial_count} -> {final_count}")
    logger.info("=" * 80)


def scrape_all(translate_to_english: bool = True) -> None:
    """Scrape all configured dealers."""
    logger.info("Connecting to MongoDB...")
    if not mongo_client.connect():
        logger.error("MongoDB connection failed")
        return

    dealers = load_dealers()
    logger.info("=" * 80)
    logger.info(f"STARTING SCRAPE FOR {len(dealers)} DEALERS")
    logger.info(f"Translation to English: {'ENABLED' if translate_to_english else 'DISABLED'}")
    logger.info("=" * 80)

    results: List[Dict[str, Any]] = []
    initial_count = mongo_client.count_listings()

    try:
        for dealer in dealers:
            result = scrape_dealer(dealer, translate_to_english=translate_to_english)
            results.append(result)

        final_count = mongo_client.count_listings()
        _print_summary(results, initial_count, final_count)
    finally:
        mongo_client.close()


def main() -> None:
    """Entry point for CLI."""
    settings.ensure_directories()
    logger.add(
        settings.base_dir / settings.log_dir / "scraper_{time}.log",
        rotation="1 day",
        retention="7 days",
        level=settings.log_level,
    )

    translate = True
    if len(sys.argv) > 1 and sys.argv[1] == "--no-translate":
        translate = False
        logger.info("Translation disabled - will keep Swedish keys")

    scrape_all(translate_to_english=translate)


if __name__ == "__main__":
    main()
