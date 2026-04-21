#!/usr/bin/env python3
"""
Scrape Erik Olsson brokers and store them in MongoDB (collection: brokers).

Requires:
- MONGODB_URI (or MONGODB_URI_DEV/PROD depending on NODE_ENV)
- CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN (for profileImage upload)
"""

import sys
from pathlib import Path

# Add src to path so ad_extractor can be imported when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ad_extractor.config.brokers import scrape_brokers


def main() -> None:
    limit = None
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        if idx + 1 < len(sys.argv):
            try:
                limit = int(sys.argv[idx + 1])
            except ValueError:
                limit = None

    brokers = scrape_brokers(store_in_mongodb=True, limit=limit)
    print(f"\nDone. Scraped {len(brokers)} brokers.")


if __name__ == "__main__":
    main()

