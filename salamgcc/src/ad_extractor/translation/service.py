"""Batch translation service for Swedish-to-English using dictionary lookups only."""

import time
from typing import Any, Dict, List

from loguru import logger

from .dictionaries import VALUE_TRANSLATIONS

# Fields we set ourselves or use codes - never translate
DO_NOT_TRANSLATE_KEYS = frozenset({
    "city", "address", "country", "regionalSpecs",
    "driveTerrain",  # must stay 4WD, 2WD, FWD, RWD
    "engineCapacityCc", "horsepower", "kilometers",  # fixed formats
})


class BatchTranslator:
    """
    Dictionary-based translator. No external API calls.
    Uses VALUE_TRANSLATIONS for known Swedish terms; unknown values are left as-is.
    """

    def __init__(self, source: str = "sv", target: str = "en", batch_size: int = 50) -> None:
        self.source = source
        self.target = target
        self.batch_size = batch_size
        logger.debug("Using dictionary-only translation (no external API)")

    def _translate_one(self, text: str) -> str:
        if not text or len(text) < 2:
            return text
        return VALUE_TRANSLATIONS.get(text, text)

    def translate_batch_sync(self, texts: List[str]) -> List[str]:
        return [self._translate_one(t) for t in texts]

    async def translate_batch_async(self, texts: List[str]) -> List[str]:
        return self.translate_batch_sync(texts)

    def translate_listing_batch(self, listings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not listings:
            return []

        logger.debug(f"Starting batch translation for {len(listings)} listings...")
        start_time = time.time()
        count = 0

        for listing in listings:
            post_ad_data = listing.get("postAdData", {})
            for key, value in post_ad_data.items():
                if key in DO_NOT_TRANSLATE_KEYS:
                    continue
                if isinstance(value, str) and value in VALUE_TRANSLATIONS:
                    post_ad_data[key] = VALUE_TRANSLATIONS[value]
                    count += 1

        elapsed = time.time() - start_time
        logger.debug(f"Translation complete in {elapsed:.2f}s ({count} values translated)")
        return listings

    async def translate_listing_batch_async(self, listings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self.translate_listing_batch(listings)


def translate_listings(
    listings: List[Dict[str, Any]],
    use_async: bool = True,
) -> List[Dict[str, Any]]:
    """Translate listings using dictionary lookups only (no external API)."""
    if not listings:
        return []
    translator = BatchTranslator(source="sv", target="en")
    return translator.translate_listing_batch(listings)
