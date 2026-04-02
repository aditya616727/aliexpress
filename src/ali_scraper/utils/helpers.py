"""Shared utility functions."""

import time
import random
import logging

from ..config import settings

logger = logging.getLogger(__name__)


def rate_limit():
    """Sleep a random interval to avoid detection."""
    delay = random.uniform(settings.delay_min, settings.delay_max)
    logger.debug(f"Rate limit: sleeping {delay:.1f}s")
    time.sleep(delay)
