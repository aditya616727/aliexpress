"""Helper utilities for scraping and HTTP."""

import os
import random
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
import re
import time
from pathlib import Path

import requests
from loguru import logger
from PIL import Image

# Request delay (seconds) to avoid detection
REQUEST_DELAY_MIN = 3
REQUEST_DELAY_MAX = 6

# Cloudflare config
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID")
CLOUDFLARE_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN")
CLOUDFLARE_IMAGES_URL = (
    f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/images/v1"
)
CLOUDFLARE_UPLOAD_WORKERS = 4

IMAGE_TEMP_DIR = Path.cwd() / "config" / "images_temp"
IMAGE_TEMP_DIR.mkdir(parents=True, exist_ok=True)


def rate_limit() -> None:
    """Wait a random amount of time between requests to avoid detection."""
    delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
    logger.debug(f"Rate limiting: sleeping for {delay:.2f} seconds")
    time.sleep(delay)


def download_image(image_url: str, save_path: str) -> bool:
    """Download an image from URL and save it locally. Validates it's a valid image."""
    try:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        response = requests.get(image_url, timeout=30, stream=True)
        response.raise_for_status()

        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        try:
            img = Image.open(save_path)
            img.verify()
            logger.info(f"Downloaded image: {save_path}")
            return True
        except Exception as e:
            logger.error(f"Invalid image file: {e}")
            Path(save_path).unlink(missing_ok=True)
            return False

    except Exception as e:
        logger.error(f"Failed to download image {image_url}: {e}")
        return False


def clean_price(price_text: str) -> Optional[str]:
    """Extract just the numbers from a price string."""
    if not price_text:
        return None
    cleaned = re.sub(r"[^\d,.]", "", str(price_text))
    cleaned = cleaned.replace(",", "").replace(" ", "")
    return cleaned


def detect_site(url: str) -> str:
    """Return which site a URL belongs to."""
    if "blocket.se" in url:
        return "blocket"
    if "olx" in url:
        return "olx"
    if "autoscout" in url:
        return "autoscout"
    return "unknown"


def get_user_agent() -> str:
    """Get a random user agent string."""
    from ..config.settings import USER_AGENTS
    return random.choice(USER_AGENTS)


def _cf_download_image(img_url: str, index: int) -> Optional[Path]:
    """Download a single image to a temp file. Returns local Path or None."""
    try:
        response = requests.get(img_url, timeout=10)
        if not response.ok:
            logger.warning(f"Could not download image {index + 1}: HTTP {response.status_code}")
            return None
        local_path = IMAGE_TEMP_DIR / f"{uuid.uuid4().hex}.jpg"
        local_path.write_bytes(response.content)
        return local_path
    except Exception as e:
        logger.warning(f"Download error for image {index + 1}: {e}")
        return None


def _cf_upload_image(local_path: Path, index: int) -> Optional[str]:
    """Upload a local image to Cloudflare Images. Deletes local file after. Returns CF URL or None."""
    try:
        with open(local_path, "rb") as f:
            cf_response = requests.post(
                CLOUDFLARE_IMAGES_URL,
                headers={"Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}"},
                files={"file": (local_path.name, f, "image/jpeg")},
                timeout=30,
            )
        cf_response.raise_for_status()
        data = cf_response.json()
        if data.get("success"):
            variants = data["result"]["variants"]
            return variants[0] if variants else None
        else:
            logger.warning(f"Cloudflare upload failed for image {index + 1}: {data.get('errors')}")
            return None
    except Exception as e:
        logger.warning(f"Upload error for image {index + 1}: {e}")
        return None
    finally:
        try:
            local_path.unlink()
        except Exception:
            pass


def _cf_process_image(args: Tuple[str, int]) -> Optional[str]:
    """Worker: download → upload to Cloudflare → delete local. Returns CF URL or None."""
    img_url, index = args
    local_path = _cf_download_image(img_url, index)
    if local_path is None:
        return None
    return _cf_upload_image(local_path, index)


def upload_images_to_cloudflare(raw_urls: List[str]) -> List[str]:
    """
    Download images from raw_urls, upload to Cloudflare, return CF URLs.
    Uses a thread pool for parallel processing.
    """
    if not raw_urls:
        return []

    logger.info(f"Uploading {len(raw_urls)} images to Cloudflare...")
    cf_urls: List[Optional[str]] = [None] * len(raw_urls)

    with ThreadPoolExecutor(max_workers=CLOUDFLARE_UPLOAD_WORKERS) as executor:
        future_to_index = {
            executor.submit(_cf_process_image, (url, i)): i
            for i, url in enumerate(raw_urls)
        }
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                cf_urls[idx] = future.result()
            except Exception as e:
                logger.warning(f"Worker failed for image {idx + 1}: {e}")

    result = [url for url in cf_urls if url is not None]
    logger.info(f"Uploaded {len(result)}/{len(raw_urls)} images to Cloudflare")
    return result
