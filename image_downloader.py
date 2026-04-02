import os
import re
import time
import random
import logging
import hashlib
from urllib.parse import urlparse

import requests

from config import DEFAULT_IMAGE_DIR, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)


class ImageDownloader:
    """Download product images from URLs."""

    def __init__(self, output_dir=None):
        self.output_dir = output_dir or DEFAULT_IMAGE_DIR
        os.makedirs(self.output_dir, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

    def _sanitize_filename(self, name, max_length=80):
        """Create a safe filename from a string."""
        name = re.sub(r'[<>:"/\\|?*]', "", name)
        name = re.sub(r"\s+", "_", name.strip())
        name = re.sub(r"[^\w\-.]", "", name)
        if len(name) > max_length:
            name = name[:max_length]
        return name

    def _get_extension(self, url, content_type=None):
        """Determine image extension from URL or content type."""
        parsed = urlparse(url)
        path = parsed.path.lower()

        for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"]:
            if path.endswith(ext):
                return ext

        if content_type:
            ct = content_type.lower()
            if "jpeg" in ct or "jpg" in ct:
                return ".jpg"
            elif "png" in ct:
                return ".png"
            elif "webp" in ct:
                return ".webp"
            elif "gif" in ct:
                return ".gif"

        return ".jpg"

    def download_image(self, url, product_title="", index=0):
        """Download a single image.

        Args:
            url: Image URL
            product_title: Product title for filename
            index: Product index for unique naming

        Returns:
            Path to downloaded image, or empty string on failure
        """
        if not url or not url.startswith("http"):
            logger.warning(f"Invalid image URL: {url}")
            return ""

        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT, stream=True)
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "")
            ext = self._get_extension(url, content_type)

            if product_title:
                safe_title = self._sanitize_filename(product_title)
                filename = f"{index:04d}_{safe_title}{ext}"
            else:
                url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
                filename = f"{index:04d}_{url_hash}{ext}"

            filepath = os.path.join(self.output_dir, filename)

            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            file_size = os.path.getsize(filepath)
            logger.info(f"Downloaded image ({file_size} bytes): {filename}")
            return filepath

        except requests.RequestException as e:
            logger.warning(f"Failed to download image {url}: {e}")
            return ""
        except IOError as e:
            logger.error(f"Failed to save image: {e}")
            return ""

    def download_all(self, products, delay=0.5):
        """Download images for all products.

        Args:
            products: List of product dicts (must have 'image_url' key)
            delay: Delay between downloads in seconds

        Returns:
            Number of successfully downloaded images
        """
        downloaded = 0
        total = len(products)

        for i, product in enumerate(products):
            image_url = product.get("image_url", "")
            title = product.get("title", "")

            if not image_url:
                logger.debug(f"No image URL for product {i}: {title[:50]}")
                continue

            filepath = self.download_image(image_url, title, index=i)
            if filepath:
                product["image_path"] = filepath
                downloaded += 1
            else:
                product["image_path"] = ""

            if i < total - 1 and delay > 0:
                time.sleep(random.uniform(delay * 0.5, delay * 1.5))

        logger.info(f"Downloaded {downloaded}/{total} images")
        return downloaded
