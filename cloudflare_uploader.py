"""Upload images to Cloudflare Images and return public URLs."""

import os
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, MAX_CONCURRENT_UPLOADS

logger = logging.getLogger(__name__)

CLOUDFLARE_UPLOAD_URL = (
    "https://api.cloudflare.com/client/v4/accounts/{account_id}/images/v1"
)


class CloudflareUploader:
    """Upload images to Cloudflare Images API."""

    def __init__(self, account_id=None, api_token=None):
        self.account_id = account_id or CLOUDFLARE_ACCOUNT_ID
        self.api_token = api_token or CLOUDFLARE_API_TOKEN
        self.upload_url = CLOUDFLARE_UPLOAD_URL.format(account_id=self.account_id)
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
        }

        if not self.account_id or not self.api_token:
            raise ValueError(
                "Cloudflare account ID and API token are required. "
                "Set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN in .env"
            )

    def upload_image(self, image_path, product_title=""):
        """Upload a single image to Cloudflare Images.

        Args:
            image_path: Local path to the image file
            product_title: Optional title for metadata

        Returns:
            Cloudflare image URL string, or empty string on failure
        """
        if not image_path or not os.path.exists(image_path):
            logger.warning(f"Image file not found: {image_path}")
            return ""

        try:
            filename = os.path.basename(image_path)
            with open(image_path, "rb") as f:
                files = {"file": (filename, f)}
                data = {}
                if product_title:
                    data["metadata"] = f'{{"title": "{product_title}"}}'

                response = requests.post(
                    self.upload_url,
                    headers=self.headers,
                    files=files,
                    data=data,
                    timeout=30,
                )

            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    variants = result["result"].get("variants", [])
                    if variants:
                        image_url = variants[0]
                        logger.info(f"Uploaded to Cloudflare: {filename} -> {image_url}")
                        return image_url

                errors = result.get("errors", [])
                logger.error(f"Cloudflare upload failed: {errors}")
                return ""
            else:
                logger.error(
                    f"Cloudflare upload HTTP {response.status_code}: {response.text[:200]}"
                )
                return ""

        except requests.RequestException as e:
            logger.error(f"Cloudflare upload request failed for {image_path}: {e}")
            return ""
        except Exception as e:
            logger.error(f"Unexpected error uploading {image_path}: {e}")
            return ""

    def upload_all(self, products, delete_local=True):
        """Upload all product images to Cloudflare and update product dicts.

        Args:
            products: List of product dicts with 'image_path' keys
            delete_local: Whether to delete local image files after upload

        Returns:
            Number of successfully uploaded images
        """
        uploaded = 0
        tasks = []

        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_UPLOADS) as executor:
            for i, product in enumerate(products):
                image_path = product.get("image_path", "")
                if not image_path or not os.path.exists(image_path):
                    continue

                future = executor.submit(
                    self.upload_image,
                    image_path,
                    product.get("title", ""),
                )
                tasks.append((future, i, image_path))

            for future, idx, local_path in tasks:
                try:
                    cloudflare_url = future.result(timeout=60)
                    if cloudflare_url:
                        products[idx]["images"] = [cloudflare_url]
                        uploaded += 1

                        if delete_local and os.path.exists(local_path):
                            os.remove(local_path)
                            logger.debug(f"Deleted local image: {local_path}")
                    else:
                        products[idx]["images"] = []
                except Exception as e:
                    logger.error(f"Upload task failed for product {idx}: {e}")
                    products[idx]["images"] = []

        logger.info(f"Uploaded {uploaded}/{len(products)} images to Cloudflare")
        return uploaded
