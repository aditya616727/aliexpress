"""CSV and JSON data exporter."""

import csv
import json
import os
import logging

from ..config import settings

logger = logging.getLogger(__name__)


class DataExporter:
    """Export scraped product data to CSV and JSON formats."""

    def __init__(self, output_dir=None):
        self.output_dir = output_dir or str(settings.default_output_dir)
        os.makedirs(self.output_dir, exist_ok=True)

    def export_to_csv(self, products, filename=None):
        """Export products to a CSV file.

        Returns:
            Path to the created CSV file
        """
        filepath = os.path.join(self.output_dir, filename or settings.csv_filename)

        try:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f, fieldnames=settings.product_fields, extrasaction="ignore"
                )
                writer.writeheader()
                for product in products:
                    writer.writerow(product)

            logger.info(f"Exported {len(products)} products to CSV: {filepath}")
            return filepath
        except IOError as e:
            logger.error(f"Failed to export CSV: {e}")
            raise

    def export_to_json(self, products, filename=None):
        """Export products to a JSON file.

        Returns:
            Path to the created JSON file
        """
        filepath = os.path.join(self.output_dir, filename or settings.json_filename)

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "total_products": len(products),
                        "products": products,
                    },
                    f,
                    indent=2,
                    ensure_ascii=False,
                )

            logger.info(f"Exported {len(products)} products to JSON: {filepath}")
            return filepath
        except IOError as e:
            logger.error(f"Failed to export JSON: {e}")
            raise

    def export_all(self, products):
        """Export products to both CSV and JSON.

        Returns:
            Tuple of (csv_path, json_path)
        """
        csv_path = self.export_to_csv(products)
        json_path = self.export_to_json(products)
        return csv_path, json_path
