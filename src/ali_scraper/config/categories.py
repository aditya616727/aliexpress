"""Category configuration loader (analogous to dealers.py in ad-extractor)."""

import logging
from dataclasses import dataclass
from pathlib import Path

import yaml

from .settings import settings

logger = logging.getLogger(__name__)


@dataclass
class CategoryConfig:
    """A search category to scrape."""
    category_id: str
    name: str
    query: str
    pages: int = 1


# Built-in default categories (used when no YAML is provided)
DEFAULT_CATEGORIES = [
    CategoryConfig(
        category_id="electronics",
        name="Electronics & Gadgets",
        query="wireless earbuds",
        pages=2,
    ),
]


def load_categories() -> list[CategoryConfig]:
    """Load categories from config/categories.yaml, falling back to defaults."""
    yaml_path = settings.config_dir / "categories.yaml"

    if yaml_path.exists():
        try:
            with open(yaml_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            categories = []
            for item in data.get("categories", []):
                categories.append(CategoryConfig(
                    category_id=item["category_id"],
                    name=item["name"],
                    query=item["query"],
                    pages=item.get("pages", 1),
                ))

            logger.info(f"Loaded {len(categories)} categories from {yaml_path}")
            return categories

        except Exception as e:
            logger.warning(f"Failed to load {yaml_path}: {e}, using defaults")

    logger.info(f"Using {len(DEFAULT_CATEGORIES)} built-in categories")
    return list(DEFAULT_CATEGORIES)
