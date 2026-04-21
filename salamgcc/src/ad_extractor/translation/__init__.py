"""Translation module - Swedish to English for car listings."""

from .dictionaries import (
    SWEDISH_TO_ENGLISH_SPECS,
    VALUE_TRANSLATIONS,
    EQUIPMENT_TRANSLATIONS,
)
from .service import BatchTranslator, translate_listings

__all__ = [
    "SWEDISH_TO_ENGLISH_SPECS",
    "VALUE_TRANSLATIONS",
    "EQUIPMENT_TRANSLATIONS",
    "BatchTranslator",
    "translate_listings",
]
