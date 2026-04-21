"""Configuration module."""

from .settings import settings
from .dealers import load_dealers, DealerConfig

__all__ = ["settings", "load_dealers", "DealerConfig"]
