#!/usr/bin/env python3
"""Run ad-extractor without installing the package."""

import sys
from pathlib import Path

# Add src to path so ad_extractor can be imported
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from ad_extractor.cli import main

if __name__ == "__main__":
    main()
