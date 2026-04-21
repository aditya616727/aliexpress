#!/usr/bin/env python3
"""Convenience script to run the scraper - delegates to ad_extractor CLI."""

import os
import sys

# Add project root and src to path for development
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _project_root)
sys.path.insert(0, os.path.join(_project_root, "src"))

from ad_extractor.cli import main

if __name__ == "__main__":
    main()
