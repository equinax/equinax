"""
Data Management Module

This module provides unified data management for the quant system:
- downloads/  - Scripts to download data from external sources (AKShare, BaoStock)
- cache/      - SQLite cache for downloaded data (~3GB, git-ignored)
- fixtures/   - Small sample data for development (~5MB, git-tracked)
"""

from pathlib import Path

# Directory paths
DATA_DIR = Path(__file__).parent
DOWNLOADS_DIR = DATA_DIR / "downloads"
CACHE_DIR = DATA_DIR / "cache"
FIXTURES_DIR = DATA_DIR / "fixtures"

# Ensure directories exist
CACHE_DIR.mkdir(exist_ok=True)
FIXTURES_DIR.mkdir(exist_ok=True)

__all__ = ["DATA_DIR", "DOWNLOADS_DIR", "CACHE_DIR", "FIXTURES_DIR"]
