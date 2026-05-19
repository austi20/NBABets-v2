"""Wipe the feature cache directory.

Usage: python scripts/clear_feature_cache.py [--yes]
"""
from __future__ import annotations

import argparse
import shutil
import sys

from app.config.settings import get_settings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", action="store_true", help="Skip confirmation")
    args = parser.parse_args()

    cache_dir = get_settings().feature_cache_dir
    if not cache_dir.exists():
        print(f"Cache directory does not exist: {cache_dir}")
        return 0

    if not args.yes:
        reply = input(f"Delete {cache_dir}? [y/N] ").strip().lower()
        if reply != "y":
            print("Aborted.")
            return 1

    shutil.rmtree(cache_dir)
    print(f"Removed {cache_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
