#!/usr/bin/env python
"""Operator tool: seal existing on-disk joblib artifacts with SHA256 manifests."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from app.config.settings import get_settings
from app.training.artifacts import reseal_existing_artifact


def main() -> None:
    settings = get_settings()
    root = settings.duckdb_path.parent / "models"
    found = list(root.glob("**/*.joblib"))
    if not found:
        print(f"No artifacts found under {root}")
        return
    for p in found:
        reseal_existing_artifact(p)
        print(f"sealed {p}")
    print(f"Done: {len(found)} artifact(s) sealed.")


if __name__ == "__main__":
    main()
