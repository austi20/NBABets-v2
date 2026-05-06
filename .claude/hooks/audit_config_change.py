#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path


def _project_root() -> Path:
    project_dir = os.environ.get("CLAUDE_PROJECT_DIR")
    if project_dir:
        return Path(project_dir).resolve()
    return Path(__file__).resolve().parents[2]


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0

    root = _project_root()
    log_dir = root / ".claude" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "config_changes.log"

    timestamp = datetime.now(UTC).isoformat()
    source = payload.get("source", "unknown")
    file_path = payload.get("file_path", "")
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{timestamp}\t{source}\t{file_path}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
