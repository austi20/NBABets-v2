from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config.settings import get_settings  # noqa: E402
from app.server.services.board_cache import BoardCache  # noqa: E402
from app.trading.decision_brain import sync_decision_brain  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync the Obsidian Kalshi Decision Brain into runtime artifacts.")
    parser.add_argument("--date", dest="board_date", type=date.fromisoformat, default=None)
    parser.add_argument("--mode", choices=("observe", "supervised-live"), default="observe")
    parser.add_argument("--candidate-limit", type=int, default=None)
    parser.add_argument("--skip-resolve", action="store_true", help="Use the existing symbol map instead of resolving markets.")
    parser.add_argument("--skip-pack", action="store_true", help="Do not build data/decisions/decisions.json.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    settings = get_settings()
    board_entry = BoardCache().get_or_build(args.board_date)
    result = sync_decision_brain(
        settings=settings,
        board_entry=board_entry,
        board_date=args.board_date or board_entry.board_date,
        mode=args.mode,
        candidate_limit=args.candidate_limit,
        resolve_markets=not args.skip_resolve,
        build_pack=not args.skip_pack,
    )
    print(json.dumps(result.to_payload(), indent=2))
    return 0 if result.state in {"synced", "observe_only"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
