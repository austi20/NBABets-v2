"""Write ``data/decisions/decisions.json`` from ``config/kalshi_symbols.json`` + live quotes."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config.settings import get_settings  # noqa: E402
from app.trading.live_pack_builder import LivePackBuildError, write_live_decision_pack  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--observe-only",
        action="store_true",
        help="Write an observe pack even when gates fail (never arms live submit).",
    )
    args = parser.parse_args()
    settings = get_settings()
    path = Path(settings.kalshi_decisions_path)
    try:
        write_live_decision_pack(
            decisions_path=path,
            settings=settings,
            arm_live=not args.observe_only,
        )
    except LivePackBuildError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 2
    print(f"OK: wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
