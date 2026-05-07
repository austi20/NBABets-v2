"""Phase 8: run legacy-vs-rotation-shock inference for today's slate."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config.settings import get_settings  # noqa: E402
from app.db.bootstrap import create_all  # noqa: E402
from app.db.session import session_scope  # noqa: E402
from app.services.rotation_shadow_compare import compare_legacy_and_rotation  # noqa: E402
from app.training.pipeline import TrainingPipeline  # noqa: E402


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Legacy vs rotation-shock shadow inference compare.")
    p.add_argument(
        "--target-date",
        type=lambda s: date.fromisoformat(s),
        default=None,
        help="Slate date (ISO). Default: today (UTC local date).",
    )
    p.add_argument(
        "--persist-legacy",
        action="store_true",
        help="Persist legacy predictions instead of running compare-only. Use only during rollback.",
    )
    p.add_argument(
        "--no-persist",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--no-rolling",
        action="store_true",
        help="Skip appending rolling_overlap_summary.csv.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    target_date = args.target_date or date.today()
    settings = get_settings()

    create_all()
    with session_scope() as session:
        pipe = TrainingPipeline(session)
        snap = compare_legacy_and_rotation(
            pipeline=pipe,
            report_date=target_date,
            persist_authoritative_legacy=bool(args.persist_legacy and not args.no_persist),
            append_rolling_summary=not args.no_rolling,
            reports_root=settings.reports_dir,
        )
    print("\n".join(snap.markdown_lines))
    print(f"Run directory: {snap.run_dir}")


if __name__ == "__main__":
    main()
