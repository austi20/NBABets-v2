"""Invalidate same-day startup outputs (and optionally ingest + artifacts).

Soft reset: predictions, backtests, model runs, report files for the target UTC day.
Hard reset: same, plus board-date line snapshots, raw payloads, provider cache
day-scoped entries, and on-disk model artifacts (see ``StartupCacheResetService``).

Usage::

    python scripts/reset_startup_day.py
    python scripts/reset_startup_day.py --hard
    python scripts/reset_startup_day.py --target-date 2026-05-07 --board-date 2026-05-07 --hard
"""

from __future__ import annotations

import argparse
from datetime import date

from app.db.session import session_scope
from app.services.startup_cache import StartupCacheResetService


def main() -> None:
    parser = argparse.ArgumentParser(description="Clear startup / training caches for a calendar day.")
    parser.add_argument(
        "--hard",
        action="store_true",
        help="Also wipe board lines, raw payloads, provider day cache, and artifact files",
    )
    parser.add_argument(
        "--target-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="UTC day for model_run / prediction / backtest deletes (default: today)",
    )
    parser.add_argument(
        "--board-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Board date for hard-reset game-scoped deletes (default: same as --target-date)",
    )
    args = parser.parse_args()

    tgt = date.fromisoformat(args.target_date) if args.target_date else date.today()
    board = date.fromisoformat(args.board_date) if args.board_date else tgt

    with session_scope() as session:
        svc = StartupCacheResetService(session)
        res = svc.hard_reset(target_date=tgt, board_date=board) if args.hard else svc.soft_reset(target_date=tgt, board_date=board)

    print(f"mode={res.mode} target_date={res.target_date} board_date={res.board_date}")
    print(
        f"deleted: predictions={res.deleted_predictions} backtests={res.deleted_backtests} "
        f"model_runs={res.deleted_model_runs} reports={res.deleted_reports}"
    )
    if args.hard:
        print(
            f"hard: line_snapshots={res.deleted_line_snapshots} raw_payloads={res.deleted_raw_payloads} "
            f"injury_reports={res.deleted_injury_reports} game_availability={res.deleted_game_availability} "
            f"provider_fetches={res.deleted_provider_cached_fetches} artifact_files={len(res.deleted_artifacts)}"
        )


if __name__ == "__main__":
    main()
