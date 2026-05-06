"""Backfill historical box scores from nba_api into parquet partitions.

Usage:
    python scripts/backfill_historical_box_scores.py --seasons 2024
    python scripts/backfill_historical_box_scores.py --seasons 1996-2025
    python scripts/backfill_historical_box_scores.py  # all seasons 1996-2024
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:  # noqa: E402
    sys.path.insert(0, str(ROOT))

from app.providers.cache.parquet_store import partition_by_season  # noqa: E402
from app.providers.historical.nba_api_loader import fetch_player_game_logs  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

PARQUET_ROOT = ROOT / "data" / "parquet" / "box_scores"
DEFAULT_START = 1996
DEFAULT_END = 2024


def _parse_seasons(raw: str | None) -> list[str]:
    if raw is None:
        return [str(y) for y in range(DEFAULT_START, DEFAULT_END + 1)]
    if "-" in raw and len(raw) > 5:
        # Range like "1996-2025"
        parts = raw.split("-")
        start, end = int(parts[0]), int(parts[1])
        return [str(y) for y in range(start, end + 1)]
    # Comma-separated or single value
    return [s.strip() for s in raw.split(",") if s.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historical box scores to parquet")
    parser.add_argument(
        "--seasons",
        default=None,
        help="Season start year(s): single '2024', range '1996-2025', or comma-list '2022,2023'",
    )
    parser.add_argument(
        "--out",
        default=str(PARQUET_ROOT),
        help="Output directory (default: data/parquet/box_scores/)",
    )
    args = parser.parse_args()

    seasons = _parse_seasons(args.seasons)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Backfilling %d season(s): %s -> %s", len(seasons), seasons[0], seasons[-1])

    for season in seasons:
        partition_dir = out_dir / f"season={season}"
        part_file = partition_dir / "part-0.parquet"
        if part_file.exists():
            logger.info("Season %s already on disk - skipping", season)
            continue

        logger.info("Fetching season %s ...", season)
        df = fetch_player_game_logs(season)

        if df.empty:
            logger.warning("Season %s returned no data", season)
            continue

        df["season"] = season
        partition_by_season(df, out_dir, season_col="season")
        logger.info("Season %s: %d rows written to %s", season, len(df), partition_dir)

    logger.info("Done.")


if __name__ == "__main__":
    main()
