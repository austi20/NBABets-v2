from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.evaluation.synthetic_surrogate import generate_surrogate_lines  # noqa: E402
from app.providers.cache.parquet_store import partition_by_season  # noqa: E402

logger = logging.getLogger(__name__)

BOX_SCORE_ROOT = ROOT / "data" / "parquet" / "box_scores"
SURROGATE_ROOT = ROOT / "data" / "parquet" / "odds_synthetic"


def _parse_seasons(raw: str | None) -> list[int]:
    if raw is None or not raw.strip():
        return []
    return [int(part.strip()) for part in raw.split(",") if part.strip()]


def _load_box_scores(root: Path, seasons: list[int]) -> pd.DataFrame:
    files: list[Path] = []
    for season in seasons:
        files.extend(sorted((root / f"season={season}").glob("*.parquet")))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build synthetic surrogate lines from historical box scores.")
    parser.add_argument("--seasons", required=True, help="Comma-separated seasons, e.g. 2023,2024")
    parser.add_argument("--trailing-games", type=int, default=4, help="Trailing game count for synthetic line generation.")
    parser.add_argument("--box-score-root", default=str(BOX_SCORE_ROOT), help="Root parquet directory for box scores.")
    parser.add_argument("--out", default=str(SURROGATE_ROOT), help="Root parquet directory for synthetic surrogate lines.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    seasons = _parse_seasons(args.seasons)
    if not seasons:
        raise SystemExit("At least one season is required.")

    box_score_root = Path(args.box_score_root)
    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    logger.info("Loading box-score partitions for seasons: %s", ", ".join(str(s) for s in seasons))
    box_scores = _load_box_scores(box_score_root, seasons)
    if box_scores.empty:
        raise SystemExit(f"No box-score parquet files found under {box_score_root} for seasons {seasons}.")

    logger.info("Generating synthetic surrogate lines (trailing_games=%d)", args.trailing_games)
    surrogate = generate_surrogate_lines(box_scores, trailing_games=max(1, int(args.trailing_games)))
    if surrogate.empty:
        raise SystemExit("Synthetic surrogate generation returned 0 rows.")

    partition_by_season(surrogate, out_root, season_col="season")
    logger.info("Wrote %d surrogate rows to %s", len(surrogate), out_root)


if __name__ == "__main__":
    main()
