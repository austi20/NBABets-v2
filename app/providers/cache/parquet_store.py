from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_parquet(path: Path | str) -> pd.DataFrame:
    return pd.read_parquet(path)


def write_parquet(df: pd.DataFrame, path: Path | str) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="snappy", index=False)


def partition_by_season(
    df: pd.DataFrame,
    output_dir: Path | str,
    season_col: str = "season",
) -> None:
    output_dir = Path(output_dir)
    for season, group in df.groupby(season_col):
        partition_path = output_dir / f"season={season}" / "part-0.parquet"
        write_parquet(group.reset_index(drop=True), partition_path)
