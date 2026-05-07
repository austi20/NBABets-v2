"""Attach finalized box-score stats to Phase 8 `rotation_shadow_overlap.csv` outputs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import bindparam, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.bootstrap import create_all  # noqa: E402
from app.db.session import session_scope  # noqa: E402


def _stat_actual(row: pd.Series, market_key: str) -> float:
    if market_key == "pra":
        values = pd.to_numeric(row[["points", "rebounds", "assists"]], errors="coerce")
        return float(values.sum()) if values.notna().all() else float("nan")
    key = {"points": "points", "rebounds": "rebounds", "assists": "assists", "threes": "threes", "turnovers": "turnovers"}
    column = key.get(market_key)
    if column is None:
        return float("nan")
    value = pd.to_numeric(pd.Series([row.get(column)]), errors="coerce").iloc[0]
    return float(value) if pd.notna(value) else float("nan")


def _load_box_scores(session, game_ids: list[int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not game_ids:
        stats = pd.DataFrame(columns=["game_id", "player_id", "points", "rebounds", "assists", "threes", "turnovers"])
        games = pd.DataFrame(columns=["game_id", "game_status"])
        return stats, games
    engine = session.bind
    stats = pd.read_sql_query(
        text(
            """
            SELECT
                game_id,
                player_id,
                points,
                rebounds,
                assists,
                threes,
                turnovers
            FROM player_game_logs
            WHERE game_id IN :game_ids
            """
        ).bindparams(bindparam("game_ids", expanding=True)),
        engine,
        params={"game_ids": game_ids},
    )
    games = pd.read_sql_query(
        text(
            """
            SELECT game_id, status AS game_status
            FROM games
            WHERE game_id IN :game_ids
            """
        ).bindparams(bindparam("game_ids", expanding=True)),
        engine,
        params={"game_ids": game_ids},
    )
    return stats, games


def main() -> None:
    p = argparse.ArgumentParser(description="Join rotation shadow overlap CSV with historical box scores.")
    p.add_argument("overlap_csv", type=Path, help="Path to rotation_shadow_overlap.csv")
    p.add_argument("-o", "--output", type=Path, default=None, help="Output path (default: sibling _with_actuals.csv)")
    args = p.parse_args()

    overlap = pd.read_csv(args.overlap_csv)
    if overlap.empty:
        raise SystemExit("Overlap CSV empty.")

    out = args.output
    if out is None:
        out = args.overlap_csv.with_name(args.overlap_csv.stem + "_with_actuals.csv")

    create_all()
    game_ids = sorted(pd.to_numeric(overlap["game_id"], errors="coerce").dropna().astype(int).unique().tolist())
    with session_scope() as session:
        hist, games = _load_box_scores(session, game_ids)
    hist = hist.drop_duplicates(subset=["game_id", "player_id"])
    merged = overlap.merge(games, on="game_id", how="left").merge(hist, on=["game_id", "player_id"], how="left")

    final_status = merged["game_status"].fillna("").astype(str).str.lower().isin({"final", "completed", "closed"})
    stat_columns = ["points", "rebounds", "assists", "threes", "turnovers"]
    merged.loc[final_status, stat_columns] = merged.loc[final_status, stat_columns].fillna(0.0)
    merged["actual_stat"] = merged.apply(lambda r: _stat_actual(r, str(r["market_key"])), axis=1)

    merged["legacy_error"] = merged["actual_stat"] - merged["legacy_proj_mean"]
    merged["shadow_error"] = merged["actual_stat"] - merged["shadow_proj_mean"]
    merged["absolute_error_legacy"] = merged["legacy_error"].abs()
    merged["absolute_error_shadow"] = merged["shadow_error"].abs()
    merged["squared_error_legacy"] = merged["legacy_error"] ** 2
    merged["squared_error_shadow"] = merged["shadow_error"] ** 2
    merged.to_csv(out, index=False)
    matched = merged["actual_stat"].notna() & merged["legacy_proj_mean"].notna() & merged["shadow_proj_mean"].notna()
    print(f"Wrote {out} ({int(matched.sum())}/{len(merged)} rows with numeric actuals matched)")


if __name__ == "__main__":
    main()
