"""Score the stat model against naive baselines on held out player games.

Baselines use only games before each target game:
  trailing 5, trailing 10, and the season to date mean.

The model side is read from a backtest folds file written by the rolling
origin backtester (backtest_folds_*.csv), restricted to the same dates.

Usage:
    python scripts/baseline_comparison.py --folds <backtest_folds.csv>
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BOX_SCORE_ROOT = ROOT / "data" / "parquet" / "box_scores"
MIN_PRIOR_GAMES = 5  # matches MINIMUM_PREDICTION_HISTORY_GAMES
MIN_MINUTES = 10.0  # model trains on minutes >= 10
MARKETS = ["points", "rebounds", "assists", "threes", "turnovers", "pra"]
BASELINES = ["trail5", "trail10", "season"]


def load_box_scores(season: int) -> pd.DataFrame:
    path = BOX_SCORE_ROOT / f"season={season}" / "part-0.parquet"
    df = pd.read_parquet(path)
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["pra"] = df["points"] + df["rebounds"] + df["assists"]
    return df.sort_values(["provider_player_id", "game_date"]).reset_index(drop=True)


def add_baselines(df: pd.DataFrame) -> pd.DataFrame:
    # DNP rows (minutes 0) are not real games, drop them before averaging
    played = df[df["minutes"] > 0].copy()
    played["prior_games"] = played.groupby("provider_player_id").cumcount()
    for market in MARKETS:
        by_player = played.groupby("provider_player_id")[market]
        # shift(1) so a game never sees itself
        played[f"{market}_trail5"] = by_player.transform(lambda s: s.shift(1).rolling(5).mean())
        played[f"{market}_trail10"] = by_player.transform(lambda s: s.shift(1).rolling(10).mean())
        played[f"{market}_season"] = by_player.transform(lambda s: s.shift(1).expanding().mean())
    return played


def baseline_errors(played: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    window = played[
        (played["game_date"] >= start)
        & (played["game_date"] <= end)
        & (played["minutes"] >= MIN_MINUTES)
        & (played["prior_games"] >= MIN_PRIOR_GAMES)
    ]
    rows = []
    for market in MARKETS:
        for name in BASELINES:
            err = window[market] - window[f"{market}_{name}"]
            rows.append(
                {
                    "market": market,
                    "method": name,
                    "n": len(err),
                    "mae": err.abs().mean(),
                    "rmse": float(np.sqrt((err**2).mean())),
                    "bias": err.mean(),
                }
            )
    return pd.DataFrame(rows)


def model_errors(folds_path: Path, start: str, end: str) -> pd.DataFrame:
    folds = pd.read_csv(folds_path)
    folds = folds[(folds["fold_start"] >= start) & (folds["fold_start"] <= end)]
    cols = ["mae", "rmse", "interval_80_coverage"]
    out = folds.groupby("market_key")[cols].mean().reset_index()
    out["folds"] = folds.groupby("market_key").size().to_numpy()
    return out.rename(columns={"market_key": "market"})


def build_report(base: pd.DataFrame, model: pd.DataFrame, start: str, end: str) -> str:
    lines = [
        f"Window {start} to {end}. Baselines need {MIN_PRIOR_GAMES}+ prior games and 10+ minutes.",
        "",
        "| market | model MAE | trail5 | trail10 | season | model vs best baseline | model 80% coverage |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for market in MARKETS:
        sub = base[base["market"] == market].set_index("method")
        m = model[model["market"] == market]
        if m.empty:
            continue
        model_mae = float(m["mae"].iloc[0])
        best = float(sub["mae"].min())
        gap = (best - model_mae) / best * 100
        lines.append(
            f"| {market} | {model_mae:.3f} | {sub.loc['trail5', 'mae']:.3f} "
            f"| {sub.loc['trail10', 'mae']:.3f} | {sub.loc['season', 'mae']:.3f} "
            f"| {gap:+.1f}% | {float(m['interval_80_coverage'].iloc[0]):.1%} |"
        )
    n = int(base["n"].iloc[0])
    lines += ["", f"Baseline rows per market: {n}. Model folds: {int(model['folds'].max())}."]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--start", default="2026-01-28")
    parser.add_argument("--end", default="2026-05-13")
    parser.add_argument("--folds", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    played = add_baselines(load_box_scores(args.season))
    base = baseline_errors(played, args.start, args.end)
    model = model_errors(args.folds, args.start, args.end)
    report = build_report(base, model, args.start, args.end)
    print(report)
    if args.out is not None:
        args.out.write_text(report + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
