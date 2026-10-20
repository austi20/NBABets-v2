"""Would an opponent strength multiplier improve the projection?

Two candidate factors, both built only from games before the one being predicted:

  team   how much opposing players beat or missed their own pre game expectation
         against this defense in its recent games (a ratio, so who they were
         is already normalized away)
  skill  the same, but each past player counts in proportion to how close his
         own level is to the player being projected (a star is compared with
         stars, a role player with role players)

Test, per market, out of sample on the walk forward stage predictions:
  1. correlation of each factor with the next game's actual / final ratio
  2. least squares lambda in  actual - final = lambda * final * (factor - 1)
     lambda = 0 means no help, 1 means apply the factor in full
  3. honest MAE: lambda fit on earlier eval seasons only, applied to the next
  4. the same signal from the existing `opponent_allowed_*_10` feature

Usage:
    python scripts/diag/extract_walkforward.py 2023 2024 2025
    python scripts/diag/opponent_factor_experiment.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import ndtr

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.session import configure_engine, session_scope  # noqa: E402
from app.training.constants import MARKET_TARGETS  # noqa: E402
from app.training.data import DatasetLoader  # noqa: E402
from app.training.features import FeatureEngineer  # noqa: E402
from app.training.locked_defaults import DEFAULT_K_SEASONS  # noqa: E402

DIAG = ROOT / "data" / "artifacts" / "diag"
MIN_HISTORY = 5
DECAY = 0.90  # weight of a game one game older
SHRINK = 480.0  # minutes of evidence worth one game of league average play
TAU = 0.50  # skill kernel width, in log units
WINDOW_GAMES = 20  # past games per defense used by the skill factor
RATIO_FLOOR = 0.25  # winsorize one game ratios
RATIO_CAP = 3.0


def season_year(dates: pd.Series) -> pd.Series:
    dates = pd.to_datetime(dates)
    return pd.Series(np.where(dates.dt.month >= 10, dates.dt.year, dates.dt.year - 1), index=dates.index)


def build_rows(frame: pd.DataFrame, market: str) -> pd.DataFrame:
    """One row per player game with the pre game expectation and the ratio to it."""
    target = MARKET_TARGETS[market]
    rows = pd.DataFrame(
        {
            "row": np.arange(len(frame)),
            "player_id": frame["player_id"].to_numpy(),
            "game_id": frame["game_id"].to_numpy(),
            "game_date": pd.to_datetime(frame["game_date"]).to_numpy(),
            "opp": frame["opponent_team_id"].to_numpy(),
            "minutes": frame["minutes"].to_numpy(float),
            "actual": frame[target].to_numpy(float),
            "expected": 0.5 * frame[f"{target}_avg_10"].to_numpy(float) + 0.5 * frame[f"{target}_season_avg"].to_numpy(float),
            "history": frame["history_games_played"].to_numpy(float),
        }
    )
    rows["eligible"] = (rows["history"] >= MIN_HISTORY) & (rows["expected"] > 0)
    smoothing = 0.25 * rows.loc[rows["eligible"], "actual"].mean()  # keeps low count stats from exploding
    ratio = (rows["actual"] + smoothing) / (rows["expected"] + smoothing)
    rows["ratio"] = ratio.clip(RATIO_FLOOR, RATIO_CAP)
    rows["weight"] = np.where(rows["eligible"], rows["minutes"], 0.0)
    return rows.sort_values(["opp", "game_date", "game_id"]).reset_index(drop=True)


def team_factor(rows: pd.DataFrame, decay: float = DECAY, shrink: float = SHRINK) -> pd.Series:
    """Decayed, shrunk average excess ratio against each defense, using earlier games only."""
    factor = pd.Series(1.0, index=rows.index)
    rows = rows.assign(excess=(rows["ratio"] - 1.0) * rows["weight"])
    for _, team_rows in rows.groupby("opp"):
        games = team_rows.groupby("game_id", sort=False).agg(
            excess=("excess", "sum"), weight=("weight", "sum"), when=("game_date", "min")
        )
        games = games.sort_values("when")
        running_excess = 0.0
        running_weight = 0.0
        before: dict[int, float] = {}
        for game_id, game in games.iterrows():
            before[game_id] = 1.0 + running_excess / (running_weight + shrink)
            running_excess = decay * running_excess + game["excess"]
            running_weight = decay * running_weight + game["weight"]
        factor.loc[team_rows.index] = team_rows["game_id"].map(before).to_numpy()
    return factor


def skill_factor(rows: pd.DataFrame, decay: float = DECAY, shrink: float = SHRINK) -> pd.Series:
    """Like team_factor, but past players are weighted by closeness in level to the target player."""
    factor = pd.Series(1.0, index=rows.index)
    for _, team_rows in rows.groupby("opp"):
        games = [g for _, g in team_rows.groupby("game_id", sort=False)]
        games.sort(key=lambda g: g["game_date"].min())
        for position, game in enumerate(games):
            past = games[max(0, position - WINDOW_GAMES) : position]
            if not past:
                continue
            old = pd.concat(past)
            age = np.concatenate([np.full(len(g), position - 1 - i) for i, g in enumerate(past, start=max(0, position - WINDOW_GAMES))])
            old_level = np.log(old["expected"].to_numpy(float).clip(min=0.05))
            old_weight = old["weight"].to_numpy(float) * decay**age
            old_excess = old["ratio"].to_numpy(float) - 1.0
            target_level = np.log(game["expected"].to_numpy(float).clip(min=0.05))
            kernel = np.exp(-np.abs(target_level[:, None] - old_level[None, :]) / TAU)
            weights = kernel * old_weight[None, :]
            numerator = (weights * old_excess[None, :]).sum(axis=1)
            denominator = weights.sum(axis=1) + shrink * 0.25
            factor.loc[game.index] = 1.0 + numerator / denominator
    return factor


def lambda_hat(final: np.ndarray, actual: np.ndarray, multiplier: np.ndarray) -> float:
    x = final * (multiplier - 1.0)
    y = actual - final
    denominator = float((x * x).sum())
    return float((x * y).sum() / denominator) if denominator > 1e-12 else float("nan")


def over_log_loss(mean: np.ndarray, variance: np.ndarray, line: np.ndarray, actual: np.ndarray) -> float:
    """Log loss of P(over) at a .5 line, normal approximation with the model's own variance."""
    sd = np.sqrt(np.maximum(variance, 0.25))
    p_over = 1.0 - ndtr((line - mean) / sd)
    p_over = np.clip(p_over, 0.02, 0.98)
    hit = (actual > line).astype(float)
    return float(-np.mean(hit * np.log(p_over) + (1.0 - hit) * np.log(1.0 - p_over)))


def evaluate(market: str, stages: pd.DataFrame, factors: dict[str, np.ndarray], existing: np.ndarray) -> list[str]:
    final = stages["final"].to_numpy(float)
    actual = stages["actual"].to_numpy(float)
    season = stages["eval_season"].to_numpy()
    seasons = sorted(set(season))
    ratio = np.clip(actual / np.maximum(final, 0.05), 0.0, 3.0)
    lines = []
    variance = stages["variance"].to_numpy(float)
    line = np.floor(stages["history_anchor"].to_numpy(float)) + 0.5  # a typical posted line sits near the anchor
    candidates = {**factors, "existing opponent_allowed_10": existing}
    for name, mult in candidates.items():
        corr = float(np.corrcoef(mult - 1.0, ratio - 1.0)[0, 1])
        lam_all = lambda_hat(final, actual, mult)
        lam_by = [lambda_hat(final[season == s], actual[season == s], mult[season == s]) for s in seasons]
        # honest: lambda from earlier seasons only
        honest_final = final.copy()
        used = np.zeros(len(final), dtype=bool)
        for s in seasons[1:]:
            earlier = season < s
            lam = lambda_hat(final[earlier], actual[earlier], mult[earlier])
            this = season == s
            honest_final[this] = final[this] * (1.0 + lam * (mult[this] - 1.0))
            used |= this
        base_mae = float(np.mean(np.abs(actual[used] - final[used])))
        new_mae = float(np.mean(np.abs(actual[used] - honest_final[used])))
        base_ll = over_log_loss(final[used], variance[used], line[used], actual[used])
        new_ll = over_log_loss(honest_final[used], variance[used], line[used], actual[used])
        spread = float(np.std(mult - 1.0))
        lines.append(
            f"| {market} | {name} | {spread:.3f} | {corr:+.3f} | {lam_all:+.2f} | "
            f"{' '.join(f'{v:+.2f}' for v in lam_by)} | {(new_mae - base_mae) / base_mae:+.2%} | "
            f"{base_ll:.4f} -> {new_ll:.4f} ({(new_ll - base_ll) / base_ll:+.2%}) |"
        )
    return lines


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--decay", type=float, default=DECAY)
    parser.add_argument("--shrink", type=float, default=SHRINK)
    parser.add_argument("--team-only", action="store_true", help="skip the slower skill matched factor")
    args = parser.parse_args()

    stages_all = pd.read_parquet(DIAG / "nba_stage_predictions.parquet")
    stages_all = stages_all[stages_all["history_games"] >= MIN_HISTORY]

    configure_engine()
    with session_scope() as session:
        historical = DatasetLoader(session).load_historical_player_games()
        frame = FeatureEngineer(k_seasons=DEFAULT_K_SEASONS).build_training_frame(historical).frame
    frame = frame.sort_values(["game_date", "start_time", "game_id", "player_id"]).reset_index(drop=True)
    frame["game_day"] = pd.to_datetime(frame["game_date"]).dt.normalize()

    out_lines = [
        "| market | factor | spread (sd) | corr with next game excess | lambda (all) | lambda by season | honest MAE change | honest log loss of P(over line) |",
        "|---|---|---:|---:|---|---|---:|---|",
    ]
    for market in MARKET_TARGETS:
        rows = build_rows(frame, market)
        team = team_factor(rows, args.decay, args.shrink)
        skill = team if args.team_only else skill_factor(rows, args.decay, args.shrink)
        lookup = pd.DataFrame(
            {
                "player_id": rows["player_id"],
                "game_day": pd.to_datetime(rows["game_date"]).dt.normalize(),
                "team": team.to_numpy(),
                "skill": skill.to_numpy(),
            }
        )
        stage = stages_all[stages_all["market"] == market].copy()
        stage["game_day"] = pd.to_datetime(stage["game_date"]).dt.normalize()
        joined = stage.merge(lookup, on=["player_id", "game_day"], how="left")
        existing_source = frame[["player_id", "game_day", f"opponent_allowed_{market}_10"]].rename(
            columns={f"opponent_allowed_{market}_10": "existing"}
        )
        joined = joined.merge(existing_source, on=["player_id", "game_day"], how="left").dropna(subset=["team", "skill", "existing"])
        league_mean = float(joined["avg10"].mean())
        existing_mult = (joined["existing"] / max(league_mean, 1e-9)).to_numpy(float)
        out_lines += evaluate(
            market,
            joined,
            {"team defense": joined["team"].to_numpy(float), "skill matched": joined["skill"].to_numpy(float)},
            existing_mult,
        )
        print(f"{market}: {len(joined)} rows", flush=True)

    text = "\n".join(out_lines)
    print(text)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
