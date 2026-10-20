"""Walk forward predictions with every pipeline stage kept separate.

For each eval season Y the models are fit on all earlier seasons and predict Y.
Per market and row we save the output of each stage, so later scripts can ask
which stage helps or hurts out of sample:

  direct   XGBoost on the stat total
  rate     XGBoost on the per minute rate times predicted minutes
  stacked  Ridge stacker over direct, rate and baseline
  pooled   partial pooling toward priors
  final    after the stabilize caps (what the app uses)

Also saved: the in sample MAE of each stage, and the OOS vs in sample error of
the direct model at increasing tree counts.

Usage:
    python scripts/diag/extract_walkforward.py 2023 2024 2025
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config.settings import get_settings  # noqa: E402
from app.db.session import configure_engine, session_scope  # noqa: E402
from app.evaluation.backtest import RollingOriginBacktester  # noqa: E402
from app.training.constants import MARKET_TARGETS  # noqa: E402
from app.training.data import DatasetLoader  # noqa: E402
from app.training.features import FeatureEngineer  # noqa: E402
from app.training.locked_defaults import DEFAULT_K_SEASONS  # noqa: E402
from app.training.models import (  # noqa: E402
    FittedMarketModel,
    StatModelSuite,
    _numeric_column,
    _recency_weighted_column,
    clip_non_negative,
)

OUT_DIR = ROOT / "data" / "artifacts" / "diag"
TREE_STEPS = [20, 40, 80, 120, 160, 220]
IN_SAMPLE_ROWS = 20000  # sample of train rows for in sample error, keeps this fast


def season_year(dates: pd.Series) -> pd.Series:
    dates = pd.to_datetime(dates)
    return pd.Series(np.where(dates.dt.month >= 10, dates.dt.year, dates.dt.year - 1), index=dates.index)


def stage_predictions(model: FittedMarketModel, frame: pd.DataFrame) -> dict[str, np.ndarray]:
    """Same steps as FittedMarketModel.predict, but keeping each stage."""
    market = model.market_key
    direct = clip_non_negative(np.asarray(model.mean_model.predict(frame[model.feature_columns]), dtype=float))
    baseline = clip_non_negative(np.nan_to_num(_numeric_column(frame, f"{market}_baseline_mean", 0.0), nan=0.0))
    exposure = _numeric_column(frame, "predicted_minutes", 0.0)
    rate_pred = np.asarray(model.rate_model.predict(frame[model.rate_feature_columns]), dtype=float)
    rate = clip_non_negative(clip_non_negative(rate_pred) * np.maximum(exposure, 0.0))
    stack_frame = model._stack_frame(frame, direct, rate, baseline)
    stacked = clip_non_negative(np.asarray(model.stacker.predict(stack_frame[model.stacker_columns]), dtype=float))
    pooled = model._apply_partial_pooling(frame, stacked, baseline)
    final = model._stabilize_mean(frame, pooled, baseline)
    # replication check: must match the production predict()
    production_mean, variance = model.predict(frame)
    if not np.allclose(final, production_mean, atol=1e-6):
        raise RuntimeError(f"{market}: stage replication drifted from FittedMarketModel.predict")
    history_anchor = model._market_history_anchor(frame, market, baseline)[1]
    return {
        "direct": direct,
        "rate": rate,
        "baseline": baseline,
        "stacked": stacked,
        "pooled": pooled,
        "final": final,
        "variance": variance,
        "history_anchor": history_anchor,
        "recency": _recency_weighted_column(frame, market, baseline),
    }


def tree_curve(model: FittedMarketModel, train: pd.DataFrame, val: pd.DataFrame, target: str) -> list[dict]:
    """RMSE of the direct XGBoost model at increasing tree counts."""
    rows = []
    sample = train.sample(n=min(IN_SAMPLE_ROWS, len(train)), random_state=7)
    for trees in TREE_STEPS:
        entry = {"trees": trees}
        for name, data in (("train", sample), ("val", val)):
            pred = model.mean_model.predict(data[model.feature_columns], iteration_range=(0, trees))
            entry[f"{name}_rmse"] = float(np.sqrt(np.mean((data[target].to_numpy(float) - pred) ** 2)))
        rows.append(entry)
    return rows


def main() -> None:
    eval_seasons = [int(arg) for arg in sys.argv[1:]] or [2023, 2024, 2025]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    settings = get_settings()
    print(f"parquet root: {settings.historical_parquet_root}", flush=True)

    configure_engine()
    with session_scope() as session:
        started = time.time()
        historical = DatasetLoader(session).load_historical_player_games()
        feature_set = FeatureEngineer(k_seasons=DEFAULT_K_SEASONS).build_training_frame(historical)
        frame = feature_set.frame.sort_values(["game_date", "start_time", "game_id", "player_id"]).reset_index(drop=True)
        frame["season_year"] = season_year(frame["game_date"]).to_numpy()
        print(f"rows={len(frame)} seasons={sorted(frame['season_year'].unique())} ({time.time() - started:.0f}s)", flush=True)

        backtester = RollingOriginBacktester(session)
        stage_rows: list[pd.DataFrame] = []
        insample_rows: list[dict] = []
        curve_rows: list[dict] = []
        for year in eval_seasons:
            train = frame[frame["season_year"] < year].copy()
            val = frame[frame["season_year"] == year].copy()
            if len(train) < 5000 or val.empty:
                print(f"skip {year}: train={len(train)} val={len(val)}", flush=True)
                continue
            fold_start = time.time()
            train_p, val_p, columns = backtester._prepare_frames(train, val, feature_set.feature_columns)
            suite = StatModelSuite(settings.training_seed)
            suite.fit(train_p, columns)
            in_sample = train_p.sample(n=min(IN_SAMPLE_ROWS, len(train_p)), random_state=7)
            for market, target in MARKET_TARGETS.items():
                model = suite.models[market]
                val_stages = stage_predictions(model, val_p)
                train_stages = stage_predictions(model, in_sample)
                for name in ("direct", "rate", "stacked", "pooled", "final"):
                    insample_rows.append(
                        {
                            "eval_season": year,
                            "market": market,
                            "stage": name,
                            "in_sample_mae": float(np.mean(np.abs(in_sample[target].to_numpy(float) - train_stages[name]))),
                            "oos_mae": float(np.mean(np.abs(val_p[target].to_numpy(float) - val_stages[name]))),
                            "train_rows": len(train_p),
                            "val_rows": len(val_p),
                        }
                    )
                for step in tree_curve(model, train_p, val_p, target):
                    curve_rows.append({"eval_season": year, "market": market, **step})
                out = pd.DataFrame(
                    {
                        "eval_season": year,
                        "market": market,
                        "game_date": val_p["game_date"].to_numpy(),
                        "player_id": val_p["player_id"].to_numpy(),
                        "actual": val_p[target].to_numpy(float),
                        "predicted_minutes": val_p["predicted_minutes"].to_numpy(float),
                        "history_games": _numeric_column(val_p, "history_games_played", 0.0),
                        "starter": _numeric_column(val_p, "starter_flag", 0.0),
                        "avg5": _numeric_column(val_p, f"{target}_avg_5", 0.0),
                        "avg10": _numeric_column(val_p, f"{target}_avg_10", 0.0),
                        "avg20": _numeric_column(val_p, f"{target}_avg_20", 0.0),
                        "season_avg": _numeric_column(val_p, f"{target}_season_avg", 0.0),
                    }
                )
                for name, values in val_stages.items():
                    out[name] = values
                stage_rows.append(out)
            print(f"fold {year}: train={len(train_p)} val={len(val_p)} ({time.time() - fold_start:.0f}s)", flush=True)

        pd.concat(stage_rows, ignore_index=True).to_parquet(OUT_DIR / "nba_stage_predictions.parquet")
        pd.DataFrame(insample_rows).to_csv(OUT_DIR / "nba_insample_vs_oos.csv", index=False)
        pd.DataFrame(curve_rows).to_csv(OUT_DIR / "nba_tree_curve.csv", index=False)
        print(f"wrote {OUT_DIR}", flush=True)


if __name__ == "__main__":
    main()
