from __future__ import annotations

import argparse
import json
import os
import sys
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.session import session_scope  # noqa: E402
from app.evaluation.metrics import compute_probability_metrics, compute_regression_metrics  # noqa: E402
from app.training.baseline import RecencyBaseline  # noqa: E402
from app.training.calibration import ProbabilityCalibrator  # noqa: E402
from app.training.constants import MARKET_TARGETS  # noqa: E402
from app.training.data import DatasetLoader  # noqa: E402
from app.training.distributions import (  # noqa: E402
    distribution_summary_from_samples,
    empirical_pit_cdf,
    sample_market_outcomes,
)
from app.training.models import MinutesModel, StatModelSuite  # noqa: E402
from app.training.pipeline import (  # noqa: E402
    TrainingPipeline,
    _apply_feature_defaults,
    _neutralize_probability_series,
    _stat_feature_columns,
    _with_minutes_predictions,
    _with_output_columns,
)

PROB_CLIP_LOW = 0.04
PROB_CLIP_HIGH = 0.96


@dataclass(frozen=True)
class SliceDefinition:
    name: str
    predicate: Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 7 backtest for rotation shock ablations.")
    parser.add_argument("--eval-days", type=int, default=28, help="Holdout evaluation window in days.")
    parser.add_argument("--train-days", type=int, default=180, help="Optional train-window cap in days.")
    parser.add_argument(
        "--modes",
        default="off,features-only,full",
        help="Comma-separated ablations from: off,features-only,full",
    )
    parser.add_argument(
        "--max-eval-rows",
        type=int,
        default=4000,
        help="Cap evaluation rows for runtime control (0 disables cap).",
    )
    parser.add_argument("--pit-sample-size", type=int, default=5000, help="Monte Carlo draws per row for empirical PIT.")
    parser.add_argument("--rng-base-seed", type=int, default=901_347, help="Base seed deterministic per-row subsamples.")
    parser.add_argument(
        "--coverage-low",
        type=float,
        default=0.70,
        help="Reject if high-injury mean p10–p90 coverage sits below this (plan target ~0.80).",
    )
    parser.add_argument(
        "--coverage-high",
        type=float,
        default=0.92,
        help="Reject if high-injury mean coverage exceeds this upper band.",
    )
    parser.add_argument("--bootstrap-iters", type=int, default=1500, help="Clustered bootstrap iterations by game_id (0 skips).")
    parser.add_argument(
        "--ll-slack",
        type=float,
        default=1e-3,
        help="Extra slack when comparing full vs off log loss on high-injury slice (full <= off + slack).",
    )
    parser.add_argument(
        "--no-calibrator",
        action="store_true",
        help="Use raw simulated over probabilities (skip train-fit ProbabilityCalibrator).",
    )
    parser.add_argument(
        "--bootstrap-as-blocker",
        action="store_true",
        help="When clustered bootstrap CI includes zero, downgrade run to NO-GO (default: advisory only).",
    )
    return parser.parse_args()


@contextmanager
def _rotation_mode(mode: str):
    old_enabled = os.getenv("ROTATION_SHOCK_ENABLED")
    old_mode = os.getenv("ROTATION_SHOCK_ABLATION_MODE")
    try:
        if mode == "off":
            os.environ["ROTATION_SHOCK_ENABLED"] = "false"
            os.environ["ROTATION_SHOCK_ABLATION_MODE"] = "off"
        else:
            os.environ["ROTATION_SHOCK_ENABLED"] = "true"
            os.environ["ROTATION_SHOCK_ABLATION_MODE"] = mode
        yield
    finally:
        if old_enabled is None:
            os.environ.pop("ROTATION_SHOCK_ENABLED", None)
        else:
            os.environ["ROTATION_SHOCK_ENABLED"] = old_enabled
        if old_mode is None:
            os.environ.pop("ROTATION_SHOCK_ABLATION_MODE", None)
        else:
            os.environ["ROTATION_SHOCK_ABLATION_MODE"] = old_mode


def _pit_ks_distance(pit_values: np.ndarray) -> float:
    if pit_values.size == 0:
        return float("nan")
    values = np.sort(np.clip(pit_values, 0.0, 1.0))
    n = values.size
    cdf = np.arange(1, n + 1, dtype=float) / n
    d_plus = np.max(cdf - values)
    d_minus = np.max(values - (np.arange(0, n, dtype=float) / n))
    return float(max(d_plus, d_minus))


def _line_for_market(frame: pd.DataFrame, market_key: str, fallback: np.ndarray) -> np.ndarray:
    col = f"line_{market_key}"
    if col not in frame.columns:
        return np.asarray(fallback, dtype=float)
    return pd.to_numeric(frame[col], errors="coerce").fillna(pd.Series(fallback, index=frame.index)).to_numpy(dtype=float)


def _row_rng(seed_base: int, game_id: int, player_id: int, market_key: str) -> np.random.Generator:
    mix = zlib.crc32(market_key.encode("utf-8")) & 0xFFFFFFFF
    agg = seed_base ^ int(mix) ^ game_id * 100_003 ^ player_id * 1_759
    return np.random.default_rng(max(agg % (2**32 - 2), 1))


def _market_prior_vector(frame: pd.DataFrame, market_key: str) -> np.ndarray | None:
    for column in (f"{market_key}_consensus_prob_mean", "consensus_prob_mean", "no_vig_over_probability"):
        if column in frame.columns:
            return _neutralize_probability_series(pd.to_numeric(frame[column], errors="coerce")).to_numpy(dtype=float)
    return None


def _apply_calibrator_maybe(
    calibrator: ProbabilityCalibrator | None,
    *,
    raw: np.ndarray,
    prior: np.ndarray | None,
) -> np.ndarray:
    if calibrator is None:
        arr = raw
    else:
        if prior is not None:
            arr = np.asarray(calibrator.transform(raw, market_priors=prior), dtype=float)
        else:
            arr = np.asarray(calibrator.transform(raw), dtype=float)
    return np.clip(arr, PROB_CLIP_LOW, PROB_CLIP_HIGH)


def _simulate_row_metrics(
    pipeline: TrainingPipeline,
    frame: pd.DataFrame,
    means: np.ndarray,
    variances: np.ndarray,
    market_key: str,
    target_column: str,
    *,
    pit_sample_size: int,
    rng_base_seed: int,
    calibrator: ProbabilityCalibrator | None,
) -> pd.DataFrame:
    lines = _line_for_market(frame, market_key, means)
    prior_vec = _market_prior_vector(frame, market_key)
    records = frame.to_dict("records")
    actual = pd.to_numeric(frame[target_column], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    game_ids = pd.to_numeric(frame["game_id"], errors="coerce").fillna(0).astype(int).to_numpy()
    player_ids = pd.to_numeric(frame["player_id"], errors="coerce").fillna(0).astype(int).to_numpy()
    magnitude = pd.to_numeric(frame.get("rotation_shock_magnitude", 0.0), errors="coerce").fillna(0.0).to_numpy(dtype=float)

    mins_baseline = pd.to_numeric(
        frame.get("baseline_projected_minutes", frame.get("predicted_minutes", 0.0)),
        errors="coerce",
    ).fillna(0.0)
    mins_adj_col = frame.get("adjusted_projected_minutes")
    mins_adj = (
        mins_baseline
        if mins_adj_col is None
        else pd.to_numeric(mins_adj_col, errors="coerce").fillna(mins_baseline)
    ).to_numpy(dtype=float)
    mins_baseline_a = mins_baseline.to_numpy(dtype=float)
    minutes_delta = np.abs(np.asarray(mins_adj, dtype=float) - mins_baseline_a)

    ras: list[float] = []
    pits: list[float] = []
    p10_list: list[float] = []
    p90_list: list[float] = []
    divergence: list[float] = []

    for i, row in enumerate(records):
        mean_f = float(means[i])
        var_f = float(max(float(variances[i]), 1e-9))
        line_f = float(lines[i])
        rng = _row_rng(rng_base_seed, int(game_ids[i]), int(player_ids[i]), market_key)
        samples = sample_market_outcomes(
            mean=mean_f,
            variance=var_f,
            sample_size=pit_sample_size,
            rng=rng,
            minutes_mean=float(
                row.get("adjusted_projected_minutes", row.get("predicted_minutes", row.get("minutes_avg_10", 0.0))) or 0.0
            ),
            minutes_std=float(max(row.get("predicted_minutes_std", row.get("minutes_volatility", 1.0)) or 1.0, 1.0)),
            market_key=market_key,
            context=pipeline._simulation_context(row, market_key),
            dist_family=pipeline._dist_family_for_market(market_key),
        )
        summary = distribution_summary_from_samples(samples, line_f)
        raw_over = float(summary.over_probability)
        ras.append(raw_over)
        pits.append(empirical_pit_cdf(samples, float(actual[i])))
        p10_list.append(float(summary.p10))
        p90_list.append(float(summary.p90))
        divergence.append(float(np.abs(mean_f - line_f) / max(np.abs(line_f), 0.5)))

    raw_arr = np.asarray(ras, dtype=float)
    cal_arr = _apply_calibrator_maybe(calibrator, raw=raw_arr, prior=prior_vec)
    p10_arr = np.asarray(p10_list, dtype=float)
    p90_arr = np.asarray(p90_list, dtype=float)

    cov = ((actual >= p10_arr) & (actual <= p90_arr)).astype(float)

    return pd.DataFrame(
        {
            "game_id": game_ids.astype(int),
            "player_id": player_ids.astype(int),
            "market_key": market_key,
            "actual": actual,
            "predicted_mean": means.astype(float),
            "predicted_variance": variances.astype(float),
            "line_value": lines.astype(float),
            "over_probability": cal_arr,
            "raw_over_probability": raw_arr,
            "p10": p10_arr,
            "p90": p90_arr,
            "team_out_count": pd.to_numeric(frame.get("team_out_count", 0.0), errors="coerce")
            .fillna(0.0)
            .to_numpy(dtype=float),
            "pit_value": np.asarray(pits, dtype=float),
            "line_divergence": np.asarray(divergence, dtype=float),
            "coverage_hit": cov,
            "rotation_shock_magnitude": magnitude.astype(float),
            "abs_minutes_delta": minutes_delta.astype(float),
        }
    )


def _metrics_for_subset(df: pd.DataFrame) -> dict[str, float]:
    if df.empty:
        return {
            "rows": 0.0,
            "mae": float("nan"),
            "rmse": float("nan"),
            "log_loss": float("nan"),
            "brier_score": float("nan"),
            "ece": float("nan"),
            "p10_p90_coverage": float("nan"),
            "pit_ks": float("nan"),
            "line_divergence": float("nan"),
            "weighted_rmse": float("nan"),
        }
    regression = compute_regression_metrics(df["actual"].to_numpy(dtype=float), df["predicted_mean"].to_numpy(dtype=float))
    labels = (df["actual"].to_numpy(dtype=float) > df["line_value"].to_numpy(dtype=float)).astype(int)
    clipped = np.clip(df["over_probability"].to_numpy(dtype=float), 1e-6, 1.0 - 1e-6)
    probability = compute_probability_metrics(labels, clipped)

    denom = df["predicted_variance"].mean() if df["predicted_variance"].notna().any() else float("nan")
    weighted_rmse = float(regression.rmse / max(np.sqrt(max(denom, 1e-9)), 1e-9))

    return {
        "rows": float(len(df)),
        "mae": float(regression.mae),
        "rmse": float(regression.rmse),
        "log_loss": float(probability.log_loss),
        "brier_score": float(probability.brier_score),
        "ece": float(probability.expected_calibration_error),
        "p10_p90_coverage": float(df["coverage_hit"].mean()),
        "pit_ks": _pit_ks_distance(df["pit_value"].to_numpy(dtype=float)),
        "line_divergence": float(df["line_divergence"].mean()),
        "weighted_rmse": weighted_rmse,
    }


def _fit_train_calibrators(
    *,
    pipeline: TrainingPipeline,
    train: pd.DataFrame,
    stat_features: list[str],
    models: StatModelSuite,
    pit_sample_size: int,
    rng_base_seed: int,
) -> dict[str, ProbabilityCalibrator]:
    out: dict[str, ProbabilityCalibrator] = {}
    for market_key, target_column in MARKET_TARGETS.items():
        subset = train[train["market_key"] == market_key].copy()
        cal = ProbabilityCalibrator()
        out[market_key] = cal
        if subset.empty or len(subset) < 20:
            continue
        means_t, vars_t = models.models[market_key].predict(subset)
        simulated = _simulate_row_metrics(
            pipeline,
            subset,
            means_t,
            vars_t,
            market_key,
            target_column,
            pit_sample_size=pit_sample_size,
            rng_base_seed=rng_base_seed,
            calibrator=None,
        )
        raw = simulated["raw_over_probability"].to_numpy(dtype=float)
        labels = (
            simulated["actual"].to_numpy(dtype=float) > simulated["line_value"].to_numpy(dtype=float)
        ).astype(int)
        priors_vec = _market_prior_vector(subset, market_key)
        if raw.size and len(np.unique(labels)) >= 2:
            cal.fit(raw, labels, market_priors=priors_vec if priors_vec is not None else None)
    return out


def _build_backtest_feature_frames(
    pipeline: TrainingPipeline,
    train_raw: pd.DataFrame,
    eval_raw: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    train_marked = train_raw.copy()
    eval_marked = eval_raw.copy()
    train_marked["_phase7_split"] = "train"
    eval_marked["_phase7_split"] = "eval"
    combined_raw = pd.concat([train_marked, eval_marked], ignore_index=True, sort=False)
    feature_set = pipeline._features.build_training_frame(combined_raw)
    frame = feature_set.frame.copy()
    if "_phase7_split" not in frame.columns:
        raise RuntimeError("Backtest split marker was dropped during feature construction.")
    train = frame[frame["_phase7_split"] == "train"].drop(columns=["_phase7_split"]).copy()
    evaluate = frame[frame["_phase7_split"] == "eval"].drop(columns=["_phase7_split"]).copy()
    feature_columns = [column for column in feature_set.feature_columns if column != "_phase7_split"]
    if train.empty or evaluate.empty:
        raise RuntimeError("Backtest feature split produced empty frame(s).")
    return train.reset_index(drop=True), evaluate.reset_index(drop=True), feature_columns


def _aggregate_treatment_diagnostics(evaluate: pd.DataFrame) -> dict[str, Any]:
    if evaluate.empty:
        return {
            "rows_unique_player_game": 0,
            "fraction_nonzero_rotation_magnitude": 0.0,
            "mean_abs_rotation_magnitude": 0.0,
            "mean_abs_minutes_delta": 0.0,
            "games_fraction_any_rotation": 0.0,
            "fraction_high_injury": 0.0,
        }
    uniq = evaluate.drop_duplicates(subset=["game_id", "player_id"]).copy()
    mag = pd.to_numeric(uniq.get("rotation_shock_magnitude", 0.0), errors="coerce").fillna(0.0).to_numpy(dtype=float)
    team_out = pd.to_numeric(uniq.get("team_out_count", 0.0), errors="coerce").fillna(0.0).to_numpy(dtype=float)

    mins_baseline = pd.to_numeric(
        uniq.get("baseline_projected_minutes", uniq.get("predicted_minutes", 0.0)),
        errors="coerce",
    ).fillna(0.0)
    aj = uniq.get("adjusted_projected_minutes")
    mins_adj = (
        mins_baseline
        if aj is None
        else pd.to_numeric(aj, errors="coerce").fillna(mins_baseline)
    ).to_numpy(dtype=float)
    delta = np.abs(mins_adj - mins_baseline.to_numpy(dtype=float))

    by_game = uniq.assign(_mag=np.abs(mag)).groupby("game_id").agg(game_mag=("_mag", "max")).reset_index()
    games_hit = float((by_game["game_mag"] > 1e-6).mean())

    return {
        "rows_unique_player_game": int(len(uniq)),
        "fraction_nonzero_rotation_magnitude": float(np.mean(np.abs(mag) > 1e-6)),
        "mean_abs_rotation_magnitude": float(np.mean(np.abs(mag))),
        "mean_abs_minutes_delta": float(np.mean(delta)),
        "games_fraction_any_rotation": games_hit,
        "fraction_high_injury": float(np.mean(team_out >= 2.0)),
    }


def _slice_specs() -> list[SliceDefinition]:
    return [
        SliceDefinition("team_out_count=0", lambda d: d[d["team_out_count"] == 0]),
        SliceDefinition("team_out_count=1", lambda d: d[d["team_out_count"] == 1]),
        SliceDefinition("team_out_count>=2", lambda d: d[d["team_out_count"] >= 2]),
    ]


def _evaluate_mode(
    pipeline: TrainingPipeline,
    mode: str,
    train_raw: pd.DataFrame,
    eval_raw: pd.DataFrame,
    *,
    pit_sample_size: int,
    rng_base_seed: int,
    skip_calibrator: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    train, evaluate, raw_feature_columns = _build_backtest_feature_frames(pipeline, train_raw, eval_raw)
    baseline_train = RecencyBaseline().fit_predict(train)
    train = _with_output_columns(train, baseline_train)
    baseline_eval = RecencyBaseline().fit_predict(evaluate)
    evaluate = _with_output_columns(evaluate, baseline_eval)

    base_feature_columns = sorted(
        set(
            raw_feature_columns
            + [c for c in train.columns if c.endswith("_baseline_mean") or c.endswith("_baseline_variance")]
        )
    )
    train = _apply_feature_defaults(train, base_feature_columns)
    evaluate = _apply_feature_defaults(evaluate, base_feature_columns)

    minutes_model = MinutesModel(pipeline._settings.training_seed)
    minutes_model.fit(train, base_feature_columns)
    train = _with_minutes_predictions(train, minutes_model.predict(train), minutes_model.predict_uncertainty(train))
    evaluate = _with_minutes_predictions(evaluate, minutes_model.predict(evaluate), minutes_model.predict_uncertainty(evaluate))
    train = pipeline._features.apply_post_minutes_features(train, minutes_column="predicted_minutes")
    evaluate = pipeline._features.apply_post_minutes_features(evaluate, minutes_column="predicted_minutes")

    with _rotation_mode(mode):
        train = pipeline._apply_rotation_treatment_mode(train, write_audit=False)
        evaluate = pipeline._apply_rotation_treatment_mode(
            evaluate, write_audit=False, historical_frame=train
        )

    stat_features = _stat_feature_columns(pipeline._features, train, base_feature_columns)
    train = _apply_feature_defaults(train, stat_features)
    evaluate = _apply_feature_defaults(evaluate, stat_features)

    models = StatModelSuite(pipeline._settings.training_seed, l1_alpha=pipeline._l1_alpha)
    models.fit(train, stat_features, l1_alpha=pipeline._l1_alpha)

    diagnostics = _aggregate_treatment_diagnostics(evaluate)
    diagnostics["mode_env"] = mode

    calibrators: dict[str, ProbabilityCalibrator | None]
    if skip_calibrator:
        calibrators = {mk: None for mk in MARKET_TARGETS}
    else:
        fitted = _fit_train_calibrators(
            pipeline=pipeline,
            train=train,
            stat_features=stat_features,
            models=models,
            pit_sample_size=pit_sample_size,
            rng_base_seed=rng_base_seed ^ 997,
        )
        calibrators = fitted

    detail_frames: list[pd.DataFrame] = []
    for market_key, target_column in MARKET_TARGETS.items():
        means, variances = models.models[market_key].predict(evaluate[evaluate["market_key"] == market_key])
        subset = evaluate[evaluate["market_key"] == market_key].copy()
        detail_frames.append(
            _simulate_row_metrics(
                pipeline,
                subset,
                means,
                variances,
                market_key,
                target_column,
                pit_sample_size=pit_sample_size,
                rng_base_seed=rng_base_seed,
                calibrator=calibrators.get(market_key),
            )
        )

    detail = pd.concat(detail_frames, ignore_index=True) if detail_frames else pd.DataFrame()

    rows: list[dict[str, Any]] = []
    slices = _slice_specs()
    for market_key, market_df in detail.groupby("market_key", dropna=False):
        overall_metrics = _metrics_for_subset(market_df)
        rows.append({"mode": mode, "market_key": market_key, "slice": "all", **overall_metrics})
        for slice_def in slices:
            metrics = _metrics_for_subset(slice_def.predicate(market_df))
            rows.append({"mode": mode, "market_key": market_key, "slice": slice_def.name, **metrics})
    summary = pd.DataFrame(rows)
    return summary, detail, diagnostics


def _cluster_bootstrap_rmse_improve(
    paired: pd.DataFrame,
    *,
    iters: int,
    seed: int,
) -> dict[str, float]:
    """Return distribution of pooled RMSE_off - RMSE_full (positive => full improves)."""
    if paired.empty or iters <= 0:
        return {"median": float("nan"), "p025": float("nan"), "p975": float("nan")}
    games = paired["game_id"].unique().astype(int)
    rng = np.random.default_rng(seed)

    deltas: list[float] = []

    actual = paired["actual"].to_numpy(dtype=float)
    po = paired["predicted_mean_off"].to_numpy(dtype=float)
    pf = paired["predicted_mean_full"].to_numpy(dtype=float)
    gid = paired["game_id"].to_numpy(dtype=int)

    for _ in range(iters):
        draw = rng.choice(games, size=len(games), replace=True)
        idx_chunks: list[np.ndarray] = []
        for g in draw.flatten():
            loc = np.flatnonzero(gid == int(g))
            if loc.size:
                idx_chunks.append(loc)
        if not idx_chunks:
            continue
        idx = np.concatenate(idx_chunks, axis=0)
        a = actual[idx]
        ao = po[idx]
        af = pf[idx]
        r_off = float(np.sqrt(np.mean((a - ao) ** 2)))
        r_full = float(np.sqrt(np.mean((a - af) ** 2)))
        deltas.append(r_off - r_full)

    arr = np.asarray(deltas, dtype=float)
    if arr.size == 0:
        return {"median": float("nan"), "p025": float("nan"), "p975": float("nan")}
    return {
        "median": float(np.median(arr)),
        "p025": float(np.quantile(arr, 0.025)),
        "p975": float(np.quantile(arr, 0.975)),
    }


def _pair_off_full_high_injury(detail_by_mode: dict[str, pd.DataFrame]) -> pd.DataFrame:
    if "off" not in detail_by_mode or "full" not in detail_by_mode:
        return pd.DataFrame()
    off_df = detail_by_mode["off"]
    fu_df = detail_by_mode["full"]
    off_hi = off_df[off_df["team_out_count"] >= 2]
    fu_hi = fu_df[fu_df["team_out_count"] >= 2]
    keys = ["game_id", "player_id", "market_key"]
    merged = off_hi.merge(fu_hi, on=keys, suffixes=("_off", "_full"), how="inner")
    if not merged.empty:
        merged = merged.assign(actual=merged["actual_off"])[[
            *keys,
            "actual",
            "predicted_mean_off",
            "predicted_mean_full",
        ]].copy()
    return merged


def _run_acceptance(
    *,
    summary: pd.DataFrame,
    paired_hi: pd.DataFrame,
    args: argparse.Namespace,
    bootstrap: dict[str, float],
    coverage_mean_full_hi: float,
    bootstrap_iters_ran: int,
    modes_requested: list[str],
    bootstrap_as_blocker: bool,
) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    hi = "team_out_count>=2"
    clean_slice = "team_out_count=0"
    compare_off_full = ("off" in modes_requested) and ("full" in modes_requested)

    subset_hi = summary[(summary["slice"] == hi) & (summary["market_key"] != "all")]
    subset_clean = summary[(summary["slice"] == clean_slice) & (summary["market_key"] != "all")]
    modes_off = subset_hi[subset_hi["mode"] == "off"]
    modes_fu = subset_hi[subset_hi["mode"] == "full"]

    if compare_off_full:
        for market_key in MARKET_TARGETS:
            om = modes_off[modes_off["market_key"] == market_key]
            fm = modes_fu[modes_fu["market_key"] == market_key]
            if om.empty or fm.empty:
                blockers.append(f"Missing high-injury metrics for `{market_key}`.")
                continue
            r_off = float(om.iloc[0]["rmse"])
            r_fu = float(fm.iloc[0]["rmse"])
            if not (np.isfinite(r_fu) and np.isfinite(r_off) and r_fu <= r_off + 1e-9):
                blockers.append(
                    f"High-injury RMSE `{market_key}` regression or tie break: full={r_fu:.4f}, off={r_off:.4f}."
                )
            ll_off = float(om.iloc[0]["log_loss"])
            ll_fu = float(fm.iloc[0]["log_loss"])
            if np.isfinite(ll_fu) and np.isfinite(ll_off) and ll_fu > ll_off + float(args.ll_slack):
                blockers.append(
                    f"High-injury log_loss `{market_key}`: full={ll_fu:.4f}, off={ll_off:.4f} "
                    f"(slack {args.ll_slack})."
                )

        oc = subset_clean[subset_clean["mode"] == "off"]
        fc = subset_clean[subset_clean["mode"] == "full"]
        for market_key in MARKET_TARGETS:
            om = oc[oc["market_key"] == market_key]
            fm = fc[fc["market_key"] == market_key]
            if om.empty or fm.empty:
                continue
            r_off = float(om.iloc[0]["rmse"])
            r_fu = float(fm.iloc[0]["rmse"])
            if np.isfinite(r_fu) and np.isfinite(r_off) and r_fu > r_off + 1e-9:
                blockers.append(
                    f"Clean-roster RMSE regression `{market_key}`: full={r_fu:.4f}, off={r_off:.4f}."
                )

    if "full" in modes_requested:
        if not np.isfinite(coverage_mean_full_hi):
            blockers.append("Coverage mean (high injury, full mode) unavailable.")
        else:
            cov_lo = float(args.coverage_low)
            cov_hi = float(args.coverage_high)
            if coverage_mean_full_hi < cov_lo or coverage_mean_full_hi > cov_hi:
                blockers.append(
                    f"High-injury mean p10–p90 coverage {coverage_mean_full_hi:.3f} outside [{cov_lo}, {cov_hi}]."
                )

    if (
        bootstrap_as_blocker
        and bootstrap_iters_ran > 0
        and not paired_hi.empty
        and ("off" in modes_requested)
        and ("full" in modes_requested)
    ):
        p_lo = bootstrap.get("p025", float("nan"))
        if np.isfinite(p_lo) and p_lo <= 0.0:
            blockers.append(
                f"Cluster-bootstrap lower 2.5% for pooled RMSE (off−full) is ≤0 ({p_lo:.5f}); "
                "difference may be noise versus control."
            )

    return len(blockers) == 0, blockers


def _phase_report(
    output_dir: Path,
    summary: pd.DataFrame,
    accepted: bool,
    blockers: list[str],
    *,
    bootstrap: dict[str, float],
    diagnostics_by_mode: dict[str, dict[str, Any]],
    coverage_band: tuple[float, float],
    coverage_observed_full_hi_mean: float,
) -> tuple[Path, Path]:
    phase_path = output_dir / "phase_change_report.md"
    bug_path = output_dir / "bug_report.md"

    diag_path = output_dir / "treatment_fire_diagnostics.json"
    diag_path.write_text(json.dumps(diagnostics_by_mode, indent=2, default=str) + "\n", encoding="utf-8")

    boot_path = output_dir / "bootstrap_high_injury.json"
    boot_path.write_text(json.dumps({"pooled_rmse_off_minus_full": bootstrap}, indent=2) + "\n", encoding="utf-8")

    high_injury = summary[(summary["slice"] == "team_out_count>=2") & (summary["market_key"] != "all")]
    mk_list = sorted({m for m in high_injury["market_key"].unique()}, key=str)

    lines_market_rmse_table: list[str] = []

    def _gather(mode: str) -> tuple[list[float], list[float]]:
        rms_, wm_ = [], []
        for mk in mk_list:
            row = high_injury[(high_injury["mode"] == mode) & (high_injury["market_key"] == mk)]
            if not row.empty:
                rms_.append(float(row.iloc[0]["rmse"]))
                wm_.append(float(row.iloc[0].get("weighted_rmse", float("nan"))))
        return rms_, wm_

    r_off_hi, wm_off = _gather("off")
    r_fu_hi, wm_fu = _gather("full")

    for mk in mk_list:
        rf = high_injury[(high_injury["mode"] == "full") & (high_injury["market_key"] == mk)]
        roff = high_injury[(high_injury["mode"] == "off") & (high_injury["market_key"] == mk)]
        if rf.empty or roff.empty:
            continue
        lines_market_rmse_table.append(
            f"| `{mk}` | off={float(roff.iloc[0]['rmse']):.4f}, full={float(rf.iloc[0]['rmse']):.4f} "
            f"| Δ={float(roff.iloc[0]['rmse']) - float(rf.iloc[0]['rmse']):+.4f} |"
        )

    # Normalized pooled comparison (weighted RMSE = RMSE/sqrt(mean var)) — scale-free across markets for monitoring.
    mean_weighted_fu = float(np.nanmean(np.asarray(wm_fu, dtype=float))) if wm_fu else float("nan")
    mean_weighted_off = float(np.nanmean(np.asarray(wm_off, dtype=float))) if wm_off else float("nan")

    cov_lo, cov_hi = coverage_band

    phase_lines = [
        "# Phase 7 Change Report (hardened)",
        "",
        f"- Generated: {datetime.now(UTC).isoformat()}",
        f"- Status: **{'go' if accepted else 'no-go'}**",
        f"- Acceptance uses per-market gates (high-injury RMSE+log_loss maxima, coverage band [{cov_lo}, {cov_hi}], clean RMSE,"
        " cluster bootstrap sanity).",
        f"- Mean p10–p90 coverage HIGH slice `full`: **{coverage_observed_full_hi_mean:.4f}**",
        "",
        "### High-injury slice (aggregate summary)",
        f"- Arithmetic mean RMSE across markets (`off`/`full`): {_safe_mean_pair(r_off_hi, r_fu_hi)}",
        f"- Mean weighted RMSE (~scale normalized) (`off`/`full`): {_fmt_pair(mean_weighted_off, mean_weighted_fu)}",
        "",
        "### Per-market high-injury RMSE",
        "",
        "| Market | RMSE pair | Δ (off − full) |",
        "|--------|-----------|----------------|",
        *[ln for ln in lines_market_rmse_table],
        "",
        (
            "### Cluster bootstrap (HIGH injury pooled RMSE, off − full)"
            f"\n- median: {bootstrap.get('median')}\n- 95% approx interval: [{bootstrap.get('p025')}, {bootstrap.get('p975')}]"
            if bootstrap
            else "### Cluster bootstrap unavailable"
        ),
        "",
        "## Treatment firing",
        "",
        f"- Diagnostics JSON: `{diag_path.name}`",
        "",
        "## Acceptance Gates",
        "",
        *[f"- {item}" for item in (blockers if blockers else ["All automated gates cleared."])],
        "",
        "## Artifacts",
        "",
        "- `ablation_summary.csv`, `ablation_details.csv`, `paired_high_injury_bootstrap.parquet`, `daily_by_game_date.csv`",
        f"- `{boot_path.name}`",
        "",
    ]
    phase_path.write_text("\n".join(phase_lines) + "\n", encoding="utf-8")

    bug_items = ["No automated gate regressions flagged in hardened acceptance."]
    bug_lines = [
        "# Phase 7 Gate Report",
        "",
        f"- Generated: {datetime.now(UTC).isoformat()}",
        "",
        "## Blockers",
        "",
    ]
    bug_lines.extend([f"- {item}" for item in (blockers if not accepted else bug_items)])
    bug_path.write_text("\n".join(bug_lines) + "\n", encoding="utf-8")
    return phase_path, bug_path


def _safe_mean_pair(a: list[float], b: list[float]) -> str:
    if not a or not b:
        return "n/a"
    return (
        f"off={float(np.nanmean(np.asarray(a, dtype=float))):.4f}, "
        f"full={float(np.nanmean(np.asarray(b, dtype=float))):.4f}"
    )


def _fmt_pair(x: float, y: float) -> str:
    if not (np.isfinite(x) and np.isfinite(y)):
        return "n/a"
    return f"off={x:.4f}, full={y:.4f}"


def main() -> None:
    args = _parse_args()
    modes = [m.strip() for m in str(args.modes).split(",") if m.strip()]
    valid_modes = {"off", "features-only", "full"}
    for mode in modes:
        if mode not in valid_modes:
            raise SystemExit(f"Unsupported mode '{mode}'. Valid modes: off, features-only, full")

    summaries: list[pd.DataFrame] = []
    details: list[pd.DataFrame] = []
    diag_by_mode: dict[str, dict[str, Any]] = {}

    with session_scope() as session:
        loader = DatasetLoader(session)
        historical = loader.load_historical_player_games()
        historical["game_date"] = pd.to_datetime(historical["game_date"], errors="coerce")
        historical = historical.dropna(subset=["game_date"]).sort_values("game_date").reset_index(drop=True)
        if historical.empty:
            raise SystemExit("Historical dataset is empty; cannot run Phase 7 backtest.")

        eval_end = historical["game_date"].max().date()
        eval_start = eval_end - timedelta(days=max(int(args.eval_days), 1) - 1)
        train_start = eval_start - timedelta(days=max(int(args.train_days), 1))
        train_raw = historical[
            (historical["game_date"].dt.date >= train_start) & (historical["game_date"].dt.date < eval_start)
        ].copy()
        eval_raw = historical[
            (historical["game_date"].dt.date >= eval_start) & (historical["game_date"].dt.date <= eval_end)
        ].copy()
        if int(args.max_eval_rows) > 0 and len(eval_raw) > int(args.max_eval_rows):
            eval_raw = eval_raw.tail(int(args.max_eval_rows)).copy()
        if train_raw.empty or eval_raw.empty:
            raise SystemExit("Train/eval split produced empty frame(s); adjust --train-days/--eval-days.")

        pipeline = TrainingPipeline(session)
        for mode in modes:
            summary, detail, diag = _evaluate_mode(
                pipeline,
                mode,
                train_raw,
                eval_raw,
                pit_sample_size=int(max(args.pit_sample_size, 100)),
                rng_base_seed=int(args.rng_base_seed),
                skip_calibrator=bool(args.no_calibrator),
            )
            summaries.append(summary)
            details.append(detail.assign(mode=mode))
            diag_by_mode[mode] = diag

    summary_df = pd.concat(summaries, ignore_index=True) if summaries else pd.DataFrame()
    detail_df = pd.concat(details, ignore_index=True) if details else pd.DataFrame()
    game_dates = (
        eval_raw[["game_id", "game_date"]]
        .drop_duplicates(subset=["game_id"])
        .assign(slate_date=lambda d: pd.to_datetime(d["game_date"]).dt.strftime("%Y-%m-%d"))
        .drop(columns=["game_date"])
    )
    detail_dated = detail_df.merge(game_dates, on="game_id", how="left") if not detail_df.empty else detail_df.copy()

    run_dir = ROOT / "reports" / "rotation_calibration" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(run_dir / "ablation_summary.csv", index=False)
    detail_df.to_csv(run_dir / "ablation_details.csv", index=False)

    if not detail_dated.empty:
        daily_agg: list[dict[str, Any]] = []
        for (mode, slate, mk), grp in detail_dated.groupby(["mode", "slate_date", "market_key"], dropna=False):
            reg = compute_regression_metrics(
                grp["actual"].to_numpy(dtype=float),
                grp["predicted_mean"].to_numpy(dtype=float),
            )
            daily_agg.append(
                {
                    "mode": mode,
                    "slate_date": slate,
                    "market_key": mk,
                    "rows": int(len(grp)),
                    "rmse": float(reg.rmse),
                    "mae": float(reg.mae),
                    "mean_team_out_count": float(grp["team_out_count"].mean()),
                }
            )
        pd.DataFrame(daily_agg).sort_values(["slate_date", "mode", "market_key"]).to_csv(
            run_dir / "daily_by_game_date.csv", index=False
        )

    paired = _pair_off_full_high_injury({m: detail_df.loc[detail_df["mode"] == m].copy() for m in modes})

    if not paired.empty:
        pq_path = run_dir / "paired_high_injury_bootstrap.parquet"
        try:
            paired.to_parquet(pq_path, index=False)
        except Exception:
            paired.to_csv(run_dir / "paired_high_injury_bootstrap.csv", index=False)

    bootstrap = _cluster_bootstrap_rmse_improve(paired, iters=int(args.bootstrap_iters), seed=42)

    coverage_mean_full_hi = float("nan")
    subcov = summary_df[
        (summary_df["mode"] == "full")
        & (summary_df["slice"] == "team_out_count>=2")
        & (summary_df["market_key"] != "all")
    ]
    if not subcov.empty:
        coverage_mean_full_hi = float(pd.to_numeric(subcov["p10_p90_coverage"], errors="coerce").mean())

    accepted, gate_blockers = _run_acceptance(
        summary=summary_df,
        paired_hi=paired,
        args=args,
        bootstrap=bootstrap,
        coverage_mean_full_hi=coverage_mean_full_hi,
        bootstrap_iters_ran=int(args.bootstrap_iters),
        modes_requested=modes,
        bootstrap_as_blocker=bool(args.bootstrap_as_blocker),
    )

    boot_hint = ""
    if (
        int(args.bootstrap_iters) > 0
        and modes and "off" in modes
        and "full" in modes
        and np.isfinite(bootstrap.get("p025", np.nan))
    ):
        p_lo = float(bootstrap["p025"])
        if p_lo <= 0:
            boot_hint = (
                f"\n- **Advisory**: bootstrap lower 2.5% = {p_lo:.5f} (CI includes zero at default settings; "
                "pass `--bootstrap-as-blocker` to treat as gate).\n"
            )

    ablation_md = [
        "# Rotation Shock Backtest (Phase 7 hardened)",
        "",
        f"- Generated: {datetime.now(UTC).isoformat()}",
        f"- Eval window: {int(args.eval_days)} days",
        f"- Train window: {int(args.train_days)} days",
        f"- PIT draws: {int(args.pit_sample_size)} empirical CDF samples",
        f"- Calibration: {'raw' if args.no_calibrator else 'train-fit ProbabilityCalibrator'}",
        f"- Acceptance: **`{'GO' if accepted else 'NO-GO'}`**",
        boot_hint,
        "",
        "### Cluster bootstrap pooled RMSE (off − full, high injury)",
        json.dumps(bootstrap, indent=2),
        "",
    ]
    (run_dir / "ablation_report.md").write_text("\n".join(ablation_md) + "\n", encoding="utf-8")

    ph, bh = _phase_report(
        run_dir,
        summary_df,
        accepted,
        gate_blockers,
        bootstrap=bootstrap,
        diagnostics_by_mode=diag_by_mode,
        coverage_band=(float(args.coverage_low), float(args.coverage_high)),
        coverage_observed_full_hi_mean=coverage_mean_full_hi,
    )

    print(f"Wrote {run_dir}")
    print(f"Status: {'go' if accepted else 'no-go'}")
    for p in sorted(run_dir.glob("*")):
        print(f"  {p.relative_to(run_dir)}")
    print(ph)
    print(bh)


if __name__ == "__main__":
    main()
