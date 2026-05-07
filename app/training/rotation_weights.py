from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd

LEAGUE_TEAM_SENTINEL = "LEAGUE"
EPSILON = 1e-9


@dataclass(frozen=True)
class RotationWeightLearningSummary:
    observation_count: int
    team_cell_count: int
    league_cell_count: int
    fallback_cell_count: int


def aggregate_rotation_weights(observations: pd.DataFrame, *, last_updated: datetime | None = None) -> pd.DataFrame:
    updated_at = last_updated or datetime.now(UTC)
    if observations.empty:
        return pd.DataFrame(
            columns=[
                "team_id",
                "season",
                "absent_archetype",
                "candidate_archetype",
                "minute_gain_weight",
                "usage_gain_weight",
                "minute_delta_mean",
                "usage_delta_mean",
                "minute_delta_variance",
                "usage_delta_variance",
                "sample_size",
                "weight_source",
                "last_updated",
            ]
        )

    required = {
        "team_id",
        "season",
        "absent_archetype",
        "candidate_archetype",
        "minute_delta",
        "usage_delta",
    }
    missing = required.difference(observations.columns)
    if missing:
        missing_cols = ", ".join(sorted(missing))
        raise ValueError(f"Missing required observation columns: {missing_cols}")

    base = observations.copy()
    base["minute_delta"] = pd.to_numeric(base["minute_delta"], errors="coerce").fillna(0.0)
    base["usage_delta"] = pd.to_numeric(base["usage_delta"], errors="coerce").fillna(0.0)
    base = base.dropna(subset=["team_id", "season", "absent_archetype", "candidate_archetype"])

    grouped = (
        base.groupby(["team_id", "season", "absent_archetype", "candidate_archetype"], dropna=False)
        .agg(
            minute_delta_mean=("minute_delta", "mean"),
            usage_delta_mean=("usage_delta", "mean"),
            minute_delta_variance=("minute_delta", _sample_variance_or_nan),
            usage_delta_variance=("usage_delta", _sample_variance_or_nan),
            sample_size=("minute_delta", "size"),
        )
        .reset_index()
    )
    grouped["weight_source"] = "team"
    grouped["minute_gain_weight"] = 0.0
    grouped["usage_gain_weight"] = 0.0
    grouped = _apply_inverse_variance_weights(grouped)

    league = (
        base.groupby(["season", "absent_archetype", "candidate_archetype"], dropna=False)
        .agg(
            minute_delta_mean=("minute_delta", "mean"),
            usage_delta_mean=("usage_delta", "mean"),
            minute_delta_variance=("minute_delta", _sample_variance_or_nan),
            usage_delta_variance=("usage_delta", _sample_variance_or_nan),
            sample_size=("minute_delta", "size"),
        )
        .reset_index()
    )
    league["team_id"] = LEAGUE_TEAM_SENTINEL
    league["weight_source"] = "league"
    league["minute_gain_weight"] = 0.0
    league["usage_gain_weight"] = 0.0
    league = _apply_inverse_variance_weights(league)

    combined = pd.concat([grouped, league], ignore_index=True, sort=False)
    combined["minute_gain_weight"] = pd.to_numeric(combined["minute_gain_weight"], errors="coerce").fillna(0.0).clip(lower=0.0)
    combined["usage_gain_weight"] = pd.to_numeric(combined["usage_gain_weight"], errors="coerce").fillna(0.0).clip(lower=0.0)
    combined["sample_size"] = pd.to_numeric(combined["sample_size"], errors="coerce").fillna(0).astype(int)
    combined["last_updated"] = pd.Timestamp(updated_at)
    # team_id is mixed int/str ("LEAGUE" sentinel) — keep as object so parquet writes cleanly
    combined["team_id"] = combined["team_id"].astype(str)
    return combined[
        [
            "team_id",
            "season",
            "absent_archetype",
            "candidate_archetype",
            "minute_gain_weight",
            "usage_gain_weight",
            "minute_delta_mean",
            "usage_delta_mean",
            "minute_delta_variance",
            "usage_delta_variance",
            "sample_size",
            "weight_source",
            "last_updated",
        ]
    ].sort_values(
        ["weight_source", "team_id", "season", "absent_archetype", "candidate_archetype"]
    ).reset_index(drop=True)


def _apply_inverse_variance_weights(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    group_keys = ["team_id", "season", "absent_archetype"]
    for _, group in result.groupby(group_keys, dropna=False):
        idx = group.index
        minute_weights = _normalized_iv_weights(group["minute_delta_variance"], group["sample_size"], group["minute_delta_mean"])
        usage_weights = _normalized_iv_weights(group["usage_delta_variance"], group["sample_size"], group["usage_delta_mean"])
        result.loc[idx, "minute_gain_weight"] = minute_weights
        result.loc[idx, "usage_gain_weight"] = usage_weights
        if (group["sample_size"] < 2).any() or group["minute_delta_variance"].isna().any() or group["usage_delta_variance"].isna().any():
            result.loc[idx, "weight_source"] = result.loc[idx, "weight_source"].replace({"team": "fallback", "league": "league"})
    return result


def _normalized_iv_weights(variance: pd.Series, sample_size: pd.Series, mean_signal: pd.Series) -> np.ndarray:
    scores = np.zeros(len(variance), dtype=float)
    for i, (var, n, mean_value) in enumerate(zip(variance.to_numpy(), sample_size.to_numpy(), mean_signal.to_numpy(), strict=False)):
        positive_mean = max(float(mean_value), 0.0)
        if n >= 2 and np.isfinite(var) and float(var) > EPSILON:
            relative_variance = float(var) / max(positive_mean * positive_mean, EPSILON)
            scores[i] = positive_mean / (1.0 + relative_variance)
        else:
            scores[i] = positive_mean
    total = float(scores.sum())
    if total <= EPSILON:
        return np.full(len(scores), 1.0 / max(len(scores), 1), dtype=float)
    return scores / total


def _sample_variance_or_nan(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if len(numeric) < 2:
        return float("nan")
    return float(np.var(numeric.to_numpy(), ddof=1))


def summarize_weight_learning(observations: pd.DataFrame, weights: pd.DataFrame) -> RotationWeightLearningSummary:
    team_rows = weights[weights["weight_source"].isin(["team", "fallback"])]
    league_rows = weights[weights["weight_source"] == "league"]
    fallback_rows = weights[weights["weight_source"] == "fallback"]
    return RotationWeightLearningSummary(
        observation_count=int(len(observations)),
        team_cell_count=int(len(team_rows)),
        league_cell_count=int(len(league_rows)),
        fallback_cell_count=int(len(fallback_rows)),
    )


def to_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    return frame.to_dict("records")
