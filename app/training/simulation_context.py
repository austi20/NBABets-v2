from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

_CONTEXT_SPECS: dict[str, tuple[tuple[str, ...], float, float]] = {
    "usage_rate": (
        ("adjusted_usage_rate", "usage_rate_recency_weighted", "usage_rate_blended", "usage_rate_avg_10", "usage_rate"),
        0.20,
        0.01,
    ),
    "touches_per_minute": (
        (
            "adjusted_touches_per_minute",
            "touches_per_minute_recency_weighted",
            "touches_per_minute_blended",
            "touches_per_minute_avg_10",
            "touches_per_minute",
        ),
        0.0,
        0.0,
    ),
    "passes_per_minute": (
        (
            "adjusted_passes_per_minute",
            "passes_per_minute_recency_weighted",
            "passes_per_minute_blended",
            "passes_per_minute_avg_10",
            "passes_per_minute",
        ),
        0.0,
        0.0,
    ),
    "assist_creation_proxy_per_minute": (
        (
            "adjusted_assist_creation_proxy_per_minute",
            "assist_creation_proxy_per_minute_recency_weighted",
            "assist_creation_proxy_per_minute_blended",
            "assist_creation_proxy_per_minute_avg_10",
            "assist_creation_proxy_per_minute",
        ),
        0.0,
        0.0,
    ),
    "rebound_chances_total_per_minute": (
        (
            "adjusted_rebound_chances_total_per_minute",
            "rebound_chances_total_per_minute_recency_weighted",
            "rebound_chances_total_per_minute_blended",
            "rebound_chances_total_per_minute_avg_10",
            "rebound_chances_total_per_minute",
        ),
        0.0,
        0.0,
    ),
    "rebound_conversion_rate": (
        (
            "rebound_conversion_rate_recency_weighted",
            "rebound_conversion_rate_blended",
            "rebound_conversion_rate_avg_10",
            "rebound_conversion_rate",
        ),
        0.35,
        0.01,
    ),
    "field_goal_attempts_per_minute": (
        (
            "adjusted_field_goal_attempts_per_minute",
            "field_goal_attempts_per_minute_recency_weighted",
            "field_goal_attempts_per_minute_blended",
            "field_goal_attempts_per_minute_avg_10",
            "field_goal_attempts_per_minute",
        ),
        0.0,
        0.0,
    ),
    "free_throw_attempts_per_minute": (
        (
            "adjusted_free_throw_attempts_per_minute",
            "free_throw_attempts_per_minute_recency_weighted",
            "free_throw_attempts_per_minute_blended",
            "free_throw_attempts_per_minute_avg_10",
            "free_throw_attempts_per_minute",
        ),
        0.02,
        0.0,
    ),
    "estimated_three_point_attempts_per_minute": (
        (
            "adjusted_estimated_three_point_attempts_per_minute",
            "estimated_three_point_attempts_per_minute_recency_weighted",
            "estimated_three_point_attempts_per_minute_blended",
            "estimated_three_point_attempts_per_minute_avg_10",
            "estimated_three_point_attempts_per_minute",
        ),
        0.0,
        0.0,
    ),
    "percentage_field_goals_attempted_3pt": (
        (
            "percentage_field_goals_attempted_3pt_recency_weighted",
            "percentage_field_goals_attempted_3pt_blended",
            "percentage_field_goals_attempted_3pt_avg_10",
            "percentage_field_goals_attempted_3pt",
        ),
        0.35,
        0.01,
    ),
    "true_shooting_percentage": (
        (
            "true_shooting_percentage_recency_weighted",
            "true_shooting_percentage_blended",
            "true_shooting_percentage_avg_10",
            "true_shooting_percentage",
        ),
        0.58,
        0.01,
    ),
    "turnover_ratio": (
        ("turnover_ratio_recency_weighted", "turnover_ratio_blended", "turnover_ratio_avg_10", "turnover_ratio"),
        0.12,
        0.01,
    ),
    "points_pace_exposure": (("points_pace_exposure",), 0.0, 0.0),
    "points_3pt_variance": (("points_3pt_variance",), 0.0, 0.0),
    "rebounds_std_10": (("rebounds_std_recency_weighted", "rebounds_std_10"), 0.0, 0.0),
}

_THREES_PER_MINUTE_KEYS = (
    "threes_per_minute_recency_weighted",
    "threes_per_minute_blended",
    "threes_per_minute_avg_10",
    "threes_per_minute",
)
_ASSISTS_PER_MINUTE_KEYS = (
    "assists_per_minute_recency_weighted",
    "assists_per_minute_blended",
    "assists_per_minute_avg_10",
    "assists_per_minute",
)


def build_simulation_context(
    row: dict[str, Any] | pd.Series,
    *,
    market_key: str | None = None,
) -> dict[str, float]:
    context = {
        name: _context_float(row, keys, fallback=fallback, minimum=minimum)
        for name, (keys, fallback, minimum) in _CONTEXT_SPECS.items()
    }
    threes_per_minute = _context_float(row, _THREES_PER_MINUTE_KEYS, fallback=0.0, minimum=0.0)
    if context["estimated_three_point_attempts_per_minute"] > 1e-6 and threes_per_minute > 0.0:
        context["three_point_make_rate"] = threes_per_minute / max(
            context["estimated_three_point_attempts_per_minute"],
            1e-6,
        )
    else:
        context["three_point_make_rate"] = 0.35

    if market_key in (None, "assists"):
        assists_rate = _context_float(row, _ASSISTS_PER_MINUTE_KEYS, fallback=0.0, minimum=0.0)
        if context["assist_creation_proxy_per_minute"] > 1e-6 and assists_rate > 0.0:
            context["assist_conversion_rate"] = assists_rate / max(
                context["assist_creation_proxy_per_minute"],
                1e-6,
            )
        else:
            context["assist_conversion_rate"] = 0.32
    return context


def _context_float(
    row: dict[str, Any] | pd.Series,
    keys: tuple[str, ...],
    *,
    fallback: float,
    minimum: float = 0.0,
) -> float:
    getter = row.get if isinstance(row, dict) else row.get
    for key in keys:
        try:
            numeric = float(getter(key))
        except (TypeError, ValueError):
            continue
        if not np.isfinite(numeric):
            continue
        if key.startswith("adjusted_") and numeric >= 0.0:
            return numeric
        if numeric >= minimum:
            return numeric
    return float(fallback)
