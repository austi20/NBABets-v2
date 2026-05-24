"""AG-TECH-004: explicit market feature prefix / embedded-token allowlists."""

from __future__ import annotations

import pandas as pd

from app.training.models import (
    _market_feature_column_match,
    _market_feature_columns,
)


def test_market_feature_prefix_not_substring_false_positive() -> None:
    # Legacy ``"pace" in column`` matched arbitrary substrings; allowlist does not.
    assert not _market_feature_column_match("team_apace_metric", "points")
    assert _market_feature_column_match("team_pace_avg_10", "points")


def test_embedded_assist_and_turnover_tokens() -> None:
    assert _market_feature_column_match("assist_ratio_blended", "assists")
    assert _market_feature_column_match("turnover_ratio_blended", "turnovers")


def test_group_columns_per_market() -> None:
    assert _market_feature_column_match("points_group_minutes_exposure", "points")
    assert not _market_feature_column_match("points_group_minutes_exposure", "rebounds")


def test_market_feature_columns_respects_excludes_and_predicted_minutes() -> None:
    cols = [
        "points_avg_10",
        "points_consensus_prob_mean",
        "points_line_delta_5",
        "recent_hit_rate_points_10",
        "sportsbook_count",
        "raw_implied_over_probability",
        "no_vig_over_probability",
        "minutes_blended",
        "predicted_minutes",
        "noise_xyz",
    ]
    frame = pd.DataFrame({c: [0.0] for c in cols})
    picked = _market_feature_columns(frame, "points", cols)
    assert "points_avg_10" in picked
    assert "minutes_blended" in picked
    assert "predicted_minutes" in picked
    assert "points_consensus_prob_mean" not in picked
    assert "points_line_delta_5" not in picked
    assert "recent_hit_rate_points_10" not in picked
    assert "sportsbook_count" not in picked
    assert "raw_implied_over_probability" not in picked
    assert "no_vig_over_probability" not in picked
    assert "noise_xyz" not in picked
