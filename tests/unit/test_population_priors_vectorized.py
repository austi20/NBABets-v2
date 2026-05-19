"""Parity test: vectorized _fill_with_population_priors == legacy iterrows loop."""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.training.features import FeatureEngineer, role_bucket_label


def _legacy_fill(
    frame: pd.DataFrame,
    priors: dict | None,
    tier_col: str,
    feature_columns: list[str],
    role_bucket_for_row,
) -> pd.DataFrame:
    """Reference impl mirroring the old iterrows() loop exactly."""
    if frame.empty or not priors:
        return frame
    result = frame.copy()
    available_columns = [c for c in feature_columns if c in result.columns]
    if not available_columns:
        return result
    position_feature_priors = priors.get("position_feature_priors", {})
    role_feature_priors = priors.get("role_feature_priors", {})
    global_feature_priors = priors.get("global_feature_priors", {})

    for idx, row in result.iterrows():
        tier = str(row.get(tier_col, "A"))
        if tier not in {"C", "D"}:
            continue
        position_group = str(row.get("position_group", "UNK") or "UNK").upper()[:1] or "UNK"
        position_values = position_feature_priors.get(position_group, {})
        role_values = (
            role_feature_priors.get(role_bucket_for_row(row, priors), {})
            if tier == "D"
            else {}
        )
        for column in available_columns:
            if tier == "D":
                prior_value = role_values.get(column, position_values.get(column, global_feature_priors.get(column)))
                if prior_value is not None and pd.notna(prior_value):
                    result.at[idx, column] = float(prior_value)
            elif pd.isna(result.at[idx, column]):
                prior_value = position_values.get(column, global_feature_priors.get(column))
                if prior_value is not None and pd.notna(prior_value):
                    result.at[idx, column] = float(prior_value)
    return result


def _build_frame(rng: np.random.Generator, n: int) -> pd.DataFrame:
    tiers = rng.choice(["A", "B", "C", "D"], size=n, p=[0.2, 0.2, 0.3, 0.3])
    positions = rng.choice(["G", "F", "C", "UNK", None, ""], size=n)
    return pd.DataFrame(
        {
            "data_sufficiency_tier": tiers,
            "position_group": positions,
            "projected_starter_flag": rng.choice([0.0, 1.0, np.nan], size=n),
            "starter_flag": rng.choice([0.0, 1.0, np.nan], size=n),
            "line_value": rng.uniform(0, 30, size=n),
            "market_key": rng.choice(["points", "rebounds", "assists"], size=n),
            "feat_a": rng.choice([1.5, np.nan, 2.7, np.nan], size=n),
            "feat_b": rng.choice([0.3, np.nan, np.nan, 4.2], size=n),
            "feat_c": rng.choice([np.nan, np.nan, 1.1, 0.9], size=n),
        }
    )


def _build_priors() -> dict:
    return {
        "position_feature_priors": {
            "G": {"feat_a": 10.0, "feat_b": 20.0},
            "F": {"feat_a": 11.0, "feat_c": 30.0},
            "C": {"feat_b": 21.0},
            "U": {"feat_a": 99.0},  # position_group "UNK" gets first letter "U"
        },
        "role_feature_priors": {
            role_bucket_label("G", 1.0): {"feat_a": 100.0, "feat_b": 200.0, "feat_c": 300.0},
            role_bucket_label("G", 0.0): {"feat_a": 110.0},
            role_bucket_label("F", 1.0): {"feat_b": 210.0},
            role_bucket_label("F", 0.0): {"feat_c": 310.0},
            role_bucket_label("C", 1.0): {"feat_a": 120.0},
        },
        "global_feature_priors": {"feat_a": 1.0, "feat_b": 2.0, "feat_c": 3.0},
        "role_bucket_thresholds": {"points": 15.0, "rebounds": 8.0, "assists": 6.0},
    }


def test_parity_with_legacy_loop():
    rng = np.random.default_rng(7)
    frame = _build_frame(rng, 100)
    priors = _build_priors()
    fe = FeatureEngineer()
    columns = ["feat_a", "feat_b", "feat_c"]
    out_new = fe._fill_with_population_priors(frame, priors, "data_sufficiency_tier", columns)
    out_legacy = _legacy_fill(frame, priors, "data_sufficiency_tier", columns, fe._population_role_bucket_for_row)
    pd.testing.assert_frame_equal(
        out_new.reset_index(drop=True),
        out_legacy.reset_index(drop=True),
        check_exact=False,
        atol=1e-12,
    )


def test_empty_frame():
    fe = FeatureEngineer()
    out = fe._fill_with_population_priors(pd.DataFrame(), _build_priors(), "tier", ["x"])
    assert out.empty


def test_no_priors():
    fe = FeatureEngineer()
    frame = pd.DataFrame({"data_sufficiency_tier": ["C"], "feat_a": [np.nan]})
    out = fe._fill_with_population_priors(frame, None, "data_sufficiency_tier", ["feat_a"])
    pd.testing.assert_frame_equal(out, frame)


def test_all_tier_a_returns_unchanged():
    fe = FeatureEngineer()
    frame = pd.DataFrame(
        {
            "data_sufficiency_tier": ["A", "A", "B"],
            "position_group": ["G", "F", "C"],
            "feat_a": [np.nan, 1.0, 2.0],
        }
    )
    out = fe._fill_with_population_priors(frame, _build_priors(), "data_sufficiency_tier", ["feat_a"])
    pd.testing.assert_frame_equal(out, frame)


def test_missing_position_group_column():
    fe = FeatureEngineer()
    frame = pd.DataFrame({"data_sufficiency_tier": ["C"], "feat_a": [np.nan]})
    out = fe._fill_with_population_priors(frame, _build_priors(), "data_sufficiency_tier", ["feat_a"])
    # position_group missing -> default "UNK" -> first letter "U" -> position prior 99.0
    assert out["feat_a"].iloc[0] == 99.0


def test_missing_tier_column_no_op():
    fe = FeatureEngineer()
    frame = pd.DataFrame({"position_group": ["G"], "feat_a": [np.nan]})
    out = fe._fill_with_population_priors(frame, _build_priors(), "data_sufficiency_tier", ["feat_a"])
    # No tier column -> defaults to "A" -> nothing filled
    assert pd.isna(out["feat_a"].iloc[0])
