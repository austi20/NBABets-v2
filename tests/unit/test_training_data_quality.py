from __future__ import annotations

import pandas as pd

from app.training.pipeline import _training_data_quality_checks


def test_training_data_quality_checks_flags_missing_features() -> None:
    frame = pd.DataFrame(
        {
            "minutes": [20.0, 30.0, 25.0],
            "feature_a": [1.0, 2.0, 3.0],
        }
    )
    result = _training_data_quality_checks(frame, ["feature_a", "feature_b"])

    assert result["status"] in {"healthy", "degraded"}
    assert int(result["missing_feature_columns"]) == 1


def test_empty_provider_only_columns_do_not_degrade_quality() -> None:
    # parquet history has no pace or rating columns, so they are all NaN
    frame = pd.DataFrame(
        {
            "minutes": [20.0] * 10,
            "feature_a": [float(i) for i in range(10)],
            "provider_pace": [float("nan")] * 10,
            "provider_rating": [float("nan")] * 10,
        }
    )
    result = _training_data_quality_checks(frame, ["feature_a"])

    assert result["status"] == "healthy"
    assert float(result["numeric_finite_ratio"]) == 1.0


def test_non_finite_feature_cells_still_degrade_quality() -> None:
    frame = pd.DataFrame(
        {
            "minutes": [20.0] * 10,
            "feature_a": [float("nan")] * 4 + [1.0] * 6,
        }
    )
    result = _training_data_quality_checks(frame, ["feature_a"])

    assert result["status"] == "degraded"
    assert "low_finite_ratio" in result["reasons"]
