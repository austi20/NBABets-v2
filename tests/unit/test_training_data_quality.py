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
