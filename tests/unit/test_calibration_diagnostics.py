from __future__ import annotations

import numpy as np

from app.training.pipeline import _edge_bucket_diagnostics


def test_edge_bucket_diagnostics_returns_expected_keys() -> None:
    probs = np.array([0.1, 0.2, 0.49, 0.51, 0.8, 0.9], dtype=float)
    labels = np.array([0, 1, 0, 1, 1, 1], dtype=int)

    diagnostics = _edge_bucket_diagnostics(probs, labels)

    assert set(diagnostics) == {"low_edge", "medium_edge", "high_edge"}
    assert diagnostics["low_edge"]["sample_count"] >= diagnostics["high_edge"]["sample_count"]
    assert "brier" in diagnostics["medium_edge"]
    assert "accuracy" in diagnostics["medium_edge"]
