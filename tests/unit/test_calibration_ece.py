from __future__ import annotations

import numpy as np

from app.config.settings import Settings
from app.training.calibration_ece import (
    compute_ece,
    dump_calibration_oof,
    load_calibration_oof,
    market_ece_gate,
    null_ece_distribution,
    null_floor,
    summarize_fold_subsets,
)


def _draw_labels(probs: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    return (rng.random(probs.size) < probs).astype(float)


def test_compute_ece_matches_hand_value() -> None:
    ece = compute_ece(np.array([0.2, 0.8]), np.array([0, 1]))
    assert abs(ece - 0.2) < 1e-9


def test_compute_ece_empty_is_zero() -> None:
    assert compute_ece(np.array([]), np.array([])) == 0.0


def test_compute_ece_perfect_bins_zero() -> None:
    probs = np.full(1000, 0.5)
    labels = np.concatenate([np.ones(500), np.zeros(500)])
    assert compute_ece(probs, labels) < 1e-9


def test_perfect_calibration_passes_floor() -> None:
    probs = np.random.default_rng(0).uniform(0.1, 0.9, 5000)
    labels = _draw_labels(probs, np.random.default_rng(1))
    floor = null_floor(probs, percentile=99.0, n_boot=1200, rng=np.random.default_rng(2))
    assert compute_ece(probs, labels) <= floor


def test_miscalibration_breaches_floor() -> None:
    probs = np.random.default_rng(0).uniform(0.1, 0.9, 5000)
    shifted = np.clip(probs + 0.15, 0.0, 1.0)
    labels = _draw_labels(shifted, np.random.default_rng(1))
    floor = null_floor(probs, percentile=99.0, n_boot=1200, rng=np.random.default_rng(2))
    assert compute_ece(probs, labels) > floor


def test_floor_decreases_with_sample_size() -> None:
    small_probs = np.random.default_rng(0).uniform(0.1, 0.9, 500)
    large_probs = np.random.default_rng(0).uniform(0.1, 0.9, 20000)
    small = null_floor(small_probs, n_boot=700, rng=np.random.default_rng(3))
    large = null_floor(large_probs, n_boot=700, rng=np.random.default_rng(3))
    assert large < small


def test_null_floor_deterministic() -> None:
    probs = np.random.default_rng(0).uniform(0.1, 0.9, 2000)
    first = null_floor(probs, n_boot=700, rng=np.random.default_rng(7))
    second = null_floor(probs, n_boot=700, rng=np.random.default_rng(7))
    assert first == second


def test_null_ece_distribution_length() -> None:
    probs = np.random.default_rng(0).uniform(0.1, 0.9, 200)
    dist = null_ece_distribution(probs, n_boot=25, rng=np.random.default_rng(1))
    assert dist.shape == (25,)


def test_gate_passes_for_perfect_calibration() -> None:
    probs = np.random.default_rng(0).uniform(0.1, 0.9, 5000)
    labels = _draw_labels(probs, np.random.default_rng(1))
    gate = market_ece_gate(probs, labels, percentile=99.0, n_boot=1000, rng=np.random.default_rng(2))
    assert gate["passed"] is True
    assert gate["sample_count"] == 5000
    assert gate["null_floor"] >= gate["null_floor_p95"]
    assert gate["null_floor_p99"] >= gate["null_floor_p95"]
    assert 0.0 <= gate["ece"] <= 1.0


def test_gate_fails_for_miscalibration() -> None:
    probs = np.random.default_rng(0).uniform(0.1, 0.9, 5000)
    shifted = np.clip(probs + 0.15, 0.0, 1.0)
    labels = _draw_labels(shifted, np.random.default_rng(1))
    gate = market_ece_gate(probs, labels, percentile=99.0, n_boot=1000, rng=np.random.default_rng(2))
    assert gate["passed"] is False


def test_dump_then_load_roundtrip(tmp_path) -> None:
    oof_raw = {"points": [np.array([0.2, 0.8]), np.array([0.4])]}
    oof_labels = {"points": [np.array([0, 1]), np.array([1])]}
    path = tmp_path / "dump.json"
    dump_calibration_oof(oof_raw, oof_labels, path, timing={"fold_seconds": [1.0, 2.0]})
    loaded = load_calibration_oof(path)
    assert loaded["markets"]["points"]["fold_probs"][0] == [0.2, 0.8]
    assert loaded["markets"]["points"]["fold_labels"][1] == [1.0]
    assert loaded["timing"]["fold_seconds"] == [1.0, 2.0]


def test_calibration_ece_settings_defaults() -> None:
    settings = Settings(_env_file=None)
    assert settings.calibration_ece_percentile == 99.0
    assert settings.calibration_ece_n_boot == 2000
    assert settings.calibration_oof_max_folds == 20
    assert settings.calibration_dump_oof is False


def test_summarize_fold_subsets_shapes() -> None:
    rng = np.random.default_rng(0)
    fold_probs = [rng.uniform(0.1, 0.9, 400) for _ in range(8)]
    fold_labels = [
        (np.random.default_rng(index).random(400) < probs).astype(float)
        for index, probs in enumerate(fold_probs)
    ]
    rows = summarize_fold_subsets(
        fold_probs,
        fold_labels,
        fold_counts=[2, 4, 8],
        percentile=99.0,
        n_boot=500,
        rng=np.random.default_rng(1),
    )
    assert [row["fold_count"] for row in rows] == [2, 4, 8]
    for row in rows:
        assert set(row) >= {"fold_count", "sample_count", "ece", "null_floor", "passed"}
    assert rows[0]["sample_count"] < rows[-1]["sample_count"]
