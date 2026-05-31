from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

DEFAULT_N_BINS = 10


def select_folds(fold_arrays: list, n_folds: int) -> list:
    """Return the n_folds most-recent arrays from fold_arrays (tail slice).

    Both the production calibration pipeline and the ECE sweep must use the
    same direction so that the fold_count table produced by the sweep matches
    what production actually trains on.
    """
    if n_folds <= 0:
        return []
    return fold_arrays[-n_folds:]


def compute_ece(probs: np.ndarray, labels: np.ndarray, n_bins: int = DEFAULT_N_BINS) -> float:
    """Count-weighted absolute calibration gap over equal-width bins on [0, 1]."""
    if n_bins <= 0:
        raise ValueError("n_bins must be positive")
    probs = np.asarray(probs, dtype=float)
    labels = np.asarray(labels, dtype=float)
    if probs.shape != labels.shape:
        raise ValueError("probs and labels must have the same shape")
    if probs.size == 0:
        return 0.0

    bin_edges = np.linspace(0.0, 1.0, n_bins + 1)
    bin_indices = np.digitize(probs, bin_edges, right=True).clip(1, n_bins) - 1
    ece = 0.0
    for bin_index in range(n_bins):
        mask = bin_indices == bin_index
        if not mask.any():
            continue
        bin_confidence = float(probs[mask].mean())
        bin_accuracy = float(labels[mask].mean())
        ece += int(mask.sum()) * abs(bin_confidence - bin_accuracy)
    return float(ece / max(probs.size, 1))


def null_ece_distribution(
    probs: np.ndarray,
    *,
    n_bins: int = DEFAULT_N_BINS,
    n_boot: int = 2000,
    rng: np.random.Generator,
) -> np.ndarray:
    """ECE values produced by a perfectly calibrated model at this probability vector."""
    n_boot = int(n_boot)
    if n_boot <= 0:
        raise ValueError("n_boot must be positive")
    probs = np.asarray(probs, dtype=float)
    draw_probs = np.clip(probs, 0.0, 1.0)
    out = np.empty(n_boot, dtype=float)
    for index in range(n_boot):
        labels = (rng.random(draw_probs.size) < draw_probs).astype(float)
        out[index] = compute_ece(probs, labels, n_bins=n_bins)
    return out


def null_floor(
    probs: np.ndarray,
    *,
    percentile: float = 99.0,
    n_bins: int = DEFAULT_N_BINS,
    n_boot: int = 2000,
    rng: np.random.Generator,
) -> float:
    """Percentile floor of the null ECE distribution for this probability vector."""
    dist = null_ece_distribution(probs, n_bins=n_bins, n_boot=n_boot, rng=rng)
    return float(np.percentile(dist, percentile))


def market_ece_gate(
    probs: np.ndarray,
    labels: np.ndarray,
    *,
    percentile: float = 99.0,
    n_bins: int = DEFAULT_N_BINS,
    n_boot: int = 2000,
    rng: np.random.Generator,
) -> dict[str, float | int | bool]:
    """Compare observed ECE with the finite-sample null floor for one market."""
    probs = np.asarray(probs, dtype=float)
    labels = np.asarray(labels, dtype=float)
    if probs.shape != labels.shape:
        raise ValueError("probs and labels must have the same shape")

    dist = null_ece_distribution(probs, n_bins=n_bins, n_boot=n_boot, rng=rng)
    ece = compute_ece(probs, labels, n_bins=n_bins)
    selected_floor = float(np.percentile(dist, percentile))
    floor_p95 = float(np.percentile(dist, 95.0))
    floor_p99 = float(np.percentile(dist, 99.0))
    return {
        "ece": round(float(ece), 6),
        "null_floor": round(selected_floor, 6),
        "null_floor_p95": round(floor_p95, 6),
        "null_floor_p99": round(floor_p99, 6),
        "passed": bool(ece <= selected_floor),
        "sample_count": int(probs.size),
    }


def dump_calibration_oof(
    oof_raw: dict[str, list[np.ndarray]],
    oof_labels: dict[str, list[np.ndarray]],
    path: str | Path,
    *,
    timing: dict[str, Any] | None = None,
) -> None:
    """Persist per-fold OOF probabilities and labels for offline calibration analysis."""
    markets: dict[str, dict[str, list[list[float]]]] = {}
    for market_key, fold_arrays in oof_raw.items():
        label_arrays = oof_labels.get(market_key, [])
        markets[market_key] = {
            "fold_probs": [np.asarray(array, dtype=float).tolist() for array in fold_arrays],
            "fold_labels": [np.asarray(array, dtype=float).tolist() for array in label_arrays],
        }

    payload = {"schema_version": 1, "markets": markets, "timing": timing or {}}
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_calibration_oof(path: str | Path) -> dict[str, Any]:
    """Load a captured calibration OOF dump."""
    data: dict[str, Any] = json.loads(Path(path).read_text(encoding="utf-8"))
    return data


def summarize_fold_subsets(
    fold_probs: list[np.ndarray],
    fold_labels: list[np.ndarray],
    *,
    fold_counts: list[int],
    percentile: float = 99.0,
    n_bins: int = DEFAULT_N_BINS,
    n_boot: int = 2000,
    rng: np.random.Generator,
) -> list[dict[str, float | int | bool]]:
    """Gate each candidate fold subset for the floor(market, fold_count) table."""
    rows: list[dict[str, float | int | bool]] = []
    total_folds = min(len(fold_probs), len(fold_labels))
    for requested_count in fold_counts:
        fold_count = min(max(int(requested_count), 0), total_folds)
        if fold_count == 0:
            probs = np.array([], dtype=float)
            labels = np.array([], dtype=float)
        else:
            probs = np.concatenate([np.asarray(array, dtype=float) for array in select_folds(fold_probs, fold_count)])
            labels = np.concatenate([np.asarray(array, dtype=float) for array in select_folds(fold_labels, fold_count)])
        gate = market_ece_gate(
            probs,
            labels,
            percentile=percentile,
            n_bins=n_bins,
            n_boot=n_boot,
            rng=rng,
        )
        rows.append({"fold_count": int(fold_count), **gate})
    return rows
