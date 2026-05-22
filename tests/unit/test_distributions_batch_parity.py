"""Parity tests for the vectorized batch Monte Carlo samplers.

The batch samplers must produce per-row over-probabilities that match the
scalar reference (`summarize_line_probability`) within a loose tolerance. The
tolerance is wider than the MC noise floor because the batched negative-binomial
RNG stream diverges bitwise from scipy's scalar `nbinom.rvs` while remaining
statistically equivalent.
"""

from __future__ import annotations

import numpy as np

from app.config.settings import get_settings
from app.training.distributions import (
    simulate_line_probabilities_batch,
    summarize_line_probability,
)

PARITY_TOL = 0.01
SAMPLE_SIZE = get_settings().simulation_min_samples
# The batched NB RNG stream diverges from scipy's scalar stream, so per-row
# parity is statistical, not bitwise. The 0.01 tolerance is below the max-row
# MC noise floor at the default sample count, so the cross-stream parity tests
# draw enough samples for the noise floor to sit under tolerance (error ~ 1/sqrt(S)).
_MIN_PARITY_SAMPLES = 40_000  # noise ~1/sqrt(S) ~= 0.005 < tolerance at this count
PARITY_SAMPLE_SIZE = max(SAMPLE_SIZE, _MIN_PARITY_SAMPLES)


def representative_rows(market_key: str, n: int) -> dict[str, np.ndarray | list]:
    """Generate ``n`` realistic synthetic rows for ``market_key``.

    Returns means/variances/lines/minutes_means/minutes_stds arrays plus a
    ``contexts`` list (empty dicts -> forces the generic count path). Reused by
    later tasks, so kept market-agnostic.
    """
    rng = np.random.default_rng(123)
    means = rng.uniform(0.5, 25.0, size=n)
    variances = means + rng.uniform(0.5, 1.0, size=n) * means  # mean+0.5*mean .. mean+mean
    lines = means + rng.uniform(-3.0, 3.0, size=n)
    minutes_means = rng.uniform(18.0, 36.0, size=n)
    minutes_stds = rng.uniform(3.0, 7.0, size=n)
    contexts: list[dict[str, float] | None] = [{} for _ in range(n)]
    return {
        "means": means,
        "variances": variances,
        "lines": lines,
        "minutes_means": minutes_means,
        "minutes_stds": minutes_stds,
        "contexts": contexts,
        "market_key": market_key,
    }


def _scalar_reference(
    means: np.ndarray,
    variances: np.ndarray,
    lines: np.ndarray,
    minutes_means: np.ndarray,
    minutes_stds: np.ndarray,
    contexts: list[dict[str, float] | None],
    market_key: str,
    sample_size: int,
) -> np.ndarray:
    out = np.empty(len(means), dtype=float)
    for i in range(len(means)):
        mm = minutes_means[i]
        ms = minutes_stds[i]
        summary = summarize_line_probability(
            float(means[i]),
            float(variances[i]),
            float(lines[i]),
            sample_size,
            minutes_mean=None if mm is None or not np.isfinite(mm) else float(mm),
            minutes_std=None if ms is None or not np.isfinite(ms) else float(ms),
            market_key=market_key,
            context=contexts[i],
        )
        out[i] = summary.over_probability
    return out


def test_generic_count_parity() -> None:
    rng = np.random.default_rng(7)
    n = 50
    means = rng.uniform(0.5, 25.0, size=n)
    variances = means + rng.uniform(0.5, 1.0, size=n) * np.clip(means * 2.0, 0.5, None)
    lines = means + rng.uniform(-2.0, 2.0, size=n)
    minutes_means = rng.uniform(20.0, 34.0, size=n)
    minutes_stds = rng.uniform(3.0, 6.0, size=n)
    contexts: list[dict[str, float] | None] = [{} for _ in range(n)]
    market_key = "rebounds"  # named market, empty context -> generic fallback

    p_new = simulate_line_probabilities_batch(
        means=means,
        variances=variances,
        lines=lines,
        minutes_means=minutes_means,
        minutes_stds=minutes_stds,
        market_key=market_key,
        contexts=contexts,
        sample_size=PARITY_SAMPLE_SIZE,
    )
    p_old = _scalar_reference(
        means, variances, lines, minutes_means, minutes_stds, contexts, market_key, PARITY_SAMPLE_SIZE
    )

    assert p_new.shape == (n,)
    assert np.max(np.abs(p_old - p_new)) <= PARITY_TOL


def test_minutes_edge_cases() -> None:
    means = np.array([8.0, 12.0, 5.0, 20.0], dtype=float)
    variances = np.array([16.0, 24.0, 9.0, 40.0], dtype=float)
    lines = np.array([7.5, 11.5, 4.5, 19.5], dtype=float)
    # row 0: NaN minutes_mean -> 30.0 default; row 1: <=0 minutes_mean -> zeros;
    # row 2: NaN minutes_std; row 3: normal.
    minutes_means = np.array([np.nan, -1.0, 24.0, 30.0], dtype=float)
    minutes_stds = np.array([4.0, 4.0, np.nan, 5.0], dtype=float)
    contexts: list[dict[str, float] | None] = [{}, {}, {}, {}]
    market_key = "assists"

    p_new = simulate_line_probabilities_batch(
        means=means,
        variances=variances,
        lines=lines,
        minutes_means=minutes_means,
        minutes_stds=minutes_stds,
        market_key=market_key,
        contexts=contexts,
        sample_size=PARITY_SAMPLE_SIZE,
    )
    p_old = _scalar_reference(
        means, variances, lines, minutes_means, minutes_stds, contexts, market_key, PARITY_SAMPLE_SIZE
    )

    assert np.max(np.abs(p_old - p_new)) <= PARITY_TOL
    # Row 1 has minutes_mean <= 0 -> minutes clamp to zero -> ~0 production ->
    # over-prob for a positive line collapses to ~0. Self-documents the edge case.
    assert p_new[1] < 0.01


def test_empty_input() -> None:
    result = simulate_line_probabilities_batch(
        means=np.zeros(0),
        variances=np.zeros(0),
        lines=np.zeros(0),
        minutes_means=np.zeros(0),
        minutes_stds=np.zeros(0),
        market_key="points",
        contexts=[],
        sample_size=SAMPLE_SIZE,
    )
    assert result.shape == (0,)
    np.testing.assert_array_equal(result, np.zeros(0, dtype=float))


def test_posterior_form() -> None:
    means = np.array([10.0], dtype=float)
    variances = np.array([10.0], dtype=float)  # poisson regime (var ~= mean)
    lines = np.array([9.5], dtype=float)
    minutes_means = np.array([30.0], dtype=float)
    minutes_stds = np.array([5.0], dtype=float)
    s = SAMPLE_SIZE

    p = simulate_line_probabilities_batch(
        means=means,
        variances=variances,
        lines=lines,
        minutes_means=minutes_means,
        minutes_stds=minutes_stds,
        market_key="points",
        contexts=[{}],
        sample_size=s,
    )[0]

    # Returned probability must lie on the (k + 0.5) / (S + 1) lattice.
    k = p * (s + 1.0) - 0.5
    assert abs(k - round(k)) < 1e-6
    assert 0.0 < p < 1.0
