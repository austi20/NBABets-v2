from __future__ import annotations

import numpy as np
import pytest

from app.training.distributions import empirical_pit_cdf


def test_empirical_pit_midrank_uniform_like() -> None:
    rng = np.random.default_rng(0)
    samples = rng.uniform(10.0, 20.0, size=10_000)
    pit = empirical_pit_cdf(samples, 15.0)
    assert 0.47 < pit < 0.53


def test_empirical_pit_discrete_mass() -> None:
    samples = np.array([1.0, 2.0, 2.0, 3.0])
    lt = float(np.sum(samples < 2.0))
    eq = float(np.sum(samples == 2.0))
    expected = float(np.clip((lt + 0.5 * eq) / len(samples), 1e-6, 1.0 - 1e-6))
    assert empirical_pit_cdf(samples, 2.0) == pytest.approx(expected)


def test_clipped_low_tail() -> None:
    rng = np.random.default_rng(1)
    s = rng.normal(0.0, 1.0, size=5000)
    pit = empirical_pit_cdf(s, -10.0)
    assert pit >= 1e-6


def test_empty_samples_fallback() -> None:
    assert empirical_pit_cdf(np.array([], dtype=float), 5.0) == pytest.approx(0.5)
