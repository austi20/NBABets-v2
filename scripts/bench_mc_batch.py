"""Wall-time benchmark: batched MC vs per-row ``summarize_line_probability``.

Compares the legacy per-row Monte Carlo loop against the vectorized
``simulate_line_probabilities_batch`` across every market, printing speedup and
max-abs-diff per market plus a PASS/FAIL against the spec thresholds
(speedup >= 3.0, max_abs_diff <= 0.02).

Per-market context key sets mirror the parity-test builders in
``tests/unit/test_distributions_batch_parity.py`` so the benchmark exercises the
real market sampler path (not the silent generic fallback).

Run: ``python scripts/bench_mc_batch.py``
"""

from __future__ import annotations

import time

import numpy as np

from app.training.distributions import (
    simulate_line_probabilities_batch,
    summarize_line_probability,
)

N = 1000
S = 10000
SPEEDUP_MIN = 3.0
MAX_ABS_DIFF_MAX = 0.02
MARKETS = ["points", "rebounds", "assists", "threes", "turnovers"]


def _realistic_context(market_key: str, rng: np.random.Generator) -> dict[str, float]:
    """Context dict populated with the keys each market's sampler reads.

    Key sets and value ranges replicate the parity-test builders in
    ``tests/unit/test_distributions_batch_parity.py`` so the benchmark drives the
    real per-market path instead of falling back to the generic count sampler.
    """
    if market_key == "rebounds":
        return {
            "rebound_chances_total_per_minute": float(rng.uniform(0.3, 1.2)),
            "rebound_conversion_rate": float(rng.uniform(0.1, 0.5)),
            "rebounds_std_10": float(rng.uniform(0.5, 4.0)),
            "shared_latent": float(rng.uniform(0.7, 1.3)),
        }
    if market_key == "assists":
        return {
            "assist_creation_proxy_per_minute": float(rng.uniform(0.05, 0.4)),
            "assist_conversion_rate": float(rng.uniform(0.1, 0.6)),
            "passes_per_minute": float(rng.uniform(2.0, 8.0)),
            "shared_latent": float(rng.uniform(0.7, 1.3)),
        }
    if market_key == "turnovers":
        return {
            "touches_per_minute": float(rng.uniform(0.5, 3.0)),
            "turnover_ratio": float(rng.uniform(0.03, 0.25)),
            "usage_rate": float(rng.uniform(0.10, 0.35)),
            "shared_latent": float(rng.uniform(0.7, 1.3)),
        }
    if market_key == "threes":
        return {
            "estimated_three_point_attempts_per_minute": float(rng.uniform(0.05, 0.30)),
            "three_point_make_rate": float(rng.uniform(0.30, 0.45)),
            "percentage_field_goals_attempted_3pt": float(rng.uniform(0.20, 0.70)),
            "shared_latent": float(rng.uniform(0.7, 1.3)),
        }
    if market_key == "points":
        return {
            "field_goal_attempts_per_minute": float(rng.uniform(0.30, 0.70)),
            "estimated_three_point_attempts_per_minute": float(rng.uniform(0.05, 0.30)),
            "free_throw_attempts_per_minute": float(rng.uniform(0.05, 0.30)),
            "usage_rate": float(rng.uniform(0.15, 0.40)),
            "true_shooting_percentage": float(rng.uniform(0.50, 0.65)),
            "three_point_make_rate": float(rng.uniform(0.30, 0.45)),
            "free_throw_make_rate": float(rng.uniform(0.65, 0.90)),
            "points_pace_exposure": float(rng.uniform(95.0, 115.0)),
            "points_3pt_variance": float(rng.uniform(0.0, 6.0)),
            "shared_latent": float(rng.uniform(0.7, 1.3)),
        }
    raise ValueError(f"no context builder for market {market_key!r}")


def _bench_market(market_key: str, rng: np.random.Generator) -> dict[str, float]:
    means = rng.uniform(3, 25, size=N)
    variances = means + rng.uniform(0.5, 5, size=N)
    lines = means + rng.normal(0, 1.5, size=N)
    minutes_means = rng.uniform(15, 36, size=N)
    minutes_stds = rng.uniform(2, 6, size=N)
    contexts = [_realistic_context(market_key, rng) for _ in range(N)]

    t0 = time.perf_counter()
    old = np.array(
        [
            summarize_line_probability(
                means[i],
                variances[i],
                lines[i],
                simulations=S,
                minutes_mean=minutes_means[i],
                minutes_std=minutes_stds[i],
                market_key=market_key,
                context=contexts[i],
            ).over_probability
            for i in range(N)
        ]
    )
    t_old = time.perf_counter() - t0

    t0 = time.perf_counter()
    new = simulate_line_probabilities_batch(
        means=means,
        variances=variances,
        lines=lines,
        minutes_means=minutes_means,
        minutes_stds=minutes_stds,
        market_key=market_key,
        contexts=contexts,
        sample_size=S,
    )
    t_new = time.perf_counter() - t0

    return {
        "t_old": t_old,
        "t_new": t_new,
        "speedup": t_old / t_new if t_new > 0 else float("inf"),
        "max_abs_diff": float(np.max(np.abs(old - new))),
    }


def main() -> int:
    rng = np.random.default_rng(7)
    all_pass = True
    print(f"Monte Carlo batch benchmark (N={N}, S={S})")
    print(f"Thresholds: speedup >= {SPEEDUP_MIN:.1f}x, max_abs_diff <= {MAX_ABS_DIFF_MAX}")
    print("-" * 78)
    for market_key in MARKETS:
        r = _bench_market(market_key, rng)
        market_pass = r["speedup"] >= SPEEDUP_MIN and r["max_abs_diff"] <= MAX_ABS_DIFF_MAX
        all_pass = all_pass and market_pass
        status = "PASS" if market_pass else "FAIL"
        print(
            f"{market_key:>10}: old={r['t_old']:.2f}s new={r['t_new']:.2f}s "
            f"speedup={r['speedup']:.1f}x max_abs_diff={r['max_abs_diff']:.4f}  [{status}]"
        )
    print("-" * 78)
    print(f"OVERALL: {'PASS' if all_pass else 'FAIL'}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
