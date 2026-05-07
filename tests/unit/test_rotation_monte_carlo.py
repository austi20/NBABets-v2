from __future__ import annotations

import numpy as np
import pytest

from app.training.rotation_monte_carlo import (
    RealizedAbsenceCache,
    aggregate_branch_samples,
    dnp_risk_from_branches,
    enumerate_or_sample_branches,
    summarize_branch_samples,
)


def test_exact_enumeration_and_dnp_risk_matches_probability() -> None:
    result = enumerate_or_sample_branches({101: 0.5, 202: 0.25}, max_exact_players=8)
    assert result.is_exact_enumeration is True
    assert len(result.branches) == 4
    assert abs(sum(branch.probability for branch in result.branches) - 1.0) < 1e-9
    assert abs(dnp_risk_from_branches(result.branches, player_id=101) - 0.5) < 1e-9
    assert abs(dnp_risk_from_branches(result.branches, player_id=202) - 0.75) < 1e-9


def test_sampled_fallback_is_deterministic_with_seed() -> None:
    probs = {idx: 0.6 for idx in range(1, 11)}  # k=10 > 8 triggers sampled mode
    first = enumerate_or_sample_branches(probs, max_exact_players=8, n_samples=3000, seed=7)
    second = enumerate_or_sample_branches(probs, max_exact_players=8, n_samples=3000, seed=7)
    assert first.is_exact_enumeration is False
    assert second.is_exact_enumeration is False
    first_map = {branch.absent_player_ids: branch.probability for branch in first.branches}
    second_map = {branch.absent_player_ids: branch.probability for branch in second.branches}
    assert first_map == second_map


def test_sampled_bernoulli_converges_to_half_probability() -> None:
    result = enumerate_or_sample_branches({77: 0.5}, max_exact_players=0, n_samples=20000, seed=99)
    estimated_dnp = dnp_risk_from_branches(result.branches, player_id=77)
    assert abs(estimated_dnp - 0.5) < 0.03


def test_exact_enumeration_counts_only_true_uncertain_players() -> None:
    probs = {idx: 1.0 for idx in range(1, 10)}
    probs[10] = 0.5
    probs[11] = 0.0

    result = enumerate_or_sample_branches(probs, max_exact_players=8, n_samples=128, seed=1)

    assert result.is_exact_enumeration is True
    assert result.uncertain_player_count == 1
    assert len(result.branches) == 2
    assert abs(dnp_risk_from_branches(result.branches, player_id=10) - 0.5) < 1e-9
    assert dnp_risk_from_branches(result.branches, player_id=11) == 1.0
    for branch in result.branches:
        assert branch.active_by_player[1] == 1
        assert branch.active_by_player[11] == 0


def test_availability_branch_active_map_is_immutable() -> None:
    result = enumerate_or_sample_branches({101: 0.5})
    with pytest.raises(TypeError):
        result.branches[0].active_by_player[101] = 1


def test_cache_and_branch_aggregation() -> None:
    cache = RealizedAbsenceCache()
    cache.set(game_id=1, team_id=2, absent_player_ids=[9, 3], value="cached")
    assert cache.get(game_id=1, team_id=2, absent_player_ids=[3, 9]) == "cached"

    samples = aggregate_branch_samples(
        [
            (0.7, np.array([20.0, 21.0, 22.0])),
            (0.3, np.array([5.0, 6.0, 7.0])),
        ],
        total_draws=5000,
        seed=11,
    )
    assert len(samples) == 5000
    assert float(np.mean(samples)) > 14.0


def test_branch_aggregation_rejects_positive_weight_empty_samples() -> None:
    with pytest.raises(ValueError, match="positive-weight branch"):
        aggregate_branch_samples(
            [
                (0.9, np.array([])),
                (0.1, np.array([1.0])),
            ],
            total_draws=1000,
            seed=3,
        )


def test_branch_aggregation_ignores_zero_weight_empty_samples() -> None:
    samples = aggregate_branch_samples(
        [
            (0.0, np.array([])),
            (1.0, np.array([1.0, 2.0])),
        ],
        total_draws=1000,
        seed=3,
    )

    assert len(samples) == 1000
    assert set(np.unique(samples)).issubset({1.0, 2.0})


def test_summarize_branch_samples_includes_tail_metrics() -> None:
    summary = summarize_branch_samples(np.array([0.0, 10.0, 20.0, 30.0]), line=20.0)

    assert summary.projected_mean == 15.0
    assert summary.projected_median == 15.0
    assert summary.percentile_25 == 7.5
    assert summary.percentile_75 == 22.5
    assert summary.boom_probability == 0.25
    assert summary.bust_probability == 0.5
    assert summary.simulation_samples == 4
