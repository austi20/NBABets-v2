from __future__ import annotations

from app.trading.allocation import AllocationPick, allocate_proportional_with_soft_cap


def _pick(candidate_id: str, model_prob: float) -> AllocationPick:
    return AllocationPick(candidate_id=candidate_id, model_prob=model_prob)


def test_empty_input_returns_empty_dict() -> None:
    assert allocate_proportional_with_soft_cap([], budget=10.0) == {}


def test_single_pick_capped_at_soft_cap() -> None:
    result = allocate_proportional_with_soft_cap([_pick("a", 0.6)], budget=10.0)
    assert result == {"a": 3.5}  # 35% of 10


def test_two_equal_picks_below_cap_split_evenly() -> None:
    picks = [_pick("a", 0.5), _pick("b", 0.5)]
    result = allocate_proportional_with_soft_cap(picks, budget=4.0)
    # raw alloc 2.0 each, cap 1.4 — both capped
    assert result == {"a": 1.4, "b": 1.4}


def test_three_picks_one_overcap_redistributes_overflow() -> None:
    # weights 0.6/0.2/0.2, budget 10, cap 3.5
    # raw: 6.0 / 2.0 / 2.0 → cap "a" at 3.5, overflow 2.5 to b+c proportionally
    # b and c get extra 1.25 each → 3.25 each, both still under 3.5 cap
    picks = [_pick("a", 0.6), _pick("b", 0.2), _pick("c", 0.2)]
    result = allocate_proportional_with_soft_cap(picks, budget=10.0)
    assert result["a"] == 3.5
    assert abs(result["b"] - 3.25) < 1e-6
    assert abs(result["c"] - 3.25) < 1e-6
    assert sum(result.values()) <= 10.0 + 1e-6


def test_all_picks_above_cap_total_lt_budget() -> None:
    picks = [_pick("a", 0.5), _pick("b", 0.5)]
    result = allocate_proportional_with_soft_cap(picks, budget=2.0)
    # cap 0.7, raw 1.0/1.0 — both cap, total 1.4 < 2.0
    assert result == {"a": 0.7, "b": 0.7}


def test_zero_total_weight_returns_zero_stakes() -> None:
    picks = [_pick("a", 0.0), _pick("b", 0.0)]
    result = allocate_proportional_with_soft_cap(picks, budget=10.0)
    assert result == {"a": 0.0, "b": 0.0}


def test_custom_cap_fraction() -> None:
    result = allocate_proportional_with_soft_cap(
        [_pick("a", 1.0)], budget=10.0, cap_fraction=0.5
    )
    assert result == {"a": 5.0}


def test_converges_within_max_iterations() -> None:
    picks = [_pick(f"p{i}", 0.1 * (i + 1)) for i in range(5)]
    result = allocate_proportional_with_soft_cap(picks, budget=10.0, max_iterations=3)
    assert sum(result.values()) <= 10.0 + 1e-6
    assert all(stake <= 10.0 * 0.35 + 1e-6 for stake in result.values())
