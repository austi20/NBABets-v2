from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AllocationPick:
    """Minimal input shape for the allocation algorithm.

    Only carries what allocation needs — keeps the function pure and trivial to mock.
    """

    candidate_id: str
    model_prob: float


def allocate_proportional_with_soft_cap(
    selected_picks: list[AllocationPick],
    budget: float,
    cap_fraction: float = 0.35,
    max_iterations: int = 3,
) -> dict[str, float]:
    """Allocate ``budget`` across ``selected_picks`` proportional to ``model_prob``.

    Any single allocation greater than ``budget * cap_fraction`` is capped at that
    ceiling. Overflow is redistributed proportionally among the uncapped picks
    (also subject to the cap). Iterates up to ``max_iterations`` times to settle.

    Total stake may be less than ``budget`` if all picks hit the cap. This is the
    intended safety property: never overspend, never violate the per-pick ceiling.
    """
    if not selected_picks:
        return {}

    cap = budget * cap_fraction
    stakes: dict[str, float] = {pick.candidate_id: 0.0 for pick in selected_picks}
    remaining = list(selected_picks)
    remaining_budget = budget

    for _ in range(max_iterations):
        if not remaining:
            break
        total_weight = sum(pick.model_prob for pick in remaining)
        if total_weight <= 0:
            break

        # Compute all raw allocations from the snapshot budget, then apply.
        snapshot_budget = remaining_budget
        newly_capped: list[AllocationPick] = []
        for pick in remaining:
            raw = snapshot_budget * pick.model_prob / total_weight
            allowed = cap - stakes[pick.candidate_id]
            if raw >= allowed:
                stakes[pick.candidate_id] = cap
                remaining_budget -= allowed
                newly_capped.append(pick)
            else:
                stakes[pick.candidate_id] += raw
                remaining_budget -= raw

        if not newly_capped:
            break
        remaining = [pick for pick in remaining if pick not in newly_capped]

    return stakes
