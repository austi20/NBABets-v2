from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import product
from types import MappingProxyType

import numpy as np

EPSILON = 1e-12


@dataclass(frozen=True)
class AvailabilityBranch:
    active_by_player: Mapping[int, int]
    absent_player_ids: frozenset[int]
    probability: float


@dataclass(frozen=True)
class AvailabilityBranchResult:
    branches: tuple[AvailabilityBranch, ...]
    is_exact_enumeration: bool
    uncertain_player_count: int


@dataclass(frozen=True)
class BranchSampleSummary:
    projected_mean: float
    projected_median: float
    percentile_10: float
    percentile_25: float
    percentile_75: float
    percentile_90: float
    boom_probability: float
    bust_probability: float
    simulation_samples: int


def enumerate_or_sample_branches(
    play_probabilities: Mapping[int, float],
    *,
    max_exact_players: int = 8,
    n_samples: int = 2048,
    seed: int = 42,
) -> AvailabilityBranchResult:
    normalized = {
        int(player_id): float(np.clip(probability, 0.0, 1.0))
        for player_id, probability in play_probabilities.items()
    }
    player_ids = sorted(normalized)
    uncertain_player_ids = [
        player_id
        for player_id in player_ids
        if EPSILON < normalized[player_id] < 1.0 - EPSILON
    ]
    deterministic_active_by_player = {
        player_id: int(normalized[player_id] >= 1.0 - EPSILON)
        for player_id in player_ids
        if player_id not in uncertain_player_ids
    }
    if len(uncertain_player_ids) <= max_exact_players:
        return AvailabilityBranchResult(
            branches=_enumerate_exact_branches(uncertain_player_ids, normalized, deterministic_active_by_player),
            is_exact_enumeration=True,
            uncertain_player_count=len(uncertain_player_ids),
        )
    return AvailabilityBranchResult(
        branches=_sampled_branches(
            uncertain_player_ids,
            normalized,
            deterministic_active_by_player,
            n_samples=n_samples,
            seed=seed,
        ),
        is_exact_enumeration=False,
        uncertain_player_count=len(uncertain_player_ids),
    )


def dnp_risk_from_branches(branches: Sequence[AvailabilityBranch], *, player_id: int) -> float:
    absent_probability = 0.0
    for branch in branches:
        if player_id in branch.absent_player_ids:
            absent_probability += branch.probability
    return float(np.clip(absent_probability, 0.0, 1.0))


def realized_absence_cache_key(*, game_id: int, team_id: int, absent_player_ids: Sequence[int]) -> tuple[int, int, frozenset[int]]:
    return game_id, team_id, frozenset(int(player_id) for player_id in absent_player_ids)


class RealizedAbsenceCache:
    def __init__(self) -> None:
        self._store: dict[tuple[int, int, frozenset[int]], object] = {}

    def get(self, *, game_id: int, team_id: int, absent_player_ids: Sequence[int]) -> object | None:
        return self._store.get(realized_absence_cache_key(game_id=game_id, team_id=team_id, absent_player_ids=absent_player_ids))

    def set(self, *, game_id: int, team_id: int, absent_player_ids: Sequence[int], value: object) -> None:
        self._store[realized_absence_cache_key(game_id=game_id, team_id=team_id, absent_player_ids=absent_player_ids)] = value


def aggregate_branch_samples(
    branch_samples: Sequence[tuple[float, np.ndarray]],
    *,
    total_draws: int = 10000,
    seed: int = 42,
) -> np.ndarray:
    if not branch_samples:
        return np.zeros(0, dtype=float)
    rng = np.random.default_rng(seed)
    valid_branch_samples: list[tuple[float, np.ndarray]] = []
    for weight, samples in branch_samples:
        bounded_weight = max(float(weight), 0.0)
        source = np.asarray(samples, dtype=float)
        if source.size == 0:
            if bounded_weight > EPSILON:
                raise ValueError("Cannot aggregate a positive-weight branch with no stat samples.")
            continue
        valid_branch_samples.append((bounded_weight, source))
    if not valid_branch_samples:
        return np.zeros(0, dtype=float)

    weights = np.asarray([weight for weight, _ in valid_branch_samples], dtype=float)
    if float(weights.sum()) <= EPSILON:
        weights = np.full(len(valid_branch_samples), 1.0 / len(valid_branch_samples), dtype=float)
    else:
        weights = weights / float(weights.sum())

    draw_counts = _multinomial_counts(weights, total_draws, rng)
    collected: list[np.ndarray] = []
    for count, (_, source) in zip(draw_counts, valid_branch_samples, strict=False):
        if count <= 0:
            continue
        picked = rng.choice(source, size=int(count), replace=True)
        collected.append(np.asarray(picked, dtype=float))
    if not collected:
        return np.zeros(0, dtype=float)
    return np.concatenate(collected)


def summarize_branch_samples(
    samples: Sequence[float] | np.ndarray,
    *,
    line: float,
    boom_multiplier: float = 1.10,
    bust_multiplier: float = 0.70,
) -> BranchSampleSummary:
    values = np.asarray(samples, dtype=float)
    if values.size == 0:
        return BranchSampleSummary(
            projected_mean=0.0,
            projected_median=0.0,
            percentile_10=0.0,
            percentile_25=0.0,
            percentile_75=0.0,
            percentile_90=0.0,
            boom_probability=0.0,
            bust_probability=0.0,
            simulation_samples=0,
        )
    boom_threshold = float(line) * float(boom_multiplier)
    bust_threshold = float(line) * float(bust_multiplier)
    return BranchSampleSummary(
        projected_mean=float(np.mean(values)),
        projected_median=float(np.quantile(values, 0.50)),
        percentile_10=float(np.quantile(values, 0.10)),
        percentile_25=float(np.quantile(values, 0.25)),
        percentile_75=float(np.quantile(values, 0.75)),
        percentile_90=float(np.quantile(values, 0.90)),
        boom_probability=float(np.mean(values >= boom_threshold)),
        bust_probability=float(np.mean(values <= bust_threshold)),
        simulation_samples=int(values.size),
    )


def _enumerate_exact_branches(
    player_ids: list[int],
    play_probabilities: dict[int, float],
    deterministic_active_by_player: Mapping[int, int],
) -> tuple[AvailabilityBranch, ...]:
    branches: list[AvailabilityBranch] = []
    for active_bits in product((0, 1), repeat=len(player_ids)):
        probability = 1.0
        active_by_player: dict[int, int] = dict(deterministic_active_by_player)
        absent: set[int] = {
            player_id
            for player_id, is_active in deterministic_active_by_player.items()
            if is_active == 0
        }
        for player_id, is_active in zip(player_ids, active_bits, strict=False):
            p_active = play_probabilities[player_id]
            probability *= p_active if is_active == 1 else (1.0 - p_active)
            active_by_player[player_id] = is_active
            if is_active == 0:
                absent.add(player_id)
        if probability <= EPSILON:
            continue
        branches.append(
            AvailabilityBranch(
                active_by_player=_freeze_active_map(active_by_player),
                absent_player_ids=frozenset(absent),
                probability=float(probability),
            )
        )
    total = sum(branch.probability for branch in branches)
    if total <= EPSILON:
        return ()
    return tuple(
        AvailabilityBranch(
            active_by_player=branch.active_by_player,
            absent_player_ids=branch.absent_player_ids,
            probability=branch.probability / total,
        )
        for branch in branches
    )


def _sampled_branches(
    player_ids: list[int],
    play_probabilities: dict[int, float],
    deterministic_active_by_player: Mapping[int, int],
    *,
    n_samples: int,
    seed: int,
) -> tuple[AvailabilityBranch, ...]:
    rng = np.random.default_rng(seed)
    deterministic_absent = {
        player_id
        for player_id, is_active in deterministic_active_by_player.items()
        if is_active == 0
    }
    counts_by_absent_set: dict[frozenset[int], int] = {}
    for _ in range(max(int(n_samples), 1)):
        absent = frozenset(
            deterministic_absent
            | {
                player_id
                for player_id in player_ids
                if rng.random() > play_probabilities[player_id]
            }
        )
        counts_by_absent_set[absent] = counts_by_absent_set.get(absent, 0) + 1
    branches: list[AvailabilityBranch] = []
    total = float(sum(counts_by_absent_set.values()))
    for absent_set, count in counts_by_absent_set.items():
        active_by_player = dict(deterministic_active_by_player)
        active_by_player.update({player_id: int(player_id not in absent_set) for player_id in player_ids})
        branches.append(
            AvailabilityBranch(
                active_by_player=_freeze_active_map(active_by_player),
                absent_player_ids=absent_set,
                probability=float(count) / total,
            )
        )
    branches.sort(key=lambda branch: (-branch.probability, sorted(branch.absent_player_ids)))
    return tuple(branches)


def _multinomial_counts(weights: np.ndarray, total_draws: int, rng: np.random.Generator) -> np.ndarray:
    if total_draws <= 0:
        return np.zeros(len(weights), dtype=int)
    return rng.multinomial(total_draws, weights)


def _freeze_active_map(active_by_player: Mapping[int, int]) -> Mapping[int, int]:
    return MappingProxyType(
        {int(player_id): int(is_active) for player_id, is_active in active_by_player.items()}
    )
