from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

ARCHETYPE_LABELS = (
    "primary_creator",
    "scoring_wing",
    "rim_big",
    "spacing_guard",
    "bench_depth",
)
LEAGUE_TEAM_ID = "LEAGUE"
PLAYER_MINUTES_CAP = 48.0
USAGE_SHARE_CAP = 0.45
EPSILON = 1e-6


@dataclass(frozen=True)
class RoleVector:
    player_id: int
    season: int
    position_group: str
    usage_proxy: float
    usage_share: float
    assist_share: float
    rebound_share: float
    three_point_rate: float
    rim_attempt_rate: float
    touches_per_minute: float
    passes_per_minute: float
    rebound_chances_per_minute: float
    blocks_per_minute: float
    starter_score: float
    role_stability: float
    archetype_label: str


@dataclass(frozen=True)
class RotationWeight:
    team_id: int | str
    season: int | None
    absent_archetype: str
    candidate_archetype: str
    minute_gain_weight: float
    usage_gain_weight: float
    minute_delta_mean: float = 0.0
    usage_delta_mean: float = 0.0
    minute_delta_variance: float | None = None
    usage_delta_variance: float | None = None
    sample_size: int = 0
    weight_source: Literal["team", "league", "fallback"] = "team"
    last_updated: datetime | None = None


@dataclass(frozen=True)
class PlayerRotationProfile:
    game_id: int
    team_id: int
    player_id: int
    player_name: str
    status: str
    position_group: str
    baseline_minutes: float
    baseline_usage_share: float
    baseline_assist_share: float = 0.0
    baseline_rebound_share: float = 0.0
    baseline_three_point_share: float = 0.0
    role_vector: RoleVector | None = None
    availability_source: Literal["official_inactive", "injury_report", "post_hoc"] = "injury_report"
    report_timestamp: datetime | None = None
    rotation_shock_confidence: float = 1.0


@dataclass(frozen=True)
class AbsenceRecord:
    game_id: int
    team_id: int
    player_id: int
    player_name: str
    status: str
    play_probability: float
    archetype_label: str
    baseline_minutes: float
    baseline_usage_share: float
    removed_minutes: float
    removed_usage_share: float
    removed_assist_share: float
    removed_rebound_share: float
    removed_three_point_share: float
    source: Literal["official_inactive", "injury_report", "post_hoc"] = "injury_report"
    report_timestamp: datetime | None = None
    rotation_shock_confidence: float = 1.0


@dataclass(frozen=True)
class TeammateAdjustment:
    game_id: int
    team_id: int
    player_id: int
    player_name: str
    baseline_minutes: float
    adjusted_minutes: float
    minutes_delta: float
    baseline_usage_share: float
    adjusted_usage_share: float
    usage_delta: float
    baseline_assist_share: float
    adjusted_assist_share: float
    baseline_rebound_share: float
    adjusted_rebound_share: float
    source_absence_player_ids: tuple[int, ...]
    absence_reason: str
    weight_source: Literal["team", "league", "fallback"]


@dataclass(frozen=True)
class AdjustedPlayer:
    player_id: int
    player_name: str
    play_probability: float
    baseline_minutes: float
    adjusted_minutes: float
    baseline_usage_share: float
    adjusted_usage_share: float


@dataclass(frozen=True)
class RedistributionResult:
    adjusted_players: tuple[AdjustedPlayer, ...]
    absences: tuple[AbsenceRecord, ...]
    teammate_adjustments: tuple[TeammateAdjustment, ...]
    team_efficiency_delta: float
    pace_delta: float
    rotation_shock_magnitude: float
    rotation_shock_confidence: float
    mass_conservation_warnings: tuple[str, ...]


class RotationWeightTable:
    def __init__(self, rows: Sequence[RotationWeight] | None = None) -> None:
        self._rows = list(rows or [])

    def lookup(
        self,
        absent_archetype: str,
        candidate_archetype: str,
        *,
        team_id: int,
        season: int | None = None,
    ) -> tuple[float, float, Literal["team", "league", "fallback"]]:
        matches = [
            row
            for row in self._rows
            if row.absent_archetype == absent_archetype and row.candidate_archetype == candidate_archetype
        ]
        ranked = sorted(
            ((rank, row) for row in matches if (rank := _weight_row_rank(row, team_id=team_id, season=season)) is not None),
            key=lambda item: item[0],
        )
        if ranked:
            row = ranked[0][1]
            return max(row.minute_gain_weight, 0.0), max(row.usage_gain_weight, 0.0), row.weight_source
        return 1.0, 1.0, "fallback"


def _weight_row_rank(row: RotationWeight, *, team_id: int, season: int | None) -> int | None:
    if _same_team_id(row.team_id, team_id) and row.weight_source == "team":
        if season is not None and row.season == season:
            return 0
        if row.season is None:
            return 1
        if season is None:
            return 2
        return None
    if str(row.team_id).upper() == LEAGUE_TEAM_ID:
        if season is not None and row.season == season:
            return 3
        if row.season is None:
            return 4
        if season is None:
            return 5
    if row.weight_source == "fallback":
        return 6
    return None


def _same_team_id(row_team_id: int | str, team_id: int) -> bool:
    try:
        return int(row_team_id) == int(team_id)
    except (TypeError, ValueError):
        return False


def _clamped_probability(play_probabilities: Mapping[int, float], player_id: int) -> float:
    return max(0.0, min(1.0, float(play_probabilities.get(player_id, 1.0))))


def _archetype(player: PlayerRotationProfile) -> str:
    return player.role_vector.archetype_label if player.role_vector else "bench_depth"


def _season(player: PlayerRotationProfile) -> int | None:
    return player.role_vector.season if player.role_vector else None


def _warn_once(warnings: list[str], warning: str) -> None:
    if warning not in warnings:
        warnings.append(warning)


def _allocate_with_caps(
    total: float,
    weights: Mapping[int, float],
    caps: Mapping[int, float],
) -> tuple[dict[int, float], float]:
    allocations = {player_id: 0.0 for player_id in weights}
    remaining = max(total, 0.0)
    active = {
        player_id
        for player_id, weight in weights.items()
        if weight > EPSILON and caps.get(player_id, 0.0) > EPSILON
    }

    while remaining > EPSILON and active:
        denominator = sum(weights[player_id] for player_id in active)
        if denominator <= EPSILON:
            break

        used = 0.0
        saturated: set[int] = set()
        for player_id in tuple(active):
            room = max(caps.get(player_id, 0.0) - allocations[player_id], 0.0)
            if room <= EPSILON:
                saturated.add(player_id)
                continue
            proposed = remaining * (weights[player_id] / denominator)
            gain = min(proposed, room)
            allocations[player_id] += gain
            used += gain
            if room - gain <= EPSILON:
                saturated.add(player_id)

        if used <= EPSILON:
            break
        remaining = max(remaining - used, 0.0)
        active.difference_update(saturated)

    return allocations, remaining


def _combine_weight_sources(sources: Sequence[Literal["team", "league", "fallback"]]) -> Literal["team", "league", "fallback"]:
    if not sources:
        return "fallback"
    unique = set(sources)
    if unique == {"team"}:
        return "team"
    if "fallback" in unique:
        return "fallback"
    return "league"


def status_to_play_probability(
    status: str | None,
    *,
    expected_availability_flag: bool | None = None,
    official_inactive: bool = False,
) -> float:
    if official_inactive:
        return 0.0
    normalized = str(status or "").strip().lower()
    mapping = {
        "out": 0.0,
        "out for season": 0.0,
        "inactive": 0.0,
        "suspended": 0.0,
        "doubtful": 0.15,
        "questionable": 0.50,
        "probable": 0.85,
        "available": 1.0,
    }
    probability = mapping.get(normalized, 1.0)
    if expected_availability_flag is False:
        return 0.0
    if expected_availability_flag is True:
        return max(probability, 0.85)
    return probability


def normalize_role_vector(values: Mapping[str, float], *, eps: float = 1e-9) -> dict[str, float]:
    cleaned = {key: max(float(raw), 0.0) for key, raw in values.items()}
    total = sum(cleaned.values())
    if total <= eps:
        if not cleaned:
            return {}
        equal = 1.0 / len(cleaned)
        return {key: equal for key in cleaned}
    return {key: value / total for key, value in cleaned.items()}


def classify_archetype(*, position_group: str, usage_share: float, assist_share: float, rebound_share: float, three_point_rate: float, starter_score: float) -> str:
    pos = (position_group or "UNK").upper()
    if starter_score < 0.30 and usage_share < 0.18:
        return "bench_depth"
    if assist_share >= 0.28 and usage_share >= 0.24:
        return "primary_creator"
    if pos in {"C", "F"} and rebound_share >= 0.22 and three_point_rate <= 0.18:
        return "rim_big"
    if three_point_rate >= 0.38 and pos in {"G", "F"}:
        return "spacing_guard"
    return "scoring_wing"


def redistribute(
    *,
    game_id: int,
    team_id: int,
    players: Sequence[PlayerRotationProfile],
    weights: RotationWeightTable,
    play_probabilities: Mapping[int, float],
    mode: Literal["expected_value", "realized"],
) -> RedistributionResult:
    del mode  # Phase 1 keeps deterministic redistribution logic only.
    warnings: list[str] = []
    scoped_players = [player for player in players if player.game_id == game_id and player.team_id == team_id]

    absent_records: list[AbsenceRecord] = []
    absence_seasons: dict[int, int | None] = {}
    available: list[PlayerRotationProfile] = []
    removed_minutes_total = 0.0
    removed_usage_total = 0.0

    for player in scoped_players:
        probability = _clamped_probability(play_probabilities, player.player_id)
        baseline_minutes = max(player.baseline_minutes, 0.0)
        baseline_usage = max(player.baseline_usage_share, 0.0)
        removed_minutes = baseline_minutes * (1.0 - probability)
        removed_usage = baseline_usage * (1.0 - probability)
        removed_minutes_total += removed_minutes
        removed_usage_total += removed_usage

        if removed_minutes > EPSILON or removed_usage > EPSILON:
            absent_records.append(
                AbsenceRecord(
                    game_id=game_id,
                    team_id=team_id,
                    player_id=player.player_id,
                    player_name=player.player_name,
                    status=player.status,
                    play_probability=probability,
                    archetype_label=_archetype(player),
                    baseline_minutes=baseline_minutes,
                    baseline_usage_share=baseline_usage,
                    removed_minutes=removed_minutes,
                    removed_usage_share=removed_usage,
                    removed_assist_share=player.baseline_assist_share * (1.0 - probability),
                    removed_rebound_share=player.baseline_rebound_share * (1.0 - probability),
                    removed_three_point_share=player.baseline_three_point_share * (1.0 - probability),
                    source=player.availability_source,
                    report_timestamp=player.report_timestamp,
                    rotation_shock_confidence=player.rotation_shock_confidence,
                )
            )
            absence_seasons[player.player_id] = _season(player)
        if probability > EPSILON:
            available.append(player)

    if not scoped_players:
        return RedistributionResult(
            adjusted_players=(),
            absences=(),
            teammate_adjustments=(),
            team_efficiency_delta=0.0,
            pace_delta=0.0,
            rotation_shock_magnitude=0.0,
            rotation_shock_confidence=1.0,
            mass_conservation_warnings=(),
        )

    minute_gains: dict[int, float] = {player.player_id: 0.0 for player in scoped_players}
    usage_gains: dict[int, float] = {player.player_id: 0.0 for player in scoped_players}
    candidate_absence_ids: dict[int, set[int]] = {player.player_id: set() for player in scoped_players}
    candidate_weight_sources: dict[int, list[Literal["team", "league", "fallback"]]] = {
        player.player_id: [] for player in scoped_players
    }

    for absence in absent_records:
        candidates = [player for player in available if player.player_id != absence.player_id]
        minute_weights: dict[int, float] = {}
        usage_weights: dict[int, float] = {}
        sources_by_player: dict[int, Literal["team", "league", "fallback"]] = {}

        for player in candidates:
            probability = _clamped_probability(play_probabilities, player.player_id)
            minute_w, usage_w, source = weights.lookup(
                absence.archetype_label,
                _archetype(player),
                team_id=team_id,
                season=absence_seasons.get(absence.player_id),
            )
            minute_weights[player.player_id] = minute_w * probability
            usage_weights[player.player_id] = usage_w * probability
            sources_by_player[player.player_id] = source

        minute_caps = {
            player.player_id: max(
                max(PLAYER_MINUTES_CAP, max(player.baseline_minutes, 0.0) * _clamped_probability(play_probabilities, player.player_id))
                - max(player.baseline_minutes, 0.0) * _clamped_probability(play_probabilities, player.player_id)
                - minute_gains[player.player_id],
                0.0,
            )
            for player in candidates
        }
        usage_caps = {
            player.player_id: max(
                max(USAGE_SHARE_CAP, max(player.baseline_usage_share, 0.0) * _clamped_probability(play_probabilities, player.player_id))
                - max(player.baseline_usage_share, 0.0) * _clamped_probability(play_probabilities, player.player_id)
                - usage_gains[player.player_id],
                0.0,
            )
            for player in candidates
        }

        minute_allocations, minute_unallocated = _allocate_with_caps(
            absence.removed_minutes,
            minute_weights,
            minute_caps,
        )
        usage_allocations, usage_unallocated = _allocate_with_caps(
            absence.removed_usage_share,
            usage_weights,
            usage_caps,
        )

        if minute_unallocated > EPSILON:
            _warn_once(warnings, "minutes_capacity_exhausted")
        if usage_unallocated > EPSILON:
            _warn_once(warnings, "usage_capacity_exhausted")

        for player_id, gain in minute_allocations.items():
            if gain > EPSILON:
                minute_gains[player_id] += gain
                candidate_absence_ids[player_id].add(absence.player_id)
                candidate_weight_sources[player_id].append(sources_by_player[player_id])
        for player_id, gain in usage_allocations.items():
            if gain > EPSILON:
                usage_gains[player_id] += gain
                candidate_absence_ids[player_id].add(absence.player_id)
                candidate_weight_sources[player_id].append(sources_by_player[player_id])

    adjusted_players: list[AdjustedPlayer] = []
    teammate_adjustments: list[TeammateAdjustment] = []
    gained_minutes_total = sum(minute_gains.values())
    gained_usage_total = sum(usage_gains.values())
    absence_status_by_id = {record.player_id: record.status for record in absent_records}

    for player in scoped_players:
        probability = _clamped_probability(play_probabilities, player.player_id)
        baseline_minutes = max(player.baseline_minutes, 0.0)
        baseline_usage = max(player.baseline_usage_share, 0.0)
        baseline_active_minutes = baseline_minutes * probability
        baseline_active_usage = baseline_usage * probability

        minute_gain = minute_gains[player.player_id]
        usage_gain = usage_gains[player.player_id]

        adjusted_minutes = max(baseline_active_minutes + minute_gain, 0.0)
        adjusted_usage = max(baseline_active_usage + usage_gain, 0.0)
        adjusted_players.append(
            AdjustedPlayer(
                player_id=player.player_id,
                player_name=player.player_name,
                play_probability=probability,
                baseline_minutes=baseline_minutes,
                adjusted_minutes=adjusted_minutes,
                baseline_usage_share=baseline_usage,
                adjusted_usage_share=adjusted_usage,
            )
        )
        if minute_gain > EPSILON or usage_gain > EPSILON:
            absence_ids = tuple(sorted(candidate_absence_ids[player.player_id]))
            absence_reason = ",".join(absence_status_by_id[player_id] for player_id in absence_ids)
            teammate_adjustments.append(
                TeammateAdjustment(
                    game_id=game_id,
                    team_id=team_id,
                    player_id=player.player_id,
                    player_name=player.player_name,
                    baseline_minutes=baseline_active_minutes,
                    adjusted_minutes=adjusted_minutes,
                    minutes_delta=minute_gain,
                    baseline_usage_share=baseline_active_usage,
                    adjusted_usage_share=adjusted_usage,
                    usage_delta=usage_gain,
                    baseline_assist_share=player.baseline_assist_share,
                    adjusted_assist_share=player.baseline_assist_share,
                    baseline_rebound_share=player.baseline_rebound_share,
                    adjusted_rebound_share=player.baseline_rebound_share,
                    source_absence_player_ids=absence_ids,
                    absence_reason=absence_reason,
                    weight_source=_combine_weight_sources(candidate_weight_sources[player.player_id]),
                )
            )

    if abs(removed_minutes_total - gained_minutes_total) > EPSILON:
        _warn_once(warnings, "minutes_mass_not_conserved")
    if abs(removed_usage_total - gained_usage_total) > EPSILON:
        _warn_once(warnings, "usage_mass_not_conserved")

    magnitude = removed_minutes_total + removed_usage_total
    confidence = min((record.rotation_shock_confidence for record in absent_records), default=1.0)
    if absent_records:
        creator_absences = sum(1 for record in absent_records if record.archetype_label == "primary_creator")
        team_eff_delta = -0.01 * creator_absences
        pace_delta = -0.005 * len(absent_records)
    else:
        team_eff_delta = 0.0
        pace_delta = 0.0

    return RedistributionResult(
        adjusted_players=tuple(adjusted_players),
        absences=tuple(absent_records),
        teammate_adjustments=tuple(teammate_adjustments),
        team_efficiency_delta=team_eff_delta,
        pace_delta=pace_delta,
        rotation_shock_magnitude=magnitude,
        rotation_shock_confidence=confidence,
        mass_conservation_warnings=tuple(warnings),
    )
