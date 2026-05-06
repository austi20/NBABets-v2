from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

from app.schemas.domain import (
    GamePayload,
    InjuryPayload,
    LineSnapshotPayload,
    PlayerGameLogPayload,
    PlayerPayload,
    TeamPayload,
)
from app.services.name_matching import normalize_name

_GAME_STATUS_PRIORITY = {
    "scheduled": 1,
    "postponed": 2,
    "live": 3,
    "final": 4,
}


def dedupe_team_payloads(payloads: Iterable[TeamPayload]) -> list[TeamPayload]:
    deduped: dict[str, TeamPayload] = {}
    for payload in payloads:
        key = payload.abbreviation.strip().upper()
        existing = deduped.get(key)
        if existing is None or _team_score(payload) > _team_score(existing):
            deduped[key] = payload
    return list(deduped.values())


def dedupe_player_payloads(payloads: Iterable[PlayerPayload]) -> list[PlayerPayload]:
    deduped: dict[tuple[str, str], PlayerPayload] = {}
    for payload in payloads:
        team_abbreviation = str(payload.team_abbreviation or "").strip().upper()
        key = (normalize_name(payload.full_name), team_abbreviation)
        existing = deduped.get(key)
        if existing is None or _player_score(payload) > _player_score(existing):
            deduped[key] = payload
    return list(deduped.values())


def dedupe_game_payloads(payloads: Iterable[GamePayload]) -> list[GamePayload]:
    deduped: dict[tuple[object, ...], GamePayload] = {}
    for payload in payloads:
        key = (
            payload.game_date,
            payload.home_team_abbreviation.strip().upper(),
            payload.away_team_abbreviation.strip().upper(),
        )
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = payload
            continue
        deduped[key] = _prefer_game_payload(existing, payload)
    return list(deduped.values())


def dedupe_player_game_log_payloads(payloads: Iterable[PlayerGameLogPayload]) -> list[PlayerGameLogPayload]:
    deduped: dict[tuple[object, ...], PlayerGameLogPayload] = {}
    for payload in payloads:
        player_name = str(payload.meta.get("player_name") or payload.provider_player_id)
        key = (
            payload.provider_game_id,
            normalize_name(player_name),
            payload.team_abbreviation.strip().upper(),
        )
        existing = deduped.get(key)
        if existing is None or _player_game_log_score(payload) > _player_game_log_score(existing):
            deduped[key] = payload
    return list(deduped.values())


def dedupe_injury_payloads(payloads: Iterable[InjuryPayload]) -> list[InjuryPayload]:
    deduped: dict[tuple[object, ...], InjuryPayload] = {}
    for payload in payloads:
        key = (
            payload.provider_player_id,
            payload.team_abbreviation.strip().upper(),
            payload.provider_game_id or "",
            payload.report_timestamp,
            payload.status.strip().lower(),
            str(payload.designation or "").strip().lower(),
        )
        existing = deduped.get(key)
        if existing is None or _injury_score(payload) > _injury_score(existing):
            deduped[key] = payload
    return list(deduped.values())


def dedupe_line_snapshot_payloads(payloads: Iterable[LineSnapshotPayload]) -> list[LineSnapshotPayload]:
    deduped: dict[tuple[object, ...], LineSnapshotPayload] = {}
    for payload in payloads:
        existing = deduped.get(_line_snapshot_key(payload))
        if existing is None or _line_snapshot_score(payload) > _line_snapshot_score(existing):
            deduped[_line_snapshot_key(payload)] = payload
    return list(deduped.values())


def _prefer_game_payload(existing: GamePayload, candidate: GamePayload) -> GamePayload:
    if _game_score(candidate) > _game_score(existing):
        return candidate.model_copy(
            update={
                "provider_game_id": existing.provider_game_id or candidate.provider_game_id,
                "meta": _merge_provider_alias_meta(existing, candidate),
            }
        )
    return existing.model_copy(
        update={
            "provider_game_id": existing.provider_game_id or candidate.provider_game_id,
            "meta": _merge_provider_alias_meta(existing, candidate),
        }
    )


def _merge_provider_alias_meta(*payloads: GamePayload) -> dict[str, object]:
    meta: dict[str, object] = {}
    provider_ids: set[str] = set()
    for payload in payloads:
        meta.update(payload.meta)
        provider_ids.add(str(payload.provider_game_id))
        existing_aliases = payload.meta.get("provider_game_ids")
        if isinstance(existing_aliases, list):
            provider_ids.update(str(item) for item in existing_aliases if item is not None)
    meta["provider_game_ids"] = sorted(provider_ids)
    return meta


def _line_snapshot_key(payload: LineSnapshotPayload) -> tuple[object, ...]:
    player_name = str(payload.meta.get("player_name") or payload.provider_player_id)
    event_start = str(payload.meta.get("event_start_time") or payload.timestamp.date().isoformat())
    home_team = str(payload.meta.get("home_team_abbreviation") or "").strip().upper()
    away_team = str(payload.meta.get("away_team_abbreviation") or "").strip().upper()
    return (
        event_start,
        home_team,
        away_team,
        payload.sportsbook_key.strip().lower(),
        normalize_name(player_name),
        payload.market_key.strip().lower(),
        float(payload.line_value),
        payload.over.odds,
        payload.under.odds,
        str(payload.event_status or "").strip().lower(),
    )


def _team_score(payload: TeamPayload) -> tuple[int, int]:
    return (int(bool(payload.city)), len(payload.name))


def _player_score(payload: PlayerPayload) -> tuple[int, int, int]:
    return (
        int(bool(payload.team_abbreviation)),
        int(bool(payload.position)),
        int(bool(payload.status)),
    )


def _game_score(payload: GamePayload) -> tuple[int, int, float]:
    return (
        _GAME_STATUS_PRIORITY.get(payload.status.strip().lower(), 0),
        int(bool(payload.total)) + int(bool(payload.spread)),
        _timestamp_value(payload.start_time),
    )


def _player_game_log_score(payload: PlayerGameLogPayload) -> tuple[float, int]:
    counting_stats = payload.points + payload.rebounds + payload.assists + payload.threes + payload.turnovers
    return (float(payload.minutes), counting_stats)


def _injury_score(payload: InjuryPayload) -> tuple[int, int, int]:
    return (
        int(payload.expected_availability_flag is not None),
        int(bool(payload.body_part)),
        int(bool(payload.notes)),
    )


def _line_snapshot_score(payload: LineSnapshotPayload) -> tuple[float, int]:
    return (
        _timestamp_value(payload.timestamp),
        int(payload.over.odds is not None) + int(payload.under.odds is not None),
    )


def _timestamp_value(value: datetime) -> float:
    if value.tzinfo is not None:
        return value.timestamp()
    return value.replace(tzinfo=None).timestamp()
