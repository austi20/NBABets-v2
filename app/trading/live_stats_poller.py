from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class LivePlayerStats:
    player_id: int
    points: int
    rebounds: int
    assists: int
    threes: int
    turnovers: int
    minutes_played: float
    period: int
    clock_seconds_remaining: float
    last_updated: datetime


def _parse_minutes(raw: Any) -> float:
    """Parse a BDL 'min' field ("MM:SS", int, float, or None) -> minutes float."""
    if raw is None or raw == "":
        return 0.0
    if isinstance(raw, (int, float)):
        return float(raw)
    raw = str(raw).strip()
    if not raw:
        return 0.0
    if ":" not in raw:
        return float(raw)
    minutes, seconds = raw.split(":", 1)
    return float(minutes) + float(seconds) / 60.0


def _parse_clock(raw: Any) -> float:
    """Parse a BDL game 'time' field ("M:SS" clock) -> total seconds float."""
    if raw is None or raw == "":
        return 0.0
    raw = str(raw).strip()
    if not raw:
        return 0.0
    if ":" not in raw:
        try:
            return float(raw)
        except ValueError:
            return 0.0
    minutes, seconds = raw.split(":", 1)
    try:
        return float(minutes) * 60.0 + float(seconds)
    except ValueError:
        return 0.0


def parse_live_stats(payload: dict[str, Any]) -> list[LivePlayerStats]:
    """Map a BDL /stats response payload to LivePlayerStats objects."""
    now = datetime.now(UTC)
    results: list[LivePlayerStats] = []
    for row in payload.get("data", []):
        player = row.get("player", {})
        player_id_raw = player.get("id")
        if player_id_raw is None:
            continue
        game = row.get("game", {})
        results.append(
            LivePlayerStats(
                player_id=int(player_id_raw),
                points=int(row.get("pts") or 0),
                rebounds=int(row.get("reb") or 0),
                assists=int(row.get("ast") or 0),
                threes=int(row.get("fg3m") or 0),
                turnovers=int(row.get("turnover") or 0),
                minutes_played=_parse_minutes(row.get("min")),
                period=int(game.get("period") or 0),
                clock_seconds_remaining=_parse_clock(game.get("time")),
                last_updated=now,
            )
        )
    return results
