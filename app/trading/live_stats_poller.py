from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.trading.live_limits import ActiveTraderConfig


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
        try:
            return float(raw)
        except ValueError:
            return 0.0
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


# market_key -> the LivePlayerStats field(s) summed to form the live stat.
# Mirrors app/training/constants.py MARKET_TARGETS (pra = points+rebounds+assists).
_STAT_FIELDS: dict[str, tuple[str, ...]] = {
    "points": ("points",),
    "rebounds": ("rebounds",),
    "assists": ("assists",),
    "threes": ("threes",),
    "turnovers": ("turnovers",),
    "pra": ("points", "rebounds", "assists"),
}

_QUARTER_MINUTES = 12.0
_REGULATION_PERIODS = 4
_REGULATION_MINUTES = _QUARTER_MINUTES * _REGULATION_PERIODS  # 48.0


def _remaining_minutes(stat: LivePlayerStats) -> float:
    """Game minutes left, linear projection horizon.

    Regulation quarters after the current one are assumed full; in the 4th
    period or any overtime only the current clock counts (no assumed extra OT).
    Period 0/unknown is treated as a full regulation game.
    """
    clock_minutes = max(0.0, stat.clock_seconds_remaining / 60.0)
    period = stat.period
    if period <= 0:
        return _REGULATION_MINUTES
    if period < _REGULATION_PERIODS:
        return clock_minutes + (_REGULATION_PERIODS - period) * _QUARTER_MINUTES
    return clock_minutes


_FINAL_MARKER = "final"


def _final_player_ids(payload: dict[str, Any]) -> set[int]:
    """player_ids whose game.status marks the game as final."""
    finals: set[int] = set()
    for row in payload.get("data", []):
        pid = row.get("player", {}).get("id")
        if pid is None:
            continue
        status = str(row.get("game", {}).get("status") or "")
        if _FINAL_MARKER in status.lower():
            finals.add(int(pid))
    return finals


class LiveStatsPoller:
    """Polls BDL live stats for watched players on a fixed cadence.

    The set of players to watch is supplied by ``player_ids_source`` (union of
    open positions + live candidates). Once a player's game goes final we stop
    re-polling them, but their last stats stay readable in the view. All I/O and
    timing go through injected seams (``fetch``, ``sleep``) so tests need no
    network and no real time. Cancel the ``run()`` task to stop in production.

    Spec: docs/plans/specs/profit-machine-design.md §3.3.
    """

    def __init__(
        self,
        *,
        config: ActiveTraderConfig,
        player_ids_source: Callable[[], Iterable[int]],
        fetch: Callable[[list[int]], Awaitable[dict[str, Any]]],
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._config = config
        self._player_ids_source = player_ids_source
        self._fetch = fetch
        self._sleep = sleep
        self._view: dict[int, LivePlayerStats] = {}
        self._final: set[int] = set()
        self._stopping = False

    # ------------------------------------------------------------------ accessors
    def stats(self, player_id: int) -> LivePlayerStats | None:
        return self._view.get(player_id)

    def snapshot(self) -> Mapping[int, LivePlayerStats]:
        return dict(self._view)

    # ------------------------------------------------------------------ lifecycle
    def stop(self) -> None:
        self._stopping = True

    async def run(self) -> None:
        while not self._stopping:
            await self._poll_once()
            if self._stopping:
                break
            await self._sleep(self._config.live_stats_poll_seconds)

    async def _poll_once(self) -> None:
        watched = set(self._player_ids_source()) - self._final
        if not watched:
            return
        payload = await self._fetch(sorted(watched))
        for stat in parse_live_stats(payload):
            self._view[stat.player_id] = stat
        self._final |= _final_player_ids(payload)

    # ------------------------------------------------------------------ pace
    def current_stat(self, player_id: int, market_key: str) -> float | None:
        """Live value of ``market_key`` for the player, or None if no data."""
        stat = self._view.get(player_id)
        if stat is None:
            return None
        fields = _STAT_FIELDS.get(market_key.lower())
        if fields is None:
            return None
        return float(sum(getattr(stat, name) for name in fields))

    def on_pace_to_hit(
        self,
        player_id: int,
        market_key: str,
        line_value: float,
        side: str = "over",
    ) -> bool:
        """Linear projection of the live stat to game end vs the line.

        ``projected = current + remaining_minutes * (current / minutes_played)``.
        For YES-OVER, on pace means ``projected >= line``; for under, ``<=``.
        Returns False when there is no live data — callers that need BDL-outage
        "hold" semantics must check ``current_stat() is None`` first (the FSM
        adapter raises in that case so the position holds).
        """
        current = self.current_stat(player_id, market_key)
        if current is None:
            return False
        stat = self._view[player_id]
        rate = current / stat.minutes_played if stat.minutes_played > 0 else 0.0
        projected = current + _remaining_minutes(stat) * rate
        if side.lower() == "under":
            return projected <= line_value
        return projected >= line_value

    def is_signaling_entry(
        self, player_id: int, market_key: str, line_value: float
    ) -> bool:
        """True when the live stat is on track to clear the line early.

        ``current >= line * pace_threshold_pct`` AND at least
        ``pace_remaining_pct_min`` of a regulation game still remains.
        """
        current = self.current_stat(player_id, market_key)
        if current is None:
            return False
        if current < line_value * self._config.pace_threshold_pct:
            return False
        stat = self._view[player_id]
        remaining_fraction = _remaining_minutes(stat) / _REGULATION_MINUTES
        return remaining_fraction >= self._config.pace_remaining_pct_min
