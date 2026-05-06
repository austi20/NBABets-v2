from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import UTC, date, datetime
from time import monotonic
from typing import Any

import httpx
from dateutil.parser import isoparse

from app.config.settings import get_settings
from app.providers.base import StatsProvider
from app.providers.http import HttpProviderMixin
from app.schemas.domain import (
    GamePayload,
    PlayerGameLogPayload,
    PlayerPayload,
    ProviderFetchResult,
    TeamPayload,
)

logger = logging.getLogger(__name__)


class BallDontLieStatsProvider(HttpProviderMixin, StatsProvider):
    provider_name = "balldontlie"
    base_url = "https://api.balldontlie.io/v1"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = get_settings().balldontlie_api_key
        self._bdl_max_rpm = int(self._settings.balldontlie_max_rpm)
        self._bdl_window_s = float(self._settings.balldontlie_rate_window_seconds)
        self._bdl_throttle_lock = asyncio.Lock()
        self._bdl_request_times: deque[float] = deque()
        self._team_lookup: dict[int, str] = {}
        self._scheduled_team_ids: set[int] = set()
        self._active_roster_player_ids: set[int] = set()
        self._team_scope_abbreviations: set[str] = set()
        self._relevant_game_ids: set[int] = set()

    async def _get(
        self,
        endpoint: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> ProviderFetchResult:
        await self._acquire_bdl_rate_slot(endpoint)
        return await HttpProviderMixin._get(self, endpoint, headers=headers, params=params)

    async def _acquire_bdl_rate_slot(self, endpoint: str) -> None:
        """Rolling-window limiter (default ~540 req / 60s) under BDL 600 rpm capacity."""
        rpm = self._bdl_max_rpm
        if rpm <= 0:
            return
        window = self._bdl_window_s
        async with self._bdl_throttle_lock:
            while True:
                now = monotonic()
                while self._bdl_request_times and self._bdl_request_times[0] <= now - window:
                    self._bdl_request_times.popleft()
                if len(self._bdl_request_times) < rpm:
                    self._bdl_request_times.append(monotonic())
                    return
                wait_s = self._bdl_request_times[0] + window - now
                wait_s = max(0.0, min(wait_s, window))
                logger.info(
                    "balldontlie throttle wait: endpoint=%s wait_seconds=%.3f in_window=%s rpm_limit=%s",
                    endpoint,
                    wait_s,
                    len(self._bdl_request_times),
                    rpm,
                )
                await asyncio.sleep(wait_s)

    async def healthcheck(self) -> bool:
        result = await self._get("/teams", headers=self._headers(), params={"per_page": 100})
        return "data" in result.payload

    async def verify_required_access(self) -> None:
        try:
            await self._get("/stats", headers=self._headers(), params={"per_page": 1})
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 401:
                raise ValueError(
                    "BALLDONTLIE_API_KEY is accepted for basic endpoints but does not have access to /stats, "
                    "which this app requires for historical player logs."
                ) from exc
            raise

    async def fetch_teams(self) -> tuple[Any, list[TeamPayload]]:
        result = await self._get_paginated("/teams")
        deduped: dict[str, dict[str, Any]] = {}
        for item in result.payload.get("data", []):
            abbreviation = item.get("abbreviation")
            if not abbreviation or abbreviation in deduped:
                continue
            deduped[str(abbreviation)] = item
        teams = [
            TeamPayload(
                provider_team_id=str(item["id"]),
                abbreviation=item["abbreviation"],
                name=item["full_name"],
                city=item.get("city"),
            )
            for item in deduped.values()
        ]
        self._team_lookup = {
            int(item["id"]): item["abbreviation"]
            for item in deduped.values()
            if item.get("id") is not None and item.get("abbreviation")
        }
        return result, teams

    async def fetch_rosters(self) -> tuple[Any, list[PlayerPayload]]:
        if not self._team_lookup:
            await self.fetch_teams()
        try:
            result = await self._get_paginated("/players/active")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code not in {401, 403, 404}:
                raise
            result = await self._get_paginated(
                "/players",
                params={
                    "team_ids[]": sorted(self._scheduled_team_ids or set(self._team_lookup)),
                },
            )
        players = [
            PlayerPayload(
                provider_player_id=str(item["id"]),
                full_name=f"{item['first_name']} {item['last_name']}".strip(),
                team_abbreviation=self._team_abbreviation(item.get("team")),
                position=item.get("position") or None,
                status="active",
            )
            for item in result.payload.get("data", [])
        ]
        self._active_roster_player_ids = {
            int(item["id"])
            for item in result.payload.get("data", [])
            if item.get("id") is not None
        }
        return result, players

    async def fetch_schedule(self, target_date: date) -> tuple[Any, list[GamePayload]]:
        result, games = await self.fetch_schedule_range(target_date, target_date)
        return result, games

    async def fetch_schedule_range(self, start_date: date, end_date: date) -> tuple[Any, list[GamePayload]]:
        dates = [
            date.fromordinal(value).isoformat()
            for value in range(start_date.toordinal(), end_date.toordinal() + 1)
        ]
        result = await self._get_paginated(
            "/games",
            params={"dates[]": dates, "per_page": 100},
        )
        if end_date >= date.today() >= start_date:
            self._scheduled_team_ids.clear()
            self._relevant_game_ids.clear()
        games = [
            GamePayload(
                provider_game_id=str(item["id"]),
                game_date=isoparse(item["date"]).date(),
                start_time=_parse_game_start(item),
                home_team_abbreviation=self._team_abbreviation(item["home_team"]),
                away_team_abbreviation=self._team_abbreviation(item["visitor_team"]),
                season_code=str(item["season"]),
                status=_normalize_game_status(item.get("status", "scheduled")),
            )
            for item in result.payload.get("data", [])
            if self._game_in_scope(item)
        ]
        for item in result.payload.get("data", []):
            if not self._game_in_scope(item):
                continue
            self._remember_team(item.get("home_team"))
            self._remember_team(item.get("visitor_team"))
            game_id = item.get("id")
            if game_id is not None:
                self._relevant_game_ids.add(int(game_id))
        return result, games

    async def fetch_player_game_logs(
        self,
        start_date: date,
        end_date: date,
    ) -> tuple[Any, list[PlayerGameLogPayload]]:
        result = await self._get_paginated(
            "/stats",
            params={
                "per_page": 100,
                **(
                    {"game_ids[]": sorted(self._relevant_game_ids)}
                    if self._relevant_game_ids
                    else {
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    }
                ),
                **(
                    {"player_ids[]": sorted(self._active_roster_player_ids)}
                    if self._active_roster_player_ids
                    else {}
                ),
            },
        )
        logs: list[PlayerGameLogPayload] = []
        for item in result.payload.get("data", []):
            player = item.get("player", {})
            game = item.get("game", {})
            team = item.get("team", {})
            team_id = team.get("id") or item.get("team_id")
            home_team_id = game.get("home_team_id") or item.get("home_team_id")
            visitor_team_id = game.get("visitor_team_id") or item.get("visitor_team_id")
            opponent_team_id = None
            if team_id is not None and home_team_id is not None and visitor_team_id is not None:
                if int(team_id) == int(home_team_id):
                    opponent_team_id = int(visitor_team_id)
                elif int(team_id) == int(visitor_team_id):
                    opponent_team_id = int(home_team_id)
            logs.append(
                PlayerGameLogPayload(
                    provider_game_id=str(game["id"]),
                    provider_player_id=str(player["id"]),
                    team_abbreviation=self._team_abbreviation(team),
                    opponent_abbreviation=self._team_lookup.get(opponent_team_id) if opponent_team_id is not None else None,
                    minutes=_parse_minutes(item.get("min")),
                    points=item.get("pts", 0),
                    rebounds=item.get("reb", 0),
                    assists=item.get("ast", 0),
                    threes=item.get("fg3m", 0),
                    steals=item.get("stl", 0),
                    blocks=item.get("blk", 0),
                    turnovers=item.get("turnover", 0),
                    fouls=item.get("pf", 0),
                    field_goal_attempts=item.get("fga", 0),
                    field_goals_made=item.get("fgm", 0),
                    free_throw_attempts=item.get("fta", 0),
                    free_throws_made=item.get("ftm", 0),
                    offensive_rebounds=item.get("oreb", 0),
                    defensive_rebounds=item.get("dreb", 0),
                    plus_minus=item.get("plus_minus"),
                    starter_flag=item.get("starter", False),
                    overtime_flag=False,
                    meta={
                        "game_status": game.get("status"),
                        "player_name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
                        "position": player.get("position"),
                        "game_date": _parse_game_date(game.get("date") or item.get("game_date")),
                    },
                )
            )
        return result, logs

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self._api_key:
            headers["Authorization"] = self._api_key
        return headers

    async def _get_paginated(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        query_params = dict(params or {})
        query_params.setdefault("per_page", 100)
        all_rows: list[dict[str, Any]] = []
        pages = 0
        cursor = query_params.pop("cursor", None)
        final_result = None

        while True:
            page_params = dict(query_params)
            if cursor is not None:
                page_params["cursor"] = cursor
            result = await self._get(endpoint, headers=self._headers(), params=page_params)
            final_result = result
            payload = result.payload
            all_rows.extend(payload.get("data", []))
            pages += 1
            cursor = payload.get("meta", {}).get("next_cursor")
            if cursor in (None, ""):
                break

        if final_result is None:
            raise RuntimeError(f"No response returned for endpoint {endpoint}")
        return final_result.model_copy(
            update={
                "payload": {
                    "data": all_rows,
                    "meta": {
                        "page_count": pages,
                        "record_count": len(all_rows),
                        "per_page": query_params.get("per_page"),
                    },
                }
            }
        )

    def _team_abbreviation(self, team: dict[str, Any] | None) -> str | None:
        if not team:
            return None
        abbreviation = team.get("abbreviation")
        if abbreviation:
            team_id = team.get("id")
            if team_id is not None:
                self._team_lookup[int(team_id)] = abbreviation
            return str(abbreviation)
        team_id = team.get("id")
        if team_id is None:
            return None
        return self._team_lookup.get(int(team_id))

    def _remember_team(self, team: dict[str, Any] | None) -> None:
        if not team:
            return
        team_id = team.get("id")
        abbreviation = team.get("abbreviation")
        if team_id is not None and abbreviation:
            self._team_lookup[int(team_id)] = str(abbreviation)
            self._scheduled_team_ids.add(int(team_id))

    def set_team_scope(self, team_abbreviations: set[str]) -> None:
        self._team_scope_abbreviations = {value.upper() for value in team_abbreviations if value}

    def _game_in_scope(self, item: dict[str, Any]) -> bool:
        if not self._team_scope_abbreviations:
            return True
        home = str(item.get("home_team", {}).get("abbreviation", "")).upper()
        away = str(item.get("visitor_team", {}).get("abbreviation", "")).upper()
        return home in self._team_scope_abbreviations or away in self._team_scope_abbreviations


_FINAL_STATUSES = frozenset({"Final", "final", "FINAL"})
_IN_PROGRESS_KEYWORDS = frozenset({"Qtr", "qtr", "Half", "half", "OT", "ot"})


def _normalize_game_status(raw_status: str) -> str:
    """Normalize BallDontLie game status to one of: scheduled, completed, in_progress."""
    if not raw_status:
        return "scheduled"
    if raw_status in _FINAL_STATUSES:
        return "completed"
    if any(kw in raw_status for kw in _IN_PROGRESS_KEYWORDS):
        return "in_progress"
    # BallDontLie returns the start time ISO string for upcoming games
    if raw_status.startswith("20") and "T" in raw_status:
        return "scheduled"
    return raw_status


def _parse_game_start(item: dict[str, Any]) -> datetime:
    raw_value = item.get("datetime") or item.get("date")
    if raw_value is None:
        raise ValueError("BallDontLie game payload missing datetime/date")
    parsed = isoparse(str(raw_value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _parse_minutes(raw_minutes: str | int | float | None) -> float:
    if raw_minutes is None:
        return 0.0
    if isinstance(raw_minutes, (int, float)):
        return float(raw_minutes)
    if ":" not in raw_minutes:
        return float(raw_minutes)
    minutes, seconds = raw_minutes.split(":")
    return float(minutes) + float(seconds) / 60.0


def _parse_game_date(raw_value: object) -> str | None:
    if raw_value in (None, ""):
        return None
    return isoparse(str(raw_value)).date().isoformat()
