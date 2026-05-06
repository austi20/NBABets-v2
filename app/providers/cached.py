from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, timedelta
from typing import Any

from pydantic import BaseModel

from app.config.settings import get_settings
from app.providers.base import InjuriesProvider, OddsProvider, StatsProvider
from app.schemas.domain import (
    GamePayload,
    InjuryPayload,
    LineSnapshotPayload,
    PlayerGameLogPayload,
    PlayerPayload,
    ProviderFetchResult,
    TeamPayload,
)
from app.services.deduplication import (
    dedupe_game_payloads,
    dedupe_injury_payloads,
    dedupe_line_snapshot_payloads,
    dedupe_player_game_log_payloads,
    dedupe_player_payloads,
    dedupe_team_payloads,
)
from app.services.provider_cache import LocalProviderCache


class CachedStatsProvider(StatsProvider):
    provider_name = "cached_stats"

    def __init__(self, provider: StatsProvider, cache: LocalProviderCache) -> None:
        self._provider = provider
        self._cache = cache
        self._cache_provider_name = provider.provider_name
        self._cache_provider_aliases = _cache_provider_names(provider)
        self.provider_name = provider.provider_name
        self._team_scope_abbreviations: set[str] = set()

    async def healthcheck(self) -> bool:
        return await self._provider.healthcheck()

    async def verify_required_access(self) -> None:
        verifier = getattr(self._provider, "verify_required_access", None)
        if verifier is not None:
            await verifier()

    def set_team_scope(self, team_abbreviations: set[str]) -> None:
        self._team_scope_abbreviations = {item.strip().upper() for item in team_abbreviations}
        setter = getattr(self._provider, "set_team_scope", None)
        if setter is not None:
            setter(team_abbreviations)

    async def fetch_teams(self) -> tuple[ProviderFetchResult, list[TeamPayload]]:
        return await self._get_or_fetch_collection(
            provider_type="stats",
            method_name="fetch_teams",
            scope_key=_scope_key(self),
            model_type=TeamPayload,
            fetcher=self._provider.fetch_teams,
            deduper=dedupe_team_payloads,
        )

    async def fetch_rosters(self) -> tuple[ProviderFetchResult, list[PlayerPayload]]:
        return await self._get_or_fetch_collection(
            provider_type="stats",
            method_name="fetch_rosters",
            scope_key=_roster_scope_key(self),
            model_type=PlayerPayload,
            fetcher=self._provider.fetch_rosters,
            deduper=dedupe_player_payloads,
            ttl=timedelta(hours=get_settings().provider_cache_rosters_ttl_hours),
        )

    async def fetch_schedule(self, target_date: date) -> tuple[ProviderFetchResult, list[GamePayload]]:
        scope_key = _scope_key(self, {"target_date": target_date.isoformat()})
        return await self._get_or_fetch_collection(
            provider_type="stats",
            method_name="fetch_schedule",
            scope_key=scope_key,
            model_type=GamePayload,
            fetcher=lambda: self._provider.fetch_schedule(target_date),
            deduper=dedupe_game_payloads,
            target_date=target_date,
            ttl=_schedule_ttl(target_date),
        )

    async def fetch_schedule_range(
        self,
        start_date: date,
        end_date: date,
    ) -> tuple[ProviderFetchResult, list[GamePayload]]:
        requested_days = list(_date_range(start_date, end_date))
        collected_games: list[GamePayload] = []
        cached_days: list[date] = []
        missing_days: list[date] = []
        provider_names = _cache_provider_names(self._provider)

        for game_day in requested_days:
            cached = self._cache.get_collection_with_metadata(
                provider_type="stats",
                provider_names=provider_names,
                method_name="fetch_schedule",
                scope_key=_scope_key(self, {"target_date": game_day.isoformat()}),
                model_type=GamePayload,
            )
            if not self._cache.is_collection_usable(
                record=cached,
                target_date=game_day,
                ttl=_schedule_ttl(game_day),
            ):
                if cached is not None:
                    self._cache.delete_collection(
                        provider_type="stats",
                        provider_name=cached.provider_name,
                        method_name="fetch_schedule",
                        scope_key=_scope_key(self, {"target_date": game_day.isoformat()}),
                    )
                missing_days.append(game_day)
                continue
            cached_days.append(game_day)
            self.provider_name = cached.provider_name
            self._cache_provider_name = cached.provider_name
            collected_games.extend(cached.items)

        fetched_days: list[date] = []
        fetched_at = datetime.now(UTC)
        for segment_start, segment_end in _contiguous_segments(missing_days):
            if hasattr(self._provider, "fetch_schedule_range"):
                result, games = await self._provider.fetch_schedule_range(segment_start, segment_end)
            else:
                segment_games: list[GamePayload] = []
                result = ProviderFetchResult(
                    endpoint=f"cache-miss://{self._cache_provider_name}/fetch_schedule_range",
                    fetched_at=datetime.now(UTC),
                    payload={"start_date": segment_start.isoformat(), "end_date": segment_end.isoformat()},
                )
                current_day = segment_start
                while current_day <= segment_end:
                    _, daily_games = await self._provider.fetch_schedule(current_day)
                    segment_games.extend(daily_games)
                    current_day += timedelta(days=1)
                games = segment_games
            fetched_at = result.fetched_at
            self.provider_name = getattr(self._provider, "provider_name", self.provider_name)
            write_provider_name = _active_provider_cache_name(self._provider)
            self._cache_provider_name = write_provider_name
            grouped = _group_games_by_day(dedupe_game_payloads(games))
            for game_day in _date_range(segment_start, segment_end):
                day_games = grouped.get(game_day, [])
                self._cache.put_collection(
                    provider_type="stats",
                    provider_name=write_provider_name,
                    method_name="fetch_schedule",
                    scope_key=_scope_key(self, {"target_date": game_day.isoformat()}),
                    result=_cache_result(
                        provider_name=write_provider_name,
                        method_name="fetch_schedule",
                        fetched_at=result.fetched_at,
                        payload={"game_date": game_day.isoformat(), "item_count": len(day_games)},
                    ),
                    items=day_games,
                )
                fetched_days.append(game_day)
                collected_games.extend(day_games)

        self.provider_name = self._provider.provider_name
        return (
            _cache_result(
                provider_name=self._cache_provider_name,
                method_name="fetch_schedule_range",
                fetched_at=fetched_at,
                payload={
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "cached_days": [item.isoformat() for item in cached_days],
                    "fetched_days": [item.isoformat() for item in fetched_days],
                    "item_count": len(collected_games),
                },
            ),
            dedupe_game_payloads(collected_games),
        )

    async def fetch_player_game_logs(
        self,
        start_date: date,
        end_date: date,
    ) -> tuple[ProviderFetchResult, list[PlayerGameLogPayload]]:
        requested_days = list(_date_range(start_date, end_date))
        provider_names = _cache_provider_names(self._provider)
        cached_days, missing_days = self._cache.describe_log_day_coverage(
            provider_names=provider_names,
            requested_days=requested_days,
        )
        collected_logs = self._cache.get_player_game_logs(
            provider_names=provider_names,
            requested_days=cached_days,
        )
        fetched_days: list[date] = []
        fetched_at = datetime.now(UTC)

        for segment_start, segment_end in _contiguous_segments(missing_days):
            result, logs = await self._provider.fetch_player_game_logs(segment_start, segment_end)
            fetched_at = result.fetched_at
            self.provider_name = getattr(self._provider, "provider_name", self.provider_name)
            write_provider_name = _active_provider_cache_name(self._provider)
            self._cache.put_player_game_logs(
                provider_name=write_provider_name,
                requested_days=list(_date_range(segment_start, segment_end)),
                result=result,
                logs=logs,
            )
            self._cache_provider_name = write_provider_name
            fetched_days.extend(_date_range(segment_start, segment_end))
            collected_logs.extend(logs)

        sorted_logs = sorted(
            dedupe_player_game_log_payloads(collected_logs),
            key=_player_log_sort_key,
        )
        if cached_days and not fetched_days:
            resolved_name = _active_provider_cache_name(self._provider)
            self.provider_name = resolved_name
            self._cache_provider_name = resolved_name
        return (
            _cache_result(
                provider_name=self._cache_provider_name,
                method_name="fetch_player_game_logs",
                fetched_at=fetched_at,
                payload={
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "cached_days": [item.isoformat() for item in cached_days],
                    "fetched_days": [item.isoformat() for item in fetched_days],
                    "item_count": len(sorted_logs),
                    "sorted_by": "player",
                },
            ),
            sorted_logs,
        )

    async def _get_or_fetch_collection(
        self,
        *,
        provider_type: str,
        method_name: str,
        scope_key: str,
        model_type: type[BaseModel],
        fetcher: Callable[[], Any],
        deduper: Callable[[Iterable[Any]], list[Any]],
        ttl: timedelta | None = None,
        target_date: date | None = None,
        allow_past_reuse: bool = False,
    ) -> tuple[ProviderFetchResult, list[Any]]:
        cached = self._cache.get_collection_with_metadata(
            provider_type=provider_type,
            provider_names=_cache_provider_names(self._provider),
            method_name=method_name,
            scope_key=scope_key,
            model_type=model_type,
        )
        if self._cache.is_collection_usable(
            record=cached,
            target_date=target_date,
            ttl=ttl,
            allow_past_reuse=allow_past_reuse,
        ):
            if cached is not None:
                self.provider_name = cached.provider_name
                self._cache_provider_name = cached.provider_name
                return cached.result, cached.items
        elif cached is not None:
            self._cache.delete_collection(
                provider_type=provider_type,
                provider_name=cached.provider_name,
                method_name=method_name,
                scope_key=scope_key,
            )
        result, items = await fetcher()
        self.provider_name = getattr(self._provider, "provider_name", self.provider_name)
        write_provider_name = _active_provider_cache_name(self._provider)
        deduped = deduper(items)
        self._cache.put_collection(
            provider_type=provider_type,
            provider_name=write_provider_name,
            method_name=method_name,
            scope_key=scope_key,
            result=result,
            items=deduped,
        )
        self._cache_provider_name = write_provider_name
        return result, deduped


class CachedOddsProvider(OddsProvider):
    provider_name = "cached_odds"

    def __init__(self, provider: OddsProvider, cache: LocalProviderCache) -> None:
        self._provider = provider
        self._cache = cache
        self._cache_provider_name = provider.provider_name
        self._cache_provider_aliases = _cache_provider_names(provider)
        self.provider_name = provider.provider_name

    async def healthcheck(self) -> bool:
        return await self._provider.healthcheck()

    async def fetch_upcoming_player_props(
        self,
        target_date: date,
    ) -> tuple[ProviderFetchResult, list[LineSnapshotPayload]]:
        scope_key = json.dumps({"target_date": target_date.isoformat()}, sort_keys=True)
        cached = self._cache.get_collection_with_metadata(
            provider_type="odds",
            provider_names=_cache_provider_names(self._provider),
            method_name="fetch_upcoming_player_props",
            scope_key=scope_key,
            model_type=LineSnapshotPayload,
        )
        if self._cache.is_collection_usable(
            record=cached,
            target_date=target_date,
            ttl=timedelta(minutes=get_settings().provider_cache_odds_ttl_minutes),
            allow_past_reuse=get_settings().provider_cache_allow_past_odds_reuse,
        ):
            if cached is not None:
                self.provider_name = cached.provider_name
                self._cache_provider_name = cached.provider_name
                return cached.result, cached.items
        elif cached is not None:
            self._cache.delete_collection(
                provider_type="odds",
                provider_name=cached.provider_name,
                method_name="fetch_upcoming_player_props",
                scope_key=scope_key,
            )
        result, lines = await self._provider.fetch_upcoming_player_props(target_date)
        self.provider_name = getattr(self._provider, "provider_name", self.provider_name)
        write_provider_name = _active_provider_cache_name(self._provider)
        deduped = dedupe_line_snapshot_payloads(lines)
        self._cache.put_collection(
            provider_type="odds",
            provider_name=write_provider_name,
            method_name="fetch_upcoming_player_props",
            scope_key=scope_key,
            result=result,
            items=deduped,
        )
        self._cache_provider_name = write_provider_name
        return result, deduped


class CachedInjuriesProvider(InjuriesProvider):
    provider_name = "cached_injuries"

    def __init__(self, provider: InjuriesProvider, cache: LocalProviderCache) -> None:
        self._provider = provider
        self._cache = cache
        self._cache_provider_name = provider.provider_name
        self._cache_provider_aliases = _cache_provider_names(provider)
        self.provider_name = provider.provider_name

    async def healthcheck(self) -> bool:
        return await self._provider.healthcheck()

    async def fetch_injuries(
        self,
        target_date: date | None = None,
    ) -> tuple[ProviderFetchResult, list[InjuryPayload]]:
        scope_key = json.dumps({"target_date": target_date.isoformat() if target_date else "all"}, sort_keys=True)
        cached = self._cache.get_collection_with_metadata(
            provider_type="injuries",
            provider_names=_cache_provider_names(self._provider),
            method_name="fetch_injuries",
            scope_key=scope_key,
            model_type=InjuryPayload,
        )
        if self._cache.is_collection_usable(
            record=cached,
            target_date=target_date,
            ttl=timedelta(minutes=get_settings().provider_cache_injuries_ttl_minutes),
        ):
            if cached is not None:
                self.provider_name = cached.provider_name
                self._cache_provider_name = cached.provider_name
                return cached.result, cached.items
        elif cached is not None:
            self._cache.delete_collection(
                provider_type="injuries",
                provider_name=cached.provider_name,
                method_name="fetch_injuries",
                scope_key=scope_key,
            )
        result, injuries = await self._provider.fetch_injuries(target_date)
        self.provider_name = getattr(self._provider, "provider_name", self.provider_name)
        write_provider_name = _active_provider_cache_name(self._provider)
        deduped = dedupe_injury_payloads(injuries)
        self._cache.put_collection(
            provider_type="injuries",
            provider_name=write_provider_name,
            method_name="fetch_injuries",
            scope_key=scope_key,
            result=result,
            items=deduped,
        )
        self._cache_provider_name = write_provider_name
        return result, deduped


def wrap_with_provider_cache(provider: Any) -> Any:
    settings = get_settings()
    if not settings.enable_provider_cache:
        return provider
    cache = LocalProviderCache()
    if isinstance(provider, StatsProvider):
        return CachedStatsProvider(provider, cache)
    if isinstance(provider, OddsProvider):
        return CachedOddsProvider(provider, cache)
    if isinstance(provider, InjuriesProvider):
        return CachedInjuriesProvider(provider, cache)
    return provider


def _cache_result(
    *,
    provider_name: str,
    method_name: str,
    fetched_at: datetime,
    payload: dict[str, Any],
) -> ProviderFetchResult:
    return ProviderFetchResult(
        endpoint=f"cache://{provider_name}/{method_name}",
        fetched_at=fetched_at,
        payload=payload,
    )


def _date_range(start_date: date, end_date: date) -> list[date]:
    days: list[date] = []
    current_day = start_date
    while current_day <= end_date:
        days.append(current_day)
        current_day += timedelta(days=1)
    return days


def _contiguous_segments(days: Iterable[date]) -> list[tuple[date, date]]:
    ordered = sorted(set(days))
    if not ordered:
        return []
    segments: list[tuple[date, date]] = []
    segment_start = ordered[0]
    previous = ordered[0]
    for current in ordered[1:]:
        if current == previous + timedelta(days=1):
            previous = current
            continue
        segments.append((segment_start, previous))
        segment_start = current
        previous = current
    segments.append((segment_start, previous))
    return segments


def _group_games_by_day(games: Iterable[GamePayload]) -> dict[date, list[GamePayload]]:
    grouped: dict[date, list[GamePayload]] = {}
    for payload in games:
        grouped.setdefault(payload.game_date, []).append(payload)
    return grouped


def _scope_key(provider: Any, extra: dict[str, Any] | None = None) -> str:
    payload = {"team_scope": sorted(getattr(provider, "_team_scope_abbreviations", set()) or [])}
    if extra:
        payload.update(extra)
    return json.dumps(payload, sort_keys=True)


def _roster_scope_key(provider: Any) -> str:
    return _scope_key(provider, {"season_context": _season_context()})


def _schedule_ttl(target_date: date) -> timedelta | None:
    if target_date < datetime.now(UTC).date():
        return None
    return timedelta(minutes=get_settings().provider_cache_schedule_ttl_minutes)


def _season_context(today: date | None = None) -> str:
    current_day = today or datetime.now(UTC).date()
    start_year = current_day.year if current_day.month >= 10 else current_day.year - 1
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def _cache_provider_names(provider: Any) -> list[str]:
    aliases = list(getattr(provider, "_cache_provider_aliases", []) or [])
    current_name = getattr(provider, "provider_name", None)
    fallback_name = getattr(provider, "_cache_provider_name", None)
    ordered = aliases.copy()
    if current_name:
        ordered.append(str(current_name))
    if fallback_name:
        ordered.append(str(fallback_name))
    return [item for item in dict.fromkeys(ordered) if item]


def _active_provider_cache_name(provider: Any) -> str:
    current_name = str(getattr(provider, "provider_name", "") or "")
    if current_name and not current_name.startswith("rotating_"):
        return current_name
    primary_name = getattr(provider, "_cache_primary_provider_name", None)
    if primary_name:
        return str(primary_name)
    names = _cache_provider_names(provider)
    if names:
        return names[0]
    return current_name


def _player_log_sort_key(payload: PlayerGameLogPayload) -> tuple[str, str, str]:
    player_name = str(payload.meta.get("player_name") or payload.provider_player_id).strip().lower()
    game_day = str(payload.meta.get("game_date") or "")
    return (player_name, payload.provider_player_id, game_day)
