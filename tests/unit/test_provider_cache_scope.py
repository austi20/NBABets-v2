from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from pathlib import Path

from app.providers.cached import CachedStatsProvider
from app.schemas.domain import PlayerGameLogPayload, ProviderFetchResult
from app.services.provider_cache import LocalProviderCache


def _log(game_day: date, player: str = "1") -> PlayerGameLogPayload:
    return PlayerGameLogPayload(
        provider_game_id=f"game-{game_day.isoformat()}",
        provider_player_id=player,
        team_abbreviation="CLE",
        minutes=30,
        points=10,
        rebounds=5,
        assists=4,
        threes=1,
        meta={"game_date": game_day.isoformat(), "player_name": f"Player {player}"},
    )


def _result() -> ProviderFetchResult:
    return ProviderFetchResult(endpoint="fake://stats", fetched_at=datetime.now(UTC), payload={})


class _ScopedStatsProvider:
    provider_name = "fake_stats"

    def __init__(self) -> None:
        self._team_scope_abbreviations: set[str] = set()
        self.fetch_count = 0

    def set_team_scope(self, team_abbreviations: set[str]) -> None:
        self._team_scope_abbreviations = {value.upper() for value in team_abbreviations}

    async def fetch_player_game_logs(
        self,
        start_date: date,
        end_date: date,
    ) -> tuple[ProviderFetchResult, list[PlayerGameLogPayload]]:
        self.fetch_count += 1
        return _result(), [_log(start_date)]


def test_repair_legacy_scoped_log_cache_runs_once(tmp_path: Path) -> None:
    cache = LocalProviderCache(path=tmp_path / "provider_cache.sqlite")
    game_day = date(2026, 5, 8)
    assert cache.put_player_game_logs(
        provider_name="fake_stats",
        requested_days=[game_day],
        result=_result(),
        logs=[_log(game_day)],
    )
    assert cache.get_cached_log_days(provider_name="fake_stats", requested_days=[game_day]) == {game_day}

    first = cache.repair_legacy_scoped_log_cache()
    second = cache.repair_legacy_scoped_log_cache()

    assert first["ran"] is True
    assert first["deleted_provider_cached_log_days"] == 1
    assert first["deleted_provider_cached_logs"] == 1
    assert second["ran"] is False
    assert cache.get_cached_log_days(provider_name="fake_stats", requested_days=[game_day]) == set()


def test_scoped_log_fetch_bypasses_complete_day_cache(tmp_path: Path) -> None:
    cache = LocalProviderCache(path=tmp_path / "provider_cache.sqlite")
    provider = _ScopedStatsProvider()
    cached = CachedStatsProvider(provider, cache)  # type: ignore[arg-type]
    cached.set_team_scope({"CLE", "DET"})
    game_day = date(2026, 5, 8)

    result, logs = asyncio.run(cached.fetch_player_game_logs(game_day, game_day))

    assert provider.fetch_count == 1
    assert len(logs) == 1
    assert result.payload["cache_mode"] == "bypassed_team_scope"
    assert cache.get_cached_log_days(provider_name="fake_stats", requested_days=[game_day]) == set()
