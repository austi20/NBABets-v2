from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace
from typing import Any

from app.tasks import ingestion


class _StatsProvider:
    provider_name = "fake_stats"

    def __init__(self) -> None:
        self._team_scope_abbreviations: set[str] = set()

    def set_team_scope(self, team_abbreviations: set[str]) -> None:
        self._team_scope_abbreviations = {value.upper() for value in team_abbreviations}

    async def fetch_game_availability(self, _target_date: date) -> tuple[Any, list[Any]]:
        return SimpleNamespace(payload={}), []


class _OddsProvider:
    provider_name = "fake_odds"

    async def fetch_upcoming_player_props(self, _target_date: date) -> tuple[Any, list[Any]]:
        return SimpleNamespace(payload={}), [
            SimpleNamespace(meta={"home_team_abbreviation": "CLE", "away_team_abbreviation": "DET"})
        ]


class _InjuriesProvider:
    provider_name = "fake_injuries"


class _Orchestrator:
    scopes: dict[str, set[str]] = {}

    def __init__(self, _session: object) -> None:
        return None

    async def refresh_reference_data(self, stats_provider: _StatsProvider, _target_date: date) -> dict[str, int]:
        self.scopes["reference"] = set(stats_provider._team_scope_abbreviations)
        return {}

    async def refresh_reference_history(
        self,
        stats_provider: _StatsProvider,
        _start_date: date,
        _end_date: date,
    ) -> dict[str, int]:
        self.scopes["history"] = set(stats_provider._team_scope_abbreviations)
        return {}

    async def ingest_game_logs(
        self,
        stats_provider: _StatsProvider,
        _start_date: date,
        _end_date: date,
    ) -> dict[str, int]:
        self.scopes["logs"] = set(stats_provider._team_scope_abbreviations)
        return {}

    async def ingest_game_availability(self, stats_provider: _StatsProvider, _target_date: date) -> dict[str, Any]:
        self.scopes["availability"] = set(stats_provider._team_scope_abbreviations)
        return {"_changed_game_ids": set()}

    async def ingest_injuries(self, _injuries_provider: _InjuriesProvider, _target_date: date) -> dict[str, int]:
        return {}

    async def ingest_odds(
        self,
        _odds_provider: _OddsProvider,
        _target_date: date,
        _prefetched_result: object | None,
        _prefetched_lines: list[Any] | None,
    ) -> dict[str, int]:
        return {}

    def mark_closing_lines(self, _target_date: date) -> dict[str, int]:
        return {}


@contextmanager
def _session_scope():
    yield object()


def test_refresh_all_clears_team_scope_for_historical_steps(monkeypatch) -> None:
    stats_provider = _StatsProvider()
    _Orchestrator.scopes = {}
    monkeypatch.setattr(
        ingestion,
        "get_settings",
        lambda: SimpleNamespace(startup_history_days=150, enable_provider_cache=False),
    )
    monkeypatch.setattr(ingestion, "get_stats_provider", lambda: stats_provider)
    monkeypatch.setattr(ingestion, "get_odds_provider", _OddsProvider)
    monkeypatch.setattr(ingestion, "get_injuries_provider", _InjuriesProvider)
    monkeypatch.setattr(ingestion, "session_scope", _session_scope)
    monkeypatch.setattr(ingestion, "IngestionOrchestrator", _Orchestrator)

    asyncio.run(ingestion.refresh_all(target_date=date(2026, 5, 13)))

    assert _Orchestrator.scopes["reference"] == {"CLE", "DET"}
    assert _Orchestrator.scopes["history"] == set()
    assert _Orchestrator.scopes["logs"] == set()
    assert _Orchestrator.scopes["availability"] == {"CLE", "DET"}
