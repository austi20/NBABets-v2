from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

from app.providers.stats.nba_api import NbaApiStatsProvider, _nba_api_retry_after_seconds


def test_retry_after_seconds_parses_header() -> None:
    exc = Exception("429 Too Many Requests")
    exc.response = SimpleNamespace(headers={"Retry-After": "2.5"})  # type: ignore[attr-defined]
    assert _nba_api_retry_after_seconds(exc) == 2.5


def test_retry_call_retries_on_rate_limit(monkeypatch) -> None:
    provider = NbaApiStatsProvider()
    provider._request_delay_seconds = 0.0
    provider._retry_max_backoff_seconds = 10.0

    sleep_calls: list[float] = []
    monkeypatch.setattr("app.providers.stats.nba_api.time_module.sleep", lambda seconds: sleep_calls.append(float(seconds)))

    attempts = {"count": 0}

    class _RateLimitError(Exception):
        pass

    def _builder(_timeout: int):
        attempts["count"] += 1
        if attempts["count"] == 1:
            error = _RateLimitError("429 Too Many Requests")
            error.response = SimpleNamespace(headers={"Retry-After": "1"})  # type: ignore[attr-defined]
            raise error
        return "ok"

    result = provider._retry_nba_api_call("TestEndpoint", _builder, retries=3)

    assert result == "ok"
    assert attempts["count"] == 2
    assert sleep_calls == [1.0]


def test_fetch_game_availability_does_not_raise_name_error(monkeypatch) -> None:
    provider = NbaApiStatsProvider()
    provider._request_delay_seconds = 0.0

    monkeypatch.setattr(
        provider,
        "_availability_rows_for_game",
        lambda _game_id: [
            {"PLAYER_ID": "1", "FIRST_NAME": "A", "LAST_NAME": "B", "TEAM_ID": 1610612737, "JERSEY_NUM": "7"}
        ],
    )

    result, payloads = asyncio.run(provider.fetch_game_availability(["0022400001"]))

    assert result.fetched_at <= datetime.now(UTC)
    assert len(payloads) == 1
    assert payloads[0].provider_player_id == "1"

