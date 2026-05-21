from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from app.providers.stats.balldontlie import BallDontLieStatsProvider
from app.schemas.domain import ProviderFetchResult
from app.trading.live_stats_poller import parse_live_stats

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_LIVE_PAYLOAD: dict = {
    "data": [
        {
            "player": {"id": 1, "first_name": "LeBron", "last_name": "James"},
            "game": {
                "id": 100,
                "status": "3rd Qtr",
                "period": 3,
                "time": "5:30",
                "home_team_id": 14,
                "visitor_team_id": 10,
                "date": "2026-04-09",
            },
            "pts": 22,
            "reb": 7,
            "ast": 5,
            "fg3m": 2,
            "turnover": 1,
            "min": "28:45",
        },
        {
            "player": {"id": 2, "first_name": "Stephen", "last_name": "Curry"},
            "game": {
                "id": 100,
                "status": "3rd Qtr",
                "period": 3,
                "time": "5:30",
                "home_team_id": 14,
                "visitor_team_id": 10,
                "date": "2026-04-09",
            },
            "pts": 18,
            "reb": 3,
            "ast": 6,
            "fg3m": 4,
            "turnover": 0,
            "min": "30:00",
        },
        {
            "player": {"id": 3, "first_name": "Anthony", "last_name": "Davis"},
            "game": {
                "id": 101,
                "status": "Final",
                "period": 4,
                "time": "",
                "home_team_id": 14,
                "visitor_team_id": 10,
                "date": "2026-04-09",
            },
            "pts": 30,
            "reb": 12,
            "ast": 2,
            "fg3m": 1,
            "turnover": 3,
            "min": "36:20",
        },
    ]
}


# ---------------------------------------------------------------------------
# parse_live_stats — field mapping
# ---------------------------------------------------------------------------


def test_parse_live_stats_returns_one_object_per_row() -> None:
    result = parse_live_stats(_LIVE_PAYLOAD)
    assert len(result) == 3


def test_parse_live_stats_player_id_and_basic_stats() -> None:
    result = parse_live_stats(_LIVE_PAYLOAD)
    lebron = result[0]
    assert lebron.player_id == 1
    assert lebron.points == 22
    assert lebron.rebounds == 7
    assert lebron.assists == 5
    assert lebron.threes == 2
    assert lebron.turnovers == 1


def test_parse_live_stats_minutes_played_mm_ss() -> None:
    result = parse_live_stats(_LIVE_PAYLOAD)
    lebron = result[0]
    # "28:45" -> 28 + 45/60
    assert lebron.minutes_played == pytest.approx(28 + 45 / 60, rel=1e-5)


def test_parse_live_stats_minutes_played_exact_minutes() -> None:
    result = parse_live_stats(_LIVE_PAYLOAD)
    curry = result[1]
    # "30:00" -> 30.0
    assert curry.minutes_played == pytest.approx(30.0, rel=1e-5)


def test_parse_live_stats_period_from_game() -> None:
    result = parse_live_stats(_LIVE_PAYLOAD)
    assert result[0].period == 3
    assert result[1].period == 3
    assert result[2].period == 4


def test_parse_live_stats_clock_seconds_remaining() -> None:
    result = parse_live_stats(_LIVE_PAYLOAD)
    # "5:30" -> 5*60 + 30 = 330
    assert result[0].clock_seconds_remaining == pytest.approx(330.0)
    assert result[1].clock_seconds_remaining == pytest.approx(330.0)


def test_parse_live_stats_final_game_clock_zero() -> None:
    result = parse_live_stats(_LIVE_PAYLOAD)
    ad = result[2]
    # time="" -> 0.0
    assert ad.clock_seconds_remaining == 0.0


def test_parse_live_stats_last_updated_is_utc_datetime() -> None:
    before = datetime.now(UTC)
    result = parse_live_stats(_LIVE_PAYLOAD)
    after = datetime.now(UTC)
    for stat in result:
        assert before <= stat.last_updated <= after
        assert stat.last_updated.tzinfo is UTC


def test_parse_live_stats_frozen_dataclass() -> None:
    result = parse_live_stats(_LIVE_PAYLOAD)
    with pytest.raises((AttributeError, TypeError)):
        result[0].points = 99  # type: ignore[misc]


# ---------------------------------------------------------------------------
# parse_live_stats — tolerance
# ---------------------------------------------------------------------------


def test_parse_live_stats_missing_min_yields_zero() -> None:
    payload = {
        "data": [
            {
                "player": {"id": 10},
                "game": {"period": 2, "time": "3:00"},
                "pts": 5,
                "reb": 2,
                "ast": 1,
                "fg3m": 0,
                "turnover": 0,
                # no "min" key
            }
        ]
    }
    result = parse_live_stats(payload)
    assert len(result) == 1
    assert result[0].minutes_played == 0.0


def test_parse_live_stats_missing_time_yields_zero() -> None:
    payload = {
        "data": [
            {
                "player": {"id": 10},
                "game": {"period": 1},  # no "time" key
                "pts": 5,
                "reb": 2,
                "ast": 1,
                "fg3m": 0,
                "turnover": 0,
                "min": "10:00",
            }
        ]
    }
    result = parse_live_stats(payload)
    assert result[0].clock_seconds_remaining == 0.0


def test_parse_live_stats_row_missing_player_id_is_skipped() -> None:
    payload = {
        "data": [
            {
                "player": {"first_name": "Ghost"},  # no "id"
                "game": {"period": 1, "time": "5:00"},
                "pts": 5,
                "reb": 0,
                "ast": 0,
                "fg3m": 0,
                "turnover": 0,
                "min": "5:00",
            },
            {
                "player": {"id": 99},
                "game": {"period": 1, "time": "5:00"},
                "pts": 10,
                "reb": 0,
                "ast": 0,
                "fg3m": 0,
                "turnover": 0,
                "min": "5:00",
            },
        ]
    }
    result = parse_live_stats(payload)
    assert len(result) == 1
    assert result[0].player_id == 99


def test_parse_live_stats_empty_data() -> None:
    assert parse_live_stats({"data": []}) == []
    assert parse_live_stats({}) == []


# ---------------------------------------------------------------------------
# BallDontLieStatsProvider.fetch_live_player_stats
# ---------------------------------------------------------------------------


def _make_result(payload: dict) -> ProviderFetchResult:
    return ProviderFetchResult(
        endpoint="unit://bdl",
        fetched_at=datetime.now(UTC),
        payload=payload,
    )


def test_fetch_live_player_stats_builds_correct_params(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = BallDontLieStatsProvider()
    captured: dict = {}

    async def fake_get_paginated(endpoint: str, params: dict | None = None) -> ProviderFetchResult:
        captured["endpoint"] = endpoint
        captured["params"] = params
        return _make_result({"data": [], "meta": {}})

    monkeypatch.setattr(provider, "_get_paginated", fake_get_paginated)

    asyncio.run(provider.fetch_live_player_stats(player_ids=[3, 1, 2], season=2025))

    assert captured["endpoint"] == "/stats"
    params = captured["params"]
    assert params is not None
    # player_ids should be sorted
    assert params["player_ids[]"] == [1, 2, 3]
    assert params["seasons[]"] == [2025]
    assert params["per_page"] == 100


def test_fetch_live_player_stats_returns_provider_fetch_result(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = BallDontLieStatsProvider()
    fake = _make_result({"data": [{"player": {"id": 42}}], "meta": {}})

    async def fake_get_paginated(endpoint: str, params: dict | None = None) -> ProviderFetchResult:
        return fake

    monkeypatch.setattr(provider, "_get_paginated", fake_get_paginated)

    result = asyncio.run(provider.fetch_live_player_stats(player_ids=[42], season=2025))

    assert isinstance(result, ProviderFetchResult)
    assert result is fake
