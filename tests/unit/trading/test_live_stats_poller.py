from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest

from app.providers.stats.balldontlie import BallDontLieStatsProvider
from app.schemas.domain import ProviderFetchResult
from app.trading.live_limits import ACTIVE_TRADER_DEFAULTS
from app.trading.live_stats_poller import (
    LivePlayerStats,
    LiveStatsPoller,
    parse_live_stats,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_LIVE_PAYLOAD: dict[str, Any] = {
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


def test_parse_live_stats_dnp_min_yields_zero() -> None:
    """BDL returns "DNP" / "N/A" for did-not-play rows; must not raise."""
    for dnp_value in ("DNP", "N/A", "DNP-CD"):
        payload: dict[str, Any] = {
            "data": [
                {
                    "player": {"id": 77},
                    "game": {"period": 1, "time": "12:00"},
                    "pts": 0,
                    "reb": 0,
                    "ast": 0,
                    "fg3m": 0,
                    "turnover": 0,
                    "min": dnp_value,
                }
            ]
        }
        result = parse_live_stats(payload)
        assert len(result) == 1, f"expected 1 row for min={dnp_value!r}"
        assert result[0].minutes_played == 0.0, f"expected 0.0 for min={dnp_value!r}"


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


# ---------------------------------------------------------------------------
# LiveStatsPoller
# ---------------------------------------------------------------------------


class _StopAfter:
    """Fake sleep that records durations and stops its poller after n calls."""

    def __init__(self, n: int) -> None:
        self._n = n
        self.sleeps: list[float] = []
        self.poller: LiveStatsPoller | None = None

    async def __call__(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        if len(self.sleeps) >= self._n:
            assert self.poller is not None
            self.poller.stop()


def test_poller_updates_view_from_fetch() -> None:
    async def fetch(ids: list[int]) -> dict[str, Any]:
        return _LIVE_PAYLOAD

    sleeper = _StopAfter(1)
    poller = LiveStatsPoller(
        config=ACTIVE_TRADER_DEFAULTS,
        player_ids_source=lambda: {1, 2, 3},
        fetch=fetch,
        sleep=sleeper,
    )
    sleeper.poller = poller
    asyncio.run(poller.run())

    lebron = poller.stats(1)
    assert lebron is not None
    assert lebron.points == 22
    assert poller.snapshot().keys() == {1, 2, 3}


def test_poller_stops_polling_final_games() -> None:
    calls: list[list[int]] = []

    async def fetch(ids: list[int]) -> dict[str, Any]:
        calls.append(ids)
        return _LIVE_PAYLOAD  # player 3's game is "Final"

    sleeper = _StopAfter(2)
    poller = LiveStatsPoller(
        config=ACTIVE_TRADER_DEFAULTS,
        player_ids_source=lambda: {1, 2, 3},
        fetch=fetch,
        sleep=sleeper,
    )
    sleeper.poller = poller
    asyncio.run(poller.run())

    assert calls[0] == [1, 2, 3]
    assert calls[1] == [1, 2]  # final player dropped from the poll set
    assert poller.stats(3) is not None  # last stats retained in the view


def test_poller_cadence_one_fetch_per_tick() -> None:
    calls: list[list[int]] = []

    async def fetch(ids: list[int]) -> dict[str, Any]:
        calls.append(ids)
        return {"data": []}

    sleeper = _StopAfter(3)
    poller = LiveStatsPoller(
        config=ACTIVE_TRADER_DEFAULTS,
        player_ids_source=lambda: {7},
        fetch=fetch,
        sleep=sleeper,
    )
    sleeper.poller = poller
    asyncio.run(poller.run())

    assert len(calls) == 3
    assert sleeper.sleeps == [ACTIVE_TRADER_DEFAULTS.live_stats_poll_seconds] * 3


def test_poller_empty_watched_set_skips_fetch() -> None:
    calls: list[list[int]] = []

    async def fetch(ids: list[int]) -> dict[str, Any]:
        calls.append(ids)
        return {"data": []}

    poller = LiveStatsPoller(
        config=ACTIVE_TRADER_DEFAULTS,
        player_ids_source=lambda: set(),
        fetch=fetch,
    )
    asyncio.run(poller._poll_once())

    assert calls == []


def test_poller_run_stops_promptly_on_cancel() -> None:
    async def fetch(ids: list[int]) -> dict:
        return {"data": []}

    async def blocking_sleep(seconds: float) -> None:
        await asyncio.Event().wait()  # never returns; only cancellation ends it

    poller = LiveStatsPoller(
        config=ACTIVE_TRADER_DEFAULTS,
        player_ids_source=lambda: {1},
        fetch=fetch,
        sleep=blocking_sleep,
    )

    async def scenario() -> None:
        task = asyncio.create_task(poller.run())
        await asyncio.sleep(0)  # let the loop reach blocking_sleep
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(asyncio.wait_for(scenario(), timeout=2))


# ---------------------------------------------------------------------------
# LiveStatsPoller — pace methods (P2.3)
# ---------------------------------------------------------------------------


def _stat(
    *,
    player_id: int = 1,
    points: int = 0,
    rebounds: int = 0,
    assists: int = 0,
    threes: int = 0,
    turnovers: int = 0,
    minutes_played: float = 0.0,
    period: int = 1,
    clock_seconds_remaining: float = 0.0,
) -> LivePlayerStats:
    return LivePlayerStats(
        player_id=player_id,
        points=points,
        rebounds=rebounds,
        assists=assists,
        threes=threes,
        turnovers=turnovers,
        minutes_played=minutes_played,
        period=period,
        clock_seconds_remaining=clock_seconds_remaining,
        last_updated=datetime.now(UTC),
    )


def _seeded_poller(*stats: LivePlayerStats) -> LiveStatsPoller:
    async def fetch(ids: list[int]) -> dict[str, Any]:
        return {"data": []}

    poller = LiveStatsPoller(
        config=ACTIVE_TRADER_DEFAULTS,
        player_ids_source=set,
        fetch=fetch,
    )
    for stat in stats:
        poller._view[stat.player_id] = stat
    return poller


def test_current_stat_single_field() -> None:
    poller = _seeded_poller(_stat(player_id=1, points=22))
    assert poller.current_stat(1, "points") == 22.0


def test_current_stat_pra_combo() -> None:
    poller = _seeded_poller(_stat(player_id=1, points=10, rebounds=5, assists=4))
    assert poller.current_stat(1, "pra") == 19.0


def test_current_stat_none_when_no_data() -> None:
    poller = _seeded_poller()
    assert poller.current_stat(99, "points") is None


def test_current_stat_none_for_unknown_market() -> None:
    poller = _seeded_poller(_stat(player_id=1, points=22))
    assert poller.current_stat(1, "steals") is None


def test_on_pace_to_hit_true_when_projected_clears() -> None:
    # 22 pts in 28.75 min, 5:30 left in Q3 -> ~17.5 min left, projects ~35 > 25.5
    poller = _seeded_poller(
        _stat(player_id=1, points=22, minutes_played=28.75, period=3, clock_seconds_remaining=330)
    )
    assert poller.on_pace_to_hit(1, "points", 25.5) is True


def test_on_pace_to_hit_false_when_off_pace() -> None:
    # 8 pts in 28.75 min, 2:00 left in Q4 -> ~2 min left, projects ~8.6 < 25.5
    poller = _seeded_poller(
        _stat(player_id=1, points=8, minutes_played=28.75, period=4, clock_seconds_remaining=120)
    )
    assert poller.on_pace_to_hit(1, "points", 25.5) is False


def test_on_pace_to_hit_false_when_no_data() -> None:
    poller = _seeded_poller()
    assert poller.on_pace_to_hit(99, "points", 10.0) is False


def test_on_pace_to_hit_zero_minutes_projects_current_only() -> None:
    # minutes_played=0 -> rate 0 -> projected == current (10)
    poller = _seeded_poller(_stat(player_id=1, points=10, minutes_played=0.0))
    assert poller.on_pace_to_hit(1, "points", 10.0) is True
    assert poller.on_pace_to_hit(1, "points", 10.5) is False


def test_on_pace_to_hit_under_side() -> None:
    # 5 pts in 30 min, 1:00 left in Q4 -> projects ~5.17; under 8 -> True, over 8 -> False
    poller = _seeded_poller(
        _stat(player_id=1, points=5, minutes_played=30.0, period=4, clock_seconds_remaining=60)
    )
    assert poller.on_pace_to_hit(1, "points", 8.0, side="under") is True
    assert poller.on_pace_to_hit(1, "points", 8.0, side="over") is False


def test_on_pace_to_hit_end_of_game_uses_current() -> None:
    # Q4, no time left -> remaining 0 -> projected == current (26) >= 25.5
    poller = _seeded_poller(
        _stat(player_id=1, points=26, minutes_played=40.0, period=4, clock_seconds_remaining=0)
    )
    assert poller.on_pace_to_hit(1, "points", 25.5) is True


def test_is_signaling_entry_true_exactly_at_thresholds() -> None:
    # line 20 * 0.75 = 15.0 stat threshold; remaining 14.4 min = 30% of 48
    # Q3 with 2.4 min left -> 2.4 + 12 = 14.4 remaining
    poller = _seeded_poller(
        _stat(player_id=1, points=15, minutes_played=20.0, period=3, clock_seconds_remaining=144)
    )
    assert poller.is_signaling_entry(1, "points", 20.0) is True


def test_is_signaling_entry_false_below_stat_threshold() -> None:
    # 14 < 15 stat threshold even though plenty of time remains
    poller = _seeded_poller(
        _stat(player_id=1, points=14, minutes_played=20.0, period=2, clock_seconds_remaining=600)
    )
    assert poller.is_signaling_entry(1, "points", 20.0) is False


def test_is_signaling_entry_false_when_little_time_left() -> None:
    # meets stat threshold (15) but only 2 min left -> fraction < 0.30
    poller = _seeded_poller(
        _stat(player_id=1, points=15, minutes_played=30.0, period=4, clock_seconds_remaining=120)
    )
    assert poller.is_signaling_entry(1, "points", 20.0) is False


def test_is_signaling_entry_false_when_no_data() -> None:
    poller = _seeded_poller()
    assert poller.is_signaling_entry(99, "points", 20.0) is False
