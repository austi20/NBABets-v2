from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta

from app.providers.stats.balldontlie import BallDontLieStatsProvider
from app.schemas.domain import ProviderFetchResult


def _result(payload: dict) -> ProviderFetchResult:
    return ProviderFetchResult(
        endpoint="unit://balldontlie",
        fetched_at=datetime.now(UTC),
        payload=payload,
    )


def _games_payload(game_id: int, home_id: int, away_id: int, game_date: date) -> dict:
    return {
        "data": [
            {
                "id": game_id,
                "date": game_date.isoformat(),
                "datetime": f"{game_date.isoformat()}T02:00:00.000Z",
                "season": 2025,
                "status": "Final",
                "home_team": {"id": home_id, "abbreviation": "LAL"},
                "visitor_team": {"id": away_id, "abbreviation": "GSW"},
            }
        ],
        "meta": {"record_count": 1},
    }


def test_player_game_logs_scopes_to_own_range() -> None:
    """Each fetch_player_game_logs call must scope the /stats query to its own
    range's game ids, never a union leaked from a prior call."""
    provider = BallDontLieStatsProvider()

    today = date.today()
    range_a = (today, today)
    range_b = (today - timedelta(days=1), today)

    # Distinct /games payloads keyed by the date range being requested.
    games_by_first_date = {
        range_a[0].isoformat(): _games_payload(1001, 14, 10, range_a[0]),
        range_b[0].isoformat(): _games_payload(2002, 14, 10, range_b[0]),
    }

    stats_calls: list[dict] = []

    async def fake_get_paginated(endpoint: str, params: dict | None = None) -> ProviderFetchResult:
        params = params or {}
        if endpoint == "/games":
            first_date = params["dates[]"][0]
            return _result(games_by_first_date[first_date])
        if endpoint == "/stats":
            stats_calls.append(params)
            return _result({"data": [], "meta": {"record_count": 0}})
        raise AssertionError(f"Unexpected endpoint: {endpoint}")

    provider._get_paginated = fake_get_paginated  # type: ignore[assignment]

    asyncio.run(provider.fetch_player_game_logs(*range_a))
    asyncio.run(provider.fetch_player_game_logs(*range_b))

    assert len(stats_calls) == 2
    # First call scoped to range A's game id only.
    assert stats_calls[0]["game_ids[]"] == [1001]
    # Second call scoped to range B's game id only -- no leaked union with 1001.
    assert stats_calls[1]["game_ids[]"] == [2002]


def test_no_shared_instance_scope_sets() -> None:
    """The leaky instance-level scope sets must be gone entirely."""
    provider = BallDontLieStatsProvider()
    assert not hasattr(provider, "_relevant_game_ids")
    assert not hasattr(provider, "_scheduled_team_ids")
