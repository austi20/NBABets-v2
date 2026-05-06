from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

from app.providers.stats.balldontlie import BallDontLieStatsProvider
from app.schemas.domain import ProviderFetchResult


def _result(payload: dict) -> ProviderFetchResult:
    return ProviderFetchResult(
        endpoint="unit://balldontlie",
        fetched_at=datetime.now(UTC),
        payload=payload,
    )


def test_fetch_schedule_uses_datetime_for_start_time(monkeypatch) -> None:
    provider = BallDontLieStatsProvider()

    async def fake_get_paginated(endpoint: str, params: dict | None = None) -> ProviderFetchResult:
        del params
        if endpoint != "/games":
            raise AssertionError(f"Unexpected endpoint: {endpoint}")
        return _result(
            {
                "data": [
                    {
                        "id": 18447983,
                        "date": "2026-04-09",
                        "datetime": "2026-04-10T02:00:00.000Z",
                        "season": 2025,
                        "status": "2026-04-10T02:00:00Z",
                        "home_team": {"id": 14, "abbreviation": "LAL"},
                        "visitor_team": {"id": 10, "abbreviation": "GSW"},
                    }
                ],
                "meta": {"record_count": 1},
            }
        )

    monkeypatch.setattr(provider, "_get_paginated", fake_get_paginated)

    _, games = asyncio.run(provider.fetch_schedule(date(2026, 4, 9)))

    assert len(games) == 1
    game = games[0]
    assert game.game_date == date(2026, 4, 9)
    assert game.start_time == datetime(2026, 4, 10, 2, 0, tzinfo=UTC)
    assert game.status == "scheduled"
