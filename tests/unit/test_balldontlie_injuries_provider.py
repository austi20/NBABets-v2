from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

from app.providers.injuries.balldontlie import BallDontLieInjuriesProvider
from app.schemas.domain import ProviderFetchResult


def _result(payload: dict) -> ProviderFetchResult:
    return ProviderFetchResult(
        endpoint="unit://balldontlie",
        fetched_at=datetime.now(UTC),
        payload=payload,
    )


def test_balldontlie_injuries_maps_team_and_game_ids(monkeypatch) -> None:
    provider = BallDontLieInjuriesProvider()

    async def fake_get_paginated(endpoint: str, params: dict | None = None) -> ProviderFetchResult:
        del params
        if endpoint == "/teams":
            return _result(
                {
                    "data": [
                        {"id": 10, "abbreviation": "GSW"},
                        {"id": 14, "abbreviation": "LAL"},
                    ],
                    "meta": {"record_count": 2},
                }
            )
        if endpoint == "/games":
            return _result(
                {
                    "data": [
                        {
                            "id": 18447983,
                            "home_team": {"id": 10},
                            "visitor_team": {"id": 14},
                        }
                    ],
                    "meta": {"record_count": 1},
                }
            )
        if endpoint == "/player_injuries":
            return _result(
                {
                    "data": [
                        {
                            "player": {"id": 115, "team_id": 10},
                            "status": "Out",
                            "description": "Stephen Curry (ankle) is out for tonight's game.",
                        }
                    ],
                    "meta": {"record_count": 1},
                }
            )
        raise AssertionError(f"Unexpected endpoint: {endpoint}")

    monkeypatch.setattr(provider, "_get_paginated", fake_get_paginated)

    result, injuries = asyncio.run(provider.fetch_injuries(date(2026, 4, 9)))

    assert result.payload["meta"]["record_count"] == 1
    assert len(injuries) == 1
    assert injuries[0].provider_player_id == "115"
    assert injuries[0].team_abbreviation == "GSW"
    assert injuries[0].provider_game_id == "18447983"
    assert injuries[0].status == "Out"
    assert injuries[0].body_part == "ankle"
    assert injuries[0].expected_availability_flag is False
