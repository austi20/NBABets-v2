from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

from app.providers._utils import safe_int
from app.providers.stats.balldontlie import BallDontLieStatsProvider
from app.schemas.domain import ProviderFetchResult


def _result(payload: dict) -> ProviderFetchResult:
    return ProviderFetchResult(
        endpoint="unit://balldontlie",
        fetched_at=datetime.now(UTC),
        payload=payload,
    )


def test_safe_int_valid() -> None:
    assert safe_int("42") == 42


def test_safe_int_invalid_string() -> None:
    assert safe_int("not-a-number") is None


def test_safe_int_none() -> None:
    assert safe_int(None) is None


def test_bdl_stats_skips_bad_id_row() -> None:
    provider = BallDontLieStatsProvider()
    payload = _result(
        {
            "data": [
                {"id": 14, "abbreviation": "LAL", "full_name": "Lakers", "city": "Los Angeles"},
                {"id": "bad", "abbreviation": "GSW", "full_name": "Warriors", "city": "San Francisco"},
            ],
            "meta": {"record_count": 2},
        }
    )
    provider._get_paginated = AsyncMock(return_value=payload)  # type: ignore[method-assign]

    _, teams = asyncio.run(provider.fetch_teams())

    assert provider._team_lookup == {14: "LAL"}
    assert all(team.abbreviation in {"LAL", "GSW"} for team in teams)
