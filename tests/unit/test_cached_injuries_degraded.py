from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.providers.cached import CachedInjuriesProvider
from app.schemas.domain import InjuryPayload, ProviderFetchResult


def _make_provider(result: ProviderFetchResult, injuries: list[InjuryPayload]) -> CachedInjuriesProvider:
    inner = MagicMock()
    inner.provider_name = "balldontlie_injuries"
    inner.fetch_injuries = AsyncMock(return_value=(result, injuries))

    provider = CachedInjuriesProvider.__new__(CachedInjuriesProvider)
    provider._provider = inner
    provider._cache = MagicMock()
    provider._cache_provider_name = "balldontlie_injuries"
    provider._cache_provider_aliases = ["balldontlie_injuries"]
    provider.provider_name = "balldontlie_injuries"

    provider._cache.get_collection_with_metadata.return_value = None
    provider._cache.is_collection_usable.return_value = False
    return provider


@pytest.mark.asyncio
async def test_degraded_injuries_not_cached() -> None:
    result = ProviderFetchResult(
        endpoint="https://api.balldontlie.io/player_injuries",
        fetched_at=datetime.now(UTC),
        payload={"data": [], "meta": {"record_count": 0, "reason": "endpoint_unavailable_403"}},
    )
    provider = _make_provider(result, [])

    out_result, out_items = await provider.fetch_injuries()

    provider._cache.put_collection.assert_not_called()
    assert out_result is result
    assert out_items == []


@pytest.mark.asyncio
async def test_healthy_injuries_cached() -> None:
    injury = InjuryPayload(
        provider_player_id="123",
        team_abbreviation="BOS",
        report_timestamp=datetime.now(UTC),
        status="out",
    )
    result = ProviderFetchResult(
        endpoint="https://api.balldontlie.io/player_injuries",
        fetched_at=datetime.now(UTC),
        payload={"data": [{"id": 1}], "meta": {"record_count": 1}},
    )
    provider = _make_provider(result, [injury])

    await provider.fetch_injuries()

    provider._cache.put_collection.assert_called_once()
