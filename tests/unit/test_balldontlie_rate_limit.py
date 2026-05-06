from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from app.config.settings import get_settings
from app.providers.http import HttpProviderMixin
from app.providers.stats.balldontlie import BallDontLieStatsProvider
from app.schemas.domain import ProviderFetchResult


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> None:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_balldontlie_throttle_sleeps_when_window_full(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BALLDONTLIE_MAX_RPM", "2")
    get_settings.cache_clear()

    clock = [0.0]
    sleeps: list[float] = []

    monkeypatch.setattr("app.providers.stats.balldontlie.monotonic", lambda: clock[0])

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(float(seconds))
        clock[0] += float(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async def fake_get(
        self: object,
        endpoint: str,
        headers: dict[str, str] | None = None,
        params: dict[str, object] | None = None,
    ) -> ProviderFetchResult:
        return ProviderFetchResult(
            endpoint=f"https://example.com{endpoint}",
            fetched_at=datetime.now(UTC),
            payload={},
        )

    monkeypatch.setattr(HttpProviderMixin, "_get", fake_get)

    async def _run() -> None:
        provider = BallDontLieStatsProvider()
        await provider._get("/a")
        await provider._get("/b")
        await provider._get("/c")

    asyncio.run(_run())

    assert len(sleeps) == 1
    assert abs(sleeps[0] - 60.0) < 0.001


def test_balldontlie_throttle_off_when_rpm_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BALLDONTLIE_MAX_RPM", "0")
    get_settings.cache_clear()

    async def fake_get(
        self: object,
        endpoint: str,
        headers: dict[str, str] | None = None,
        params: dict[str, object] | None = None,
    ) -> ProviderFetchResult:
        return ProviderFetchResult(
            endpoint=f"https://example.com{endpoint}",
            fetched_at=datetime.now(UTC),
            payload={},
        )

    monkeypatch.setattr(HttpProviderMixin, "_get", fake_get)

    sleeps: list[float] = []
    monkeypatch.setattr(asyncio, "sleep", lambda s: sleeps.append(float(s)))

    async def _run() -> None:
        provider = BallDontLieStatsProvider()
        for _ in range(5):
            await provider._get("/x")

    asyncio.run(_run())

    assert sleeps == []
