from __future__ import annotations

from datetime import UTC, date, datetime

import httpx
import pytest

from app.providers.base import OddsProvider
from app.providers.odds import fallback as fallback_module
from app.providers.odds.fallback import OddsWithKalshiFallback
from app.schemas.domain import ProviderFetchResult

TARGET = date(2026, 10, 21)


def _result(name: str) -> ProviderFetchResult:
    return ProviderFetchResult(endpoint=f"https://{name}", fetched_at=datetime.now(UTC), payload={"data": []})


class _Fake(OddsProvider):
    def __init__(self, name: str, error: BaseException | None = None) -> None:
        self.provider_name = name
        self.error = error
        self.calls = 0

    async def healthcheck(self) -> bool:
        return True

    async def fetch_upcoming_player_props(self, target_date):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return _result(self.provider_name), []


def _status_error(code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://api.balldontlie.io/v2/odds/player_props")
    return httpx.HTTPStatusError("denied", request=request, response=httpx.Response(code, request=request))


@pytest.fixture(autouse=True)
def _reset_denial():
    fallback_module._denied_until = 0.0
    yield
    fallback_module._denied_until = 0.0


@pytest.mark.asyncio
async def test_primary_used_when_it_works():
    primary, backup = _Fake("balldontlie"), _Fake("kalshi")
    provider = OddsWithKalshiFallback(primary, backup)

    result, _ = await provider.fetch_upcoming_player_props(TARGET)

    assert result.endpoint == "https://balldontlie"
    assert backup.calls == 0
    assert provider.provider_name == "balldontlie"


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [401, 403])
async def test_access_denied_falls_back_and_relabels(code):
    primary, backup = _Fake("balldontlie", _status_error(code)), _Fake("kalshi")
    provider = OddsWithKalshiFallback(primary, backup)

    result, _ = await provider.fetch_upcoming_player_props(TARGET)

    assert result.endpoint == "https://kalshi"
    assert provider.provider_name == "kalshi"


@pytest.mark.asyncio
async def test_denial_is_remembered_so_primary_is_not_retried():
    primary, backup = _Fake("balldontlie", _status_error(401)), _Fake("kalshi")
    provider = OddsWithKalshiFallback(primary, backup)

    await provider.fetch_upcoming_player_props(TARGET)
    await provider.fetch_upcoming_player_props(TARGET)

    assert primary.calls == 1
    assert backup.calls == 2


@pytest.mark.asyncio
async def test_circuit_open_after_denials_also_falls_back():
    primary = _Fake("balldontlie", RuntimeError("Circuit open for endpoint=/v2/odds/player_props"))
    provider = OddsWithKalshiFallback(primary, _Fake("kalshi"))

    result, _ = await provider.fetch_upcoming_player_props(TARGET)

    assert result.endpoint == "https://kalshi"


@pytest.mark.asyncio
async def test_other_errors_are_not_swallowed():
    provider = OddsWithKalshiFallback(_Fake("balldontlie", _status_error(500)), _Fake("kalshi"))

    with pytest.raises(httpx.HTTPStatusError):
        await provider.fetch_upcoming_player_props(TARGET)
