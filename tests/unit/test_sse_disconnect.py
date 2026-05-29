from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.server.routers import local_agent as local_agent_router
from app.server.routers import trading as trading_router


def _disconnected_request() -> MagicMock:
    request = MagicMock()
    request.is_disconnected = AsyncMock(return_value=True)
    return request


@pytest.mark.asyncio
async def test_trading_stream_stops_on_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    # Snapshot builder should never run once the client is gone.
    build_spy = MagicMock(side_effect=AssertionError("snapshot built after disconnect"))
    monkeypatch.setattr(trading_router, "_build_live_snapshot", build_spy)

    request = _disconnected_request()
    service = MagicMock()
    service.publisher.wait_for_update = AsyncMock(
        side_effect=AssertionError("waited for update after disconnect")
    )
    monkeypatch.setattr(trading_router, "_snapshot_service", lambda _req: service)

    response = await trading_router.trading_stream(request)
    generator = response.body_iterator

    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()

    request.is_disconnected.assert_awaited()
    build_spy.assert_not_called()


@pytest.mark.asyncio
async def test_local_agent_stream_stops_on_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    # Terminal text builder and sleep should never run once the client is gone.
    text_spy = MagicMock(side_effect=AssertionError("terminal text built after disconnect"))
    monkeypatch.setattr(local_agent_router, "build_local_ai_terminal_text", text_spy)
    monkeypatch.setattr(
        local_agent_router.asyncio,
        "sleep",
        AsyncMock(side_effect=AssertionError("slept after disconnect")),
    )

    request = _disconnected_request()

    response = await local_agent_router.local_agent_terminal_stream(request)
    generator = response.body_iterator

    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()

    request.is_disconnected.assert_awaited()
    text_spy.assert_not_called()
