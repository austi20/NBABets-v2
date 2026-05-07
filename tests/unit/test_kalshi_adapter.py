from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from app.trading.kalshi_adapter import KalshiAdapter

from app.trading.symbol_resolver import SymbolResolver
from app.trading.types import ExecutionIntent, MarketRef, Signal


class _FakeClient:
    def __init__(
        self,
        *,
        market: dict[str, Any] | None = None,
        order: dict[str, Any] | None = None,
        order_polls: list[dict[str, Any]] | None = None,
    ) -> None:
        self._market = market or {"market": {"yes_ask": 40, "yes_bid": 38, "status": "open"}}
        self._order = order or {"order": {"order_id": "ord1", "status": "executed"}}
        self._polls = list(order_polls or [{"order": {"order_id": "ord1", "status": "executed", "fills": [
            {"trade_id": "t1", "count": 1, "yes_price": 40, "fee": 1}
        ]}}])
        self.create_calls: list[dict[str, Any]] = []
        self.poll_calls: list[str] = []

    def get_market(self, ticker: str) -> dict[str, Any]:
        return self._market

    def create_order(self, **kwargs: Any) -> dict[str, Any]:
        self.create_calls.append(kwargs)
        return self._order

    def get_order(self, order_id: str) -> dict[str, Any]:
        self.poll_calls.append(order_id)
        if self._polls:
            return self._polls.pop(0)
        return {"order": {"order_id": order_id, "status": "executed", "fills": []}}


def _resolver_with(ticker: str) -> SymbolResolver:
    return SymbolResolver(entries=[{
        "market_key": "points", "side": "over", "line_value": 25.5,
        "player_id": 237, "game_date": "2026-05-06", "kalshi_ticker": ticker,
    }])


def _intent(stake: float = 0.50, player_id: int = 237) -> ExecutionIntent:
    signal = Signal(
        signal_id="s1",
        created_at=datetime(2026, 5, 6, tzinfo=UTC),
        market_key="points",
        side="OVER",
        confidence="high",
        edge=0.05,
        model_probability=0.55,
        line_value=25.5,
        metadata={"player_id": player_id, "game_id": 1, "game_date": "2026-05-06"},
    )
    return ExecutionIntent(
        intent_id="intent-1",
        signal=signal,
        market=MarketRef(exchange="kalshi", symbol="kalshi:x", market_key="points", side="OVER", line_value=25.5),
        side="buy",
        stake=stake,
    )


def test_place_order_resolves_ticker_and_fires_create() -> None:
    client = _FakeClient()
    adapter = KalshiAdapter(client=client, resolver=_resolver_with("KX-T1"))
    events, fills = adapter.place_order(_intent(stake=0.50))
    assert client.create_calls[0]["ticker"] == "KX-T1"
    assert client.create_calls[0]["count"] == 1  # 0.50 / 0.40 = 1 contract
    assert client.create_calls[0]["client_order_id"].startswith("intent-1")
    assert any(e.event_type == "filled" for e in events)
    assert len(fills) == 1
    assert fills[0].price == pytest.approx(0.40)


def test_place_order_unresolved_ticker_emits_rejected_no_call() -> None:
    client = _FakeClient()
    adapter = KalshiAdapter(client=client, resolver=SymbolResolver(entries=[]))
    events, fills = adapter.place_order(_intent())
    assert client.create_calls == []
    assert any(e.event_type == "rejected" for e in events)
    assert fills == []


def test_place_order_count_zero_when_stake_below_contract_price_emits_rejected() -> None:
    # contract is 40 cents, stake is 25 cents -> count = 0
    client = _FakeClient()
    adapter = KalshiAdapter(client=client, resolver=_resolver_with("KX-T1"))
    events, fills = adapter.place_order(_intent(stake=0.25))
    assert client.create_calls == []
    assert any(e.event_type == "rejected" and "exceeds" in e.message for e in events)
    assert fills == []


def test_place_order_handles_no_fills_in_response() -> None:
    client = _FakeClient(order_polls=[{"order": {"order_id": "ord1", "status": "canceled", "fills": []}}])
    adapter = KalshiAdapter(client=client, resolver=_resolver_with("KX-T1"), poll_interval_seconds=0.0, poll_timeout_seconds=0.1)
    events, fills = adapter.place_order(_intent(stake=0.50))
    assert fills == []
    assert any(e.status == "canceled" or e.event_type == "filled" for e in events) or any(e.event_type == "error" for e in events)
