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
        fills: dict[str, Any] | None = None,
    ) -> None:
        self._market = market or {
            "market": {
                "yes_ask_dollars": "0.4000",
                "yes_bid_dollars": "0.3800",
                "no_ask_dollars": "0.6200",
                "status": "open",
            }
        }
        self._order = order or {"order_id": "ord1", "fill_count": "1.00", "remaining_count": "0.00"}
        self._polls = list(order_polls or [{"order": {"order_id": "ord1", "status": "executed"}}])
        self._fills = fills or {
            "fills": [
                {
                    "fill_id": "f1",
                    "trade_id": "t1",
                    "order_id": "ord1",
                    "count_fp": "1.00",
                    "yes_price_dollars": "0.4000",
                    "fee_cost": "0.0100",
                }
            ],
            "cursor": "",
        }
        self.create_calls: list[dict[str, Any]] = []
        self.poll_calls: list[str] = []
        self.fill_calls: list[dict[str, Any]] = []

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

    def get_fills(self, **kwargs: Any) -> dict[str, Any]:
        self.fill_calls.append(kwargs)
        return self._fills


def _resolver_with(ticker: str, side: str = "over") -> SymbolResolver:
    return SymbolResolver(entries=[{
        "market_key": "points", "side": side, "line_value": 25.5,
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


def _intent_with_metadata(metadata: dict[str, Any], stake: float = 0.50) -> ExecutionIntent:
    intent = _intent(stake=stake)
    signal = Signal(
        **{
            **intent.signal.__dict__,
            "metadata": {**intent.signal.metadata, **metadata},
        }
    )
    return ExecutionIntent(
        intent_id=intent.intent_id,
        signal=signal,
        market=intent.market,
        side=intent.side,
        stake=intent.stake,
    )


def test_place_order_resolves_ticker_and_fires_create() -> None:
    client = _FakeClient()
    adapter = KalshiAdapter(client=client, resolver=_resolver_with("KX-T1"))
    events, fills = adapter.place_order(_intent(stake=0.50))
    assert client.create_calls[0]["ticker"] == "KX-T1"
    assert client.create_calls[0]["side"] == "bid"
    assert client.create_calls[0]["price_dollars"] == pytest.approx(0.40)
    assert client.create_calls[0]["count"] == 1  # 0.50 / 0.40 = 1 contract
    assert client.create_calls[0]["client_order_id"].startswith("intent-1")
    assert client.fill_calls[0]["order_id"] == "ord1"
    assert any(e.event_type == "filled" for e in events)
    assert len(fills) == 1
    assert fills[0].price == pytest.approx(0.40)
    assert fills[0].market.symbol == _intent().market.symbol
    assert fills[0].exchange_order_id == "ord1"


def test_place_order_honors_decision_execution_metadata() -> None:
    client = _FakeClient()
    adapter = KalshiAdapter(client=client, resolver=_resolver_with("KX-T1"))
    events, fills = adapter.place_order(
        _intent_with_metadata(
            {
                "max_price_dollars": "0.6200",
                "post_only": True,
                "time_in_force": "good_till_canceled",
            }
        )
    )
    assert client.create_calls[0]["post_only"] is True
    assert client.create_calls[0]["time_in_force"] == "good_till_canceled"
    assert any(e.status == "filled" for e in events)
    assert len(fills) == 1


def test_place_order_rejects_above_decision_max_price() -> None:
    client = _FakeClient()
    adapter = KalshiAdapter(client=client, resolver=_resolver_with("KX-T1"))
    events, fills = adapter.place_order(_intent_with_metadata({"max_price_dollars": "0.3900"}))
    assert client.create_calls == []
    assert fills == []
    assert events[0].status == "blocked"
    assert "exceeds max price" in events[0].message


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
    client = _FakeClient(
        order_polls=[{"order": {"order_id": "ord1", "status": "canceled"}}],
        fills={"fills": [], "cursor": ""},
    )
    adapter = KalshiAdapter(client=client, resolver=_resolver_with("KX-T1"), poll_interval_seconds=0.0, poll_timeout_seconds=0.1)
    events, fills = adapter.place_order(_intent(stake=0.50))
    assert fills == []
    assert any(e.status == "canceled" or e.event_type == "filled" for e in events) or any(e.event_type == "error" for e in events)


def test_place_order_under_uses_v2_ask_and_no_price() -> None:
    client = _FakeClient(
        fills={
            "fills": [
                {
                    "fill_id": "f1",
                    "trade_id": "t1",
                    "order_id": "ord1",
                    "count_fp": "1.00",
                    "no_price_dollars": "0.6200",
                    "fee_cost": "0.0000",
                }
            ],
            "cursor": "",
        }
    )
    adapter = KalshiAdapter(client=client, resolver=_resolver_with("KX-T1", side="under"))
    intent = _intent(stake=0.70)
    under_signal = Signal(
        **{
            **intent.signal.__dict__,
            "side": "UNDER",
            "metadata": {"player_id": 237, "game_id": 1, "game_date": "2026-05-06"},
        }
    )
    under_intent = ExecutionIntent(
        intent_id="intent-2",
        signal=under_signal,
        market=MarketRef(
            exchange="kalshi",
            symbol="kalshi:points:under:25.5:g1:p237",
            market_key="points",
            side="UNDER",
            line_value=25.5,
        ),
        side="buy",
        stake=0.70,
    )
    events, fills = adapter.place_order(under_intent)
    assert client.create_calls[0]["side"] == "ask"
    assert client.create_calls[0]["price_dollars"] == pytest.approx(0.38)
    assert any(event.status == "filled" for event in events)
    assert fills[0].price == pytest.approx(0.62)
    assert fills[0].market.symbol.endswith(":under:25.5:g1:p237")
