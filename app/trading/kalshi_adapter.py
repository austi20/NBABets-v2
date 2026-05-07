from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from app.providers.exchanges.kalshi_errors import KalshiApiError
from app.trading.symbol_resolver import SymbolResolver
from app.trading.types import ExecutionIntent, Fill, OrderEvent

_TERMINAL_STATUSES = {"executed", "filled", "canceled", "cancelled", "rejected"}


class _KalshiClientLike(Protocol):
    def get_market(self, ticker: str) -> dict[str, Any]: ...
    def create_order(
        self,
        *,
        ticker: str,
        side: str,
        count: int | float | Decimal | str,
        price_dollars: int | float | Decimal | str,
        client_order_id: str,
        time_in_force: str = "fill_or_kill",
        self_trade_prevention_type: str = "taker_at_cross",
    ) -> dict[str, Any]: ...
    def get_order(self, order_id: str) -> dict[str, Any]: ...
    def get_fills(
        self,
        *,
        order_id: str | None = None,
        ticker: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class _Quote:
    order_side: str
    order_price_dollars: float
    contract_cost_dollars: float


class KalshiAdapter:
    def __init__(
        self,
        *,
        client: _KalshiClientLike,
        resolver: SymbolResolver,
        poll_interval_seconds: float = 0.25,
        poll_timeout_seconds: float = 5.0,
    ) -> None:
        self._client = client
        self._resolver = resolver
        self._poll_interval = float(poll_interval_seconds)
        self._poll_timeout = float(poll_timeout_seconds)

    def place_order(self, intent: ExecutionIntent) -> tuple[list[OrderEvent], list[Fill]]:
        ticker = self._resolver.resolve(intent)
        if ticker is None:
            return (
                [OrderEvent(intent_id=intent.intent_id, event_type="rejected",
                            status="blocked", message="no kalshi ticker for signal",
                            market=intent.market, side=intent.side, stake=intent.stake)],
                [],
            )
        try:
            market = self._client.get_market(ticker)
        except KalshiApiError as exc:
            return (
                [OrderEvent(intent_id=intent.intent_id, event_type="error",
                            status="failed", message=f"market lookup failed: {exc}",
                            market=intent.market, side=intent.side, stake=intent.stake)],
                [],
            )
        quote = self._quote_for_signal(intent.signal.side, market)
        if quote is None or quote.contract_cost_dollars <= 0:
            return (
                [OrderEvent(intent_id=intent.intent_id, event_type="rejected",
                            status="blocked", message="market has no tradable ask",
                            market=intent.market, side=intent.side, stake=intent.stake)],
                [],
            )
        count = int(intent.stake // quote.contract_cost_dollars)
        if count < 1:
            return (
                [OrderEvent(
                    intent_id=intent.intent_id,
                    event_type="rejected",
                    status="blocked",
                    message=(
                        f"contract price {quote.contract_cost_dollars:.2f} "
                        f"exceeds stake cap {intent.stake:.2f}"
                    ),
                    market=intent.market,
                    side=intent.side,
                    stake=intent.stake,
                )],
                [],
            )
        client_order_id = f"{intent.intent_id}-1"
        try:
            order_response = self._client.create_order(
                ticker=ticker,
                side=quote.order_side,
                count=count,
                price_dollars=quote.order_price_dollars,
                client_order_id=client_order_id,
            )
        except KalshiApiError as exc:
            return (
                [OrderEvent(intent_id=intent.intent_id, event_type="error",
                            status="failed", message=f"create_order failed: {exc}",
                            market=intent.market, side=intent.side, stake=intent.stake)],
                [],
            )
        order_id = order_response.get("order_id") or order_response.get("order", {}).get("order_id")
        events: list[OrderEvent] = [
            OrderEvent(intent_id=intent.intent_id, event_type="accepted",
                       status="ok", message=f"kalshi order {order_id}",
                       market=intent.market, side=intent.side, stake=intent.stake,
                       exchange_order_id=str(order_id) if order_id else None),
        ]
        terminal = self._poll_until_terminal(order_id) if order_id else None
        if terminal is None:
            events.append(OrderEvent(intent_id=intent.intent_id, event_type="error",
                                     status="failed", message="poll timeout, fill unknown",
                                     market=intent.market, side=intent.side, stake=intent.stake,
                                     exchange_order_id=str(order_id) if order_id else None))
            return events, []
        status = str(terminal.get("order", {}).get("status", "")).lower()
        try:
            fill_payload = self._client.get_fills(order_id=str(order_id), ticker=ticker, limit=100)
        except KalshiApiError as exc:
            events.append(OrderEvent(intent_id=intent.intent_id, event_type="error",
                                     status="failed", message=f"fill lookup failed: {exc}",
                                     market=intent.market, side=intent.side, stake=intent.stake,
                                     exchange_order_id=str(order_id)))
            return events, []
        fills = self._extract_fills(intent, order_id=str(order_id), fills_payload=fill_payload)
        events.append(OrderEvent(
            intent_id=intent.intent_id,
            event_type="filled" if fills else status,
            status="filled" if fills else status,
            message=f"status={status} fills={len(fills)}",
            market=intent.market,
            side=intent.side,
            stake=intent.stake,
            exchange_order_id=str(order_id),
        ))
        return events, fills

    def _poll_until_terminal(self, order_id: str) -> dict[str, Any] | None:
        deadline = time.monotonic() + self._poll_timeout
        last: dict[str, Any] | None = None
        while time.monotonic() <= deadline:
            try:
                last = self._client.get_order(order_id)
            except KalshiApiError:
                return None
            status = str(last.get("order", {}).get("status", "")).lower()
            if status in _TERMINAL_STATUSES:
                return last
            if self._poll_interval > 0:
                time.sleep(self._poll_interval)
        return last

    def _quote_for_signal(self, signal_side: str, market_payload: dict[str, Any]) -> _Quote | None:
        market = market_payload.get("market") if isinstance(market_payload, dict) else None
        if not isinstance(market, dict):
            return None
        if signal_side.upper() == "OVER":
            yes_ask = self._dollars_from_market(market, "yes_ask_dollars", "yes_ask")
            if yes_ask is None:
                return None
            return _Quote(order_side="bid", order_price_dollars=yes_ask, contract_cost_dollars=yes_ask)

        no_ask = self._dollars_from_market(market, "no_ask_dollars", "no_ask")
        yes_bid = self._dollars_from_market(market, "yes_bid_dollars", "yes_bid")
        if no_ask is None and yes_bid is not None:
            no_ask = max(0.0, 1.0 - yes_bid)
        if yes_bid is None and no_ask is not None:
            yes_bid = max(0.0, 1.0 - no_ask)
        if no_ask is None or yes_bid is None:
            return None
        return _Quote(order_side="ask", order_price_dollars=yes_bid, contract_cost_dollars=no_ask)

    def _dollars_from_market(
        self,
        market: dict[str, Any],
        dollars_field: str,
        legacy_cents_field: str,
    ) -> float | None:
        raw_dollars = market.get(dollars_field)
        if raw_dollars is not None:
            return self._decimal_to_float(raw_dollars)
        raw_cents = market.get(legacy_cents_field)
        cents = self._decimal_to_float(raw_cents)
        return cents / 100.0 if cents is not None else None

    def _decimal_to_float(self, value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(Decimal(str(value)))
        except (InvalidOperation, ValueError):
            return None

    def _extract_fills(
        self,
        intent: ExecutionIntent,
        *,
        order_id: str,
        fills_payload: dict[str, Any],
    ) -> list[Fill]:
        raw_fills = fills_payload.get("fills") or []
        if not isinstance(raw_fills, list):
            return []
        results: list[Fill] = []
        for idx, raw in enumerate(raw_fills, start=1):
            if not isinstance(raw, dict):
                continue
            count = self._decimal_to_float(raw.get("count_fp", raw.get("count", 0)))
            price = self._fill_price(intent.signal.side, raw)
            fee = self._decimal_to_float(raw.get("fee_cost", raw.get("fee_cost_dollars", raw.get("fee", 0)))) or 0.0
            trade_id = str(raw.get("trade_id", f"{intent.intent_id}-trade-{idx}"))
            fill_id = str(raw.get("fill_id", f"{intent.intent_id}-{trade_id}"))
            if count is None or price is None:
                continue
            if count <= 0:
                continue
            stake_dollars = count * price
            results.append(
                Fill(
                    fill_id=fill_id,
                    intent_id=intent.intent_id,
                    market=intent.market,
                    side=intent.side,
                    stake=round(stake_dollars, 4),
                    price=round(price, 4),
                    fee=round(fee, 4),
                    realized_pnl=0.0,
                    timestamp=datetime.now(UTC),
                    exchange_order_id=order_id,
                    exchange_trade_id=trade_id,
                )
            )
        return results

    def _fill_price(self, signal_side: str, raw: dict[str, Any]) -> float | None:
        if signal_side.upper() == "OVER":
            price = self._decimal_to_float(raw.get("yes_price_dollars"))
            if price is not None:
                return price
            legacy = self._decimal_to_float(raw.get("yes_price"))
            return legacy / 100.0 if legacy is not None else None

        price = self._decimal_to_float(raw.get("no_price_dollars"))
        if price is not None:
            return price
        legacy = self._decimal_to_float(raw.get("no_price"))
        return legacy / 100.0 if legacy is not None else None
