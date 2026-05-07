from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Protocol

from app.providers.exchanges.kalshi_errors import KalshiApiError
from app.trading.symbol_resolver import SymbolResolver
from app.trading.types import ExecutionIntent, Fill, MarketRef, OrderEvent

_TERMINAL_STATUSES = {"executed", "filled", "canceled", "cancelled", "rejected"}


class _KalshiClientLike(Protocol):
    def get_market(self, ticker: str) -> dict[str, Any]: ...
    def create_order(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_order(self, order_id: str) -> dict[str, Any]: ...


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
                            status="blocked", message="no kalshi ticker for signal")],
                [],
            )
        try:
            market = self._client.get_market(ticker)
        except KalshiApiError as exc:
            return (
                [OrderEvent(intent_id=intent.intent_id, event_type="error",
                            status="failed", message=f"market lookup failed: {exc}")],
                [],
            )
        ask_cents = self._extract_yes_ask_cents(market)
        if ask_cents is None or ask_cents <= 0:
            return (
                [OrderEvent(intent_id=intent.intent_id, event_type="rejected",
                            status="blocked", message="market has no tradable ask")],
                [],
            )
        contract_price_dollars = ask_cents / 100.0
        count = int(intent.stake // contract_price_dollars)
        if count < 1:
            return (
                [OrderEvent(
                    intent_id=intent.intent_id,
                    event_type="rejected",
                    status="blocked",
                    message=f"contract price {contract_price_dollars:.2f} exceeds stake cap {intent.stake:.2f}",
                )],
                [],
            )
        side_yesno = "yes" if intent.signal.side.upper() == "OVER" else "no"
        client_order_id = f"{intent.intent_id}-1"
        try:
            order_response = self._client.create_order(
                ticker=ticker,
                side=side_yesno,
                count=count,
                order_type="market",
                client_order_id=client_order_id,
            )
        except KalshiApiError as exc:
            return (
                [OrderEvent(intent_id=intent.intent_id, event_type="error",
                            status="failed", message=f"create_order failed: {exc}")],
                [],
            )
        order_id = order_response.get("order", {}).get("order_id")
        events: list[OrderEvent] = [
            OrderEvent(intent_id=intent.intent_id, event_type="accepted",
                       status="ok", message=f"kalshi order {order_id}"),
        ]
        terminal = self._poll_until_terminal(order_id) if order_id else None
        if terminal is None:
            events.append(OrderEvent(intent_id=intent.intent_id, event_type="error",
                                     status="failed", message="poll timeout, fill unknown"))
            return events, []
        status = str(terminal.get("order", {}).get("status", "")).lower()
        fills = self._extract_fills(intent, ticker, terminal)
        events.append(OrderEvent(
            intent_id=intent.intent_id,
            event_type="filled" if fills else status,
            status="ok" if fills else status,
            message=f"status={status} fills={len(fills)}",
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

    def _extract_yes_ask_cents(self, market_payload: dict[str, Any]) -> int | None:
        market = market_payload.get("market") if isinstance(market_payload, dict) else None
        if not isinstance(market, dict):
            return None
        ask = market.get("yes_ask")
        try:
            return int(ask) if ask is not None else None
        except (TypeError, ValueError):
            return None

    def _extract_fills(
        self,
        intent: ExecutionIntent,
        ticker: str,
        order_payload: dict[str, Any],
    ) -> list[Fill]:
        order = order_payload.get("order", {})
        raw_fills = order.get("fills") or []
        if not isinstance(raw_fills, list):
            return []
        market_ref = MarketRef(
            exchange="kalshi",
            symbol=f"kalshi:{ticker}",
            market_key=intent.signal.market_key,
            side=intent.signal.side.upper(),
            line_value=float(intent.signal.line_value),
        )
        results: list[Fill] = []
        for idx, raw in enumerate(raw_fills, start=1):
            try:
                count = int(raw.get("count", 0))
                price_cents = int(raw.get("yes_price", 0))
                fee_cents = int(raw.get("fee", 0))
                trade_id = str(raw.get("trade_id", f"{intent.intent_id}-trade-{idx}"))
            except (TypeError, ValueError):
                continue
            if count <= 0:
                continue
            stake_dollars = count * price_cents / 100.0
            results.append(
                Fill(
                    fill_id=f"{intent.intent_id}-{trade_id}",
                    intent_id=intent.intent_id,
                    market=market_ref,
                    side=intent.side,
                    stake=round(stake_dollars, 4),
                    price=round(price_cents / 100.0, 4),
                    fee=round(fee_cents / 100.0, 4),
                    realized_pnl=0.0,
                    timestamp=datetime.now(UTC),
                )
            )
        return results
