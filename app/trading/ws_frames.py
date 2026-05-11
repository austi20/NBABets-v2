"""Pure parsers for Kalshi WebSocket frame payloads."""
from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from app.trading.market_book import MarketEntry


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _spread(yes_bid: float | None, yes_ask: float | None) -> float | None:
    if yes_bid is None or yes_ask is None:
        return None
    return round(yes_ask - yes_bid, 4)


def parse_frame(
    frame: Any,
    *,
    now: Callable[[], datetime] = _utcnow,
) -> MarketEntry | None:
    """Convert a raw decoded JSON frame into a MarketEntry, or None to skip.

    Kalshi sends `{"type": "ticker", "msg": {...}}` envelopes. Non-ticker frames
    return None in v1; the caller must handle subscription acks, errors, etc.
    """
    if not isinstance(frame, dict):
        return None
    frame_type = frame.get("type")
    if frame_type != "ticker":
        return None
    msg = frame.get("msg")
    if not isinstance(msg, dict):
        return None
    ticker = msg.get("market_ticker")
    if not isinstance(ticker, str) or not ticker:
        return None
    yes_bid = _float_or_none(msg.get("yes_bid_dollars"))
    yes_ask = _float_or_none(msg.get("yes_ask_dollars"))
    no_bid = _float_or_none(msg.get("no_bid_dollars"))
    no_ask = _float_or_none(msg.get("no_ask_dollars"))
    return MarketEntry(
        ticker=ticker,
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        last=_float_or_none(msg.get("last_dollars")),
        spread=_spread(yes_bid, yes_ask),
        status=str(msg.get("status") or "open"),
        updated_at=now(),
    )
