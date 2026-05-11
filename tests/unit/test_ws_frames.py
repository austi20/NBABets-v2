from datetime import UTC, datetime

from app.trading.market_book import MarketEntry
from app.trading.ws_frames import parse_frame

_FIXED_NOW = datetime(2026, 5, 10, 12, 0, 0, tzinfo=UTC)


def _now() -> datetime:
    return _FIXED_NOW


def test_parse_ticker_frame_returns_market_entry():
    frame = {
        "type": "ticker",
        "msg": {
            "market_ticker": "KXNBA-LAL-W",
            "yes_bid_dollars": 0.52,
            "yes_ask_dollars": 0.55,
            "no_bid_dollars": 0.45,
            "no_ask_dollars": 0.48,
        },
    }
    entry = parse_frame(frame, now=_now)
    assert isinstance(entry, MarketEntry)
    assert entry.ticker == "KXNBA-LAL-W"
    assert entry.yes_bid == 0.52
    assert entry.yes_ask == 0.55
    assert entry.no_bid == 0.45
    assert entry.no_ask == 0.48
    assert entry.spread == 0.03
    assert entry.status == "open"
    assert entry.updated_at == _FIXED_NOW


def test_parse_ticker_frame_with_missing_optional_prices():
    frame = {
        "type": "ticker",
        "msg": {
            "market_ticker": "KXNBA-LAL-W",
            "yes_bid_dollars": 0.52,
            "yes_ask_dollars": 0.55,
        },
    }
    entry = parse_frame(frame, now=_now)
    assert entry is not None
    assert entry.no_bid is None
    assert entry.no_ask is None
    assert entry.spread == 0.03


def test_parse_orderbook_delta_returns_none_in_v1():
    # v1 does not turn orderbook_delta into MarketEntry; it is ack-only.
    frame = {"type": "orderbook_delta", "msg": {"market_ticker": "KXNBA-LAL-W"}}
    assert parse_frame(frame, now=_now) is None


def test_parse_unknown_type_returns_none():
    assert parse_frame({"type": "weird"}, now=_now) is None


def test_parse_missing_market_ticker_returns_none():
    frame = {"type": "ticker", "msg": {"yes_bid_dollars": 0.52, "yes_ask_dollars": 0.55}}
    assert parse_frame(frame, now=_now) is None


def test_parse_non_dict_returns_none():
    assert parse_frame("not-a-dict", now=_now) is None  # type: ignore[arg-type]
    assert parse_frame(None, now=_now) is None  # type: ignore[arg-type]
