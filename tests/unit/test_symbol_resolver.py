from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest  # noqa: F401 - used by pytest fixtures implicitly
from app.trading.symbol_resolver import (
    SymbolResolver,
    SymbolResolverConfigError,
    load_symbol_resolver,
)

from app.trading.types import (
    ExecutionIntent,
    MarketRef,
    Signal,
)


def _signal(player_id: int, market_key: str, side: str, line: float, game_date: str | None) -> Signal:
    metadata: dict = {"player_id": player_id, "game_id": 1}
    if game_date is not None:
        metadata["game_date"] = game_date
    return Signal(
        signal_id="s1",
        created_at=datetime.now(UTC),
        market_key=market_key,
        side=side,
        confidence="high",
        edge=0.05,
        model_probability=0.55,
        line_value=line,
        metadata=metadata,
    )


def _intent(signal: Signal) -> ExecutionIntent:
    return ExecutionIntent(
        intent_id="i1",
        signal=signal,
        market=MarketRef(
            exchange="kalshi",
            symbol="kalshi:x",
            market_key=signal.market_key,
            side=signal.side,
            line_value=signal.line_value,
        ),
        side="buy",
        stake=0.25,
    )


def test_resolver_exact_match(tmp_path: Path) -> None:
    config = tmp_path / "syms.json"
    config.write_text(
        json.dumps(
            [
                {
                    "market_key": "points",
                    "side": "over",
                    "line_value": 25.5,
                    "player_id": 237,
                    "game_date": "2026-05-06",
                    "kalshi_ticker": "KX-LEBRON-OPTS25",
                }
            ]
        )
    )
    resolver = load_symbol_resolver(config)
    intent = _intent(_signal(237, "points", "OVER", 25.5, "2026-05-06"))
    assert resolver.resolve(intent) == "KX-LEBRON-OPTS25"


def test_resolver_miss_returns_none(tmp_path: Path) -> None:
    config = tmp_path / "syms.json"
    config.write_text(json.dumps([]))
    resolver = load_symbol_resolver(config)
    intent = _intent(_signal(999, "points", "OVER", 25.5, "2026-05-06"))
    assert resolver.resolve(intent) is None


def test_resolver_uses_signal_created_date_when_metadata_absent(tmp_path: Path) -> None:
    today = datetime.now(UTC).date().isoformat()
    config = tmp_path / "syms.json"
    config.write_text(
        json.dumps(
            [
                {
                    "market_key": "points",
                    "side": "over",
                    "line_value": 25.5,
                    "player_id": 237,
                    "game_date": today,
                    "kalshi_ticker": "KX-LEBRON-OPTS25",
                }
            ]
        )
    )
    resolver = load_symbol_resolver(config)
    intent = _intent(_signal(237, "points", "OVER", 25.5, game_date=None))
    assert resolver.resolve(intent) == "KX-LEBRON-OPTS25"


def test_resolver_malformed_json_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    with pytest.raises(SymbolResolverConfigError, match="malformed"):
        load_symbol_resolver(bad)


def test_resolver_missing_field_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps([{"market_key": "points"}]))
    with pytest.raises(SymbolResolverConfigError, match="missing"):
        load_symbol_resolver(bad)


def test_resolver_count_property(tmp_path: Path) -> None:
    config = tmp_path / "syms.json"
    config.write_text(json.dumps([
        {"market_key": "points", "side": "over", "line_value": 25.5,
         "player_id": 237, "game_date": "2026-05-06", "kalshi_ticker": "T1"},
        {"market_key": "points", "side": "over", "line_value": 27.5,
         "player_id": 237, "game_date": "2026-05-06", "kalshi_ticker": "T2"},
    ]))
    resolver = load_symbol_resolver(config)
    assert resolver.ticker_count == 2


def test_direct_constructor_accepts_inline_entries() -> None:
    resolver = SymbolResolver(entries=[
        {"market_key": "points", "side": "over", "line_value": 25.5,
         "player_id": 237, "game_date": "2026-05-06", "kalshi_ticker": "T1"},
    ])
    assert resolver.ticker_count == 1
