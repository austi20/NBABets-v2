from __future__ import annotations

from app.trading.types import MarketRef, Signal


def signal_to_market_ref(signal: Signal, exchange: str) -> MarketRef:
    market_slug = signal.market_key.strip().lower()
    side_slug = signal.side.strip().lower()
    game_id = signal.metadata.get("game_id", "na")
    player_id = signal.metadata.get("player_id", "na")
    symbol = (
        f"{exchange.lower()}:{market_slug}:{side_slug}:{signal.line_value:.1f}:"
        f"g{game_id}:p{player_id}"
    )
    return MarketRef(
        exchange=exchange.lower(),
        symbol=symbol,
        market_key=market_slug,
        side=signal.side.upper(),
        line_value=float(signal.line_value),
    )
