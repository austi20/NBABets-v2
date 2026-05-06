from __future__ import annotations

from datetime import UTC, datetime

from app.trading.mapper import signal_to_market_ref
from app.trading.types import Signal


def test_signal_to_market_ref_uses_game_and_player_scope() -> None:
    signal = Signal(
        signal_id="sig-1",
        created_at=datetime.now(UTC),
        market_key="points",
        side="OVER",
        confidence="solid",
        edge=0.04,
        model_probability=0.57,
        line_value=21.5,
        metadata={"game_id": 1001, "player_id": 55},
    )
    market = signal_to_market_ref(signal, "paper")
    assert "g1001" in market.symbol
    assert "p55" in market.symbol
