from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from app.trading.types import ExecutionIntent, MarketRef, Signal


def test_trading_types_are_frozen() -> None:
    signal = Signal(
        signal_id="sig-1",
        created_at=datetime.now(UTC),
        market_key="points",
        side="OVER",
        confidence="strong",
        edge=0.04,
        model_probability=0.57,
        line_value=21.5,
    )
    market = MarketRef(exchange="paper", symbol="paper:points:over:21.5", market_key="points", side="OVER", line_value=21.5)
    intent = ExecutionIntent(intent_id="intent-1", signal=signal, market=market, side="buy", stake=10.0)

    with pytest.raises(FrozenInstanceError):
        intent.stake = 15.0  # type: ignore[misc]
