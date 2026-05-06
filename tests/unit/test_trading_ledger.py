from __future__ import annotations

from datetime import UTC, datetime

from app.trading.ledger import InMemoryPortfolioLedger
from app.trading.types import Fill, MarketRef


def test_ledger_sell_fill_reduces_open_stake() -> None:
    ledger = InMemoryPortfolioLedger()
    market = MarketRef(exchange="paper", symbol="paper:points:over:22.5:g1:p7:s1", market_key="points", side="OVER", line_value=22.5)
    ledger.record_fill(
        Fill(
            fill_id="f-buy",
            intent_id="i1",
            market=market,
            side="buy",
            stake=10.0,
            price=0.5,
            timestamp=datetime.now(UTC),
        )
    )
    ledger.record_fill(
        Fill(
            fill_id="f-sell",
            intent_id="i2",
            market=market,
            side="sell",
            stake=4.0,
            price=0.6,
            timestamp=datetime.now(UTC),
        )
    )
    positions = ledger.open_positions()
    assert len(positions) == 1
    assert abs(positions[0].open_stake - 6.0) < 1e-6
