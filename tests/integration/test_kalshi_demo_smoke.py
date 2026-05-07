"""Gated end-to-end smoke test against the Kalshi demo environment."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.db.models.trading import (
    TradingDailyPnL,
    TradingFill,
    TradingKillSwitch,
    TradingOrder,
    TradingPosition,
)
from app.providers.exchanges.kalshi_client import KalshiClient
from app.trading.kalshi_adapter import KalshiAdapter
from app.trading.loop import TradingLoop
from app.trading.risk import ExposureRiskEngine, RiskLimits
from app.trading.sql_ledger import SqlPortfolioLedger
from app.trading.symbol_resolver import SymbolResolver
from app.trading.types import Signal

REQUIRED_ENV = (
    "KALSHI_DEMO_API_KEY_ID",
    "KALSHI_DEMO_PRIVATE_KEY_PATH",
    "KALSHI_DEMO_TICKER",
    "KALSHI_DEMO_PLAYER_ID",
    "KALSHI_DEMO_GAME_DATE",
)


@pytest.mark.integration
def test_kalshi_demo_one_order_persists_fill(tmp_path: Path) -> None:
    missing = [key for key in REQUIRED_ENV if not os.environ.get(key)]
    if missing:
        pytest.skip(f"Kalshi demo env vars missing: {missing}")

    market_key = os.environ.get("KALSHI_DEMO_MARKET_KEY", "points")
    side = os.environ.get("KALSHI_DEMO_SIDE", "over").upper()
    line_value = float(os.environ.get("KALSHI_DEMO_LINE", "25.5"))
    stake = float(os.environ.get("KALSHI_DEMO_STAKE", "1.0"))
    player_id = int(os.environ["KALSHI_DEMO_PLAYER_ID"])
    game_date = os.environ["KALSHI_DEMO_GAME_DATE"]

    engine = create_engine(f"sqlite:///{tmp_path / 'kalshi-demo.sqlite'}", future=True)
    Base.metadata.create_all(
        engine,
        tables=[
            TradingOrder.__table__,
            TradingFill.__table__,
            TradingPosition.__table__,
            TradingKillSwitch.__table__,
            TradingDailyPnL.__table__,
        ],
    )
    factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    resolver = SymbolResolver(
        entries=[
            {
                "market_key": market_key,
                "side": side.lower(),
                "line_value": line_value,
                "player_id": player_id,
                "game_date": game_date,
                "kalshi_ticker": os.environ["KALSHI_DEMO_TICKER"],
            }
        ]
    )
    client = KalshiClient(
        api_key_id=os.environ["KALSHI_DEMO_API_KEY_ID"],
        private_key_path=os.environ["KALSHI_DEMO_PRIVATE_KEY_PATH"],
        base_url=os.environ.get(
            "KALSHI_DEMO_BASE_URL",
            "https://external-api.demo.kalshi.co/trade-api/v2",
        ),
    )

    ledger = SqlPortfolioLedger(factory)
    adapter = KalshiAdapter(client=client, resolver=resolver)
    signal = Signal(
        signal_id="kalshi-demo-1",
        created_at=datetime.now(UTC),
        market_key=market_key,
        side=side,
        confidence="demo",
        edge=0.0,
        model_probability=0.5,
        line_value=line_value,
        metadata={
            "player_id": player_id,
            "game_id": os.environ.get("KALSHI_DEMO_GAME_ID", "demo"),
            "game_date": game_date,
        },
    )
    loop = TradingLoop(
        risk_engine=ExposureRiskEngine(
            RiskLimits(
                per_order_cap=stake,
                per_market_cap=stake,
                max_open_notional=stake,
                daily_loss_cap=10.0,
                reject_cooldown_seconds=0,
            )
        ),
        ledger=ledger,
        adapter=adapter,
        session_factory=factory,
    )

    try:
        result = loop.run_signals([signal], exchange="kalshi", stake=stake)
    finally:
        client.close()

    assert result.fills >= 1, (
        "Kalshi demo smoke must persist at least one fill; "
        f"got accepted={result.accepted} rejected={result.rejected} events={result.events}"
    )
    with factory() as session:
        fills = session.execute(select(TradingFill)).scalars().all()
        assert len(fills) == result.fills
