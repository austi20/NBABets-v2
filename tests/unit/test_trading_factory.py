from __future__ import annotations

import json
from pathlib import Path

from app.config.settings import Settings
from app.trading.factory import DisabledExchangeAdapter, KalshiAdapter, build_exchange_adapter
from app.trading.kalshi_adapter import KalshiAdapter as SignedKalshiAdapter
from app.trading.paper_adapter import FakePaperAdapter


def test_trading_factory_builds_deterministic_paper_adapter() -> None:
    config = build_exchange_adapter(
        Settings(
            TRADING_EXCHANGE="paper",
            TRADING_PAPER_ADAPTER="fake",
        )
    )

    assert config.exchange == "paper"
    assert isinstance(config.adapter, FakePaperAdapter)


def test_trading_factory_gates_kalshi_live_adapter() -> None:
    config = build_exchange_adapter(Settings(TRADING_EXCHANGE="kalshi", TRADING_LIVE_ENABLED=False))

    assert config.exchange == "kalshi"
    assert isinstance(config.adapter, KalshiAdapter)
    assert isinstance(config.adapter, DisabledExchangeAdapter)
    assert "disabled" in config.adapter.reason.lower()


def test_trading_factory_builds_signed_kalshi_adapter_when_live_ready(tmp_path: Path) -> None:
    key_path = tmp_path / "kalshi.pem"
    key_path.write_text("not-used-by-constructor", encoding="utf-8")
    symbols_path = tmp_path / "symbols.json"
    symbols_path.write_text(
        json.dumps(
            {
                "version": 1,
                "symbols": [
                    {
                        "market_key": "points",
                        "recommendation": "buy_yes",
                        "candidate_status": "selected_live",
                        "line_value": 25.5,
                        "player_id": "237",
                        "game_date": "2026-05-07",
                        "kalshi_ticker": "KX-TEST",
                    }
                ],
                "unresolved": [],
            }
        ),
        encoding="utf-8",
    )

    config = build_exchange_adapter(
        Settings(
            TRADING_EXCHANGE="kalshi",
            TRADING_LIVE_ENABLED=True,
            KALSHI_API_KEY_ID="key-id",
            KALSHI_PRIVATE_KEY_PATH=str(key_path),
            KALSHI_SYMBOLS_PATH=str(symbols_path),
        )
    )

    assert config.exchange == "kalshi"
    assert isinstance(config.adapter, SignedKalshiAdapter)
