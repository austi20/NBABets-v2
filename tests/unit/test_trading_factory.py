from __future__ import annotations

from app.config.settings import Settings
from app.trading.factory import DisabledExchangeAdapter, KalshiAdapter, build_exchange_adapter
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
    config = build_exchange_adapter(Settings(TRADING_EXCHANGE="kalshi"))

    assert config.exchange == "kalshi"
    assert isinstance(config.adapter, KalshiAdapter)
    assert isinstance(config.adapter, DisabledExchangeAdapter)
    assert "disabled" in config.adapter.reason.lower()
