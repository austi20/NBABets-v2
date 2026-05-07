from __future__ import annotations

from dataclasses import dataclass

from app.config.settings import Settings, get_settings
from app.trading.paper_adapter import FakePaperAdapter, RealisticPaperAdapter
from app.trading.protocols import ExchangeAdapter
from app.trading.types import ExecutionIntent, Fill, OrderEvent


@dataclass(frozen=True)
class ExchangeAdapterConfig:
    exchange: str
    adapter: ExchangeAdapter


class DisabledExchangeAdapter(ExchangeAdapter):
    def __init__(self, *, exchange: str, reason: str) -> None:
        self.exchange = exchange
        self.reason = reason

    def place_order(self, intent: ExecutionIntent) -> tuple[list[OrderEvent], list[Fill]]:
        return (
            [
                OrderEvent(
                    intent_id=intent.intent_id,
                    event_type="rejected",
                    status="blocked",
                    message=self.reason,
                )
            ],
            [],
        )


class KalshiAdapter(DisabledExchangeAdapter):
    def __init__(self, *, reason: str) -> None:
        super().__init__(exchange="kalshi", reason=reason)


def build_exchange_adapter(settings: Settings | None = None) -> ExchangeAdapterConfig:
    settings = settings or get_settings()
    exchange = (settings.trading_exchange or "paper").strip().lower()
    if exchange == "paper":
        adapter_name = (settings.trading_paper_adapter or "realistic").strip().lower()
        adapter: ExchangeAdapter
        if adapter_name in {"fake", "deterministic"}:
            adapter = FakePaperAdapter()
        else:
            adapter = RealisticPaperAdapter()
        return ExchangeAdapterConfig(exchange="paper", adapter=adapter)

    if exchange == "kalshi":
        reason = _kalshi_disabled_reason(settings)
        return ExchangeAdapterConfig(exchange="kalshi", adapter=KalshiAdapter(reason=reason))

    return ExchangeAdapterConfig(
        exchange=exchange,
        adapter=DisabledExchangeAdapter(
            exchange=exchange,
            reason=f"Trading exchange '{exchange}' is not supported. Set TRADING_EXCHANGE=paper.",
        ),
    )


def _kalshi_disabled_reason(settings: Settings) -> str:
    if not settings.trading_live_enabled:
        return "Kalshi live trading is disabled. Set TRADING_LIVE_ENABLED=true only after signing and market mapping are implemented."
    if not settings.kalshi_api_key_id or settings.kalshi_private_key_path is None:
        return "Kalshi credentials are incomplete. Configure KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH."
    return "Kalshi adapter boundary is configured, but signed order placement is not implemented yet."
