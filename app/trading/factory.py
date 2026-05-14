from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.config.settings import Settings, get_settings
from app.providers.exchanges.kalshi_client import KalshiClient
from app.trading.kalshi_adapter import KalshiAdapter as SignedKalshiAdapter
from app.trading.paper_adapter import FakePaperAdapter, RealisticPaperAdapter
from app.trading.protocols import ExchangeAdapter
from app.trading.symbol_resolver import SymbolResolverConfigError, load_symbol_resolver
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
    """Disabled-exchange fallback for the Kalshi exchange slot.

    Always rejects orders with a human-readable reason (credentials missing,
    live flag not set, etc.). The real HTTP client is ``app.trading.kalshi_adapter``.
    """

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
        if reason is not None:
            return ExchangeAdapterConfig(exchange="kalshi", adapter=KalshiAdapter(reason=reason))
        try:
            resolver = load_symbol_resolver(settings.kalshi_symbols_path)
        except SymbolResolverConfigError as exc:
            return ExchangeAdapterConfig(
                exchange="kalshi",
                adapter=KalshiAdapter(reason=f"Kalshi symbol resolver is not live-ready: {exc}"),
            )
        client = KalshiClient(
            api_key_id=str(settings.kalshi_api_key_id),
            private_key_path=Path(str(settings.kalshi_private_key_path)),
            base_url=settings.kalshi_base_url,
            timeout_seconds=float(settings.request_timeout_seconds),
        )
        return ExchangeAdapterConfig(
            exchange="kalshi",
            adapter=SignedKalshiAdapter(client=client, resolver=resolver),
        )

    return ExchangeAdapterConfig(
        exchange=exchange,
        adapter=DisabledExchangeAdapter(
            exchange=exchange,
            reason=f"Trading exchange '{exchange}' is not supported. Set TRADING_EXCHANGE=paper.",
        ),
    )


def _kalshi_disabled_reason(settings: Settings) -> str | None:
    if not settings.trading_live_enabled:
        return "Kalshi live trading is disabled. Set TRADING_LIVE_ENABLED=true when the operator intends to use production Kalshi."
    if not settings.kalshi_api_key_id or settings.kalshi_private_key_path is None:
        return "Kalshi credentials are incomplete. Configure KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH."
    if not Path(str(settings.kalshi_private_key_path)).exists():
        return f"Kalshi private key file does not exist: {settings.kalshi_private_key_path}"
    return None
