from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast

import httpx

from app.trading.protocols import PortfolioLedger
from app.trading.risk import RiskLimits
from app.trading.types import Position

_API_ROOT = "/trade-api/v2"


class MarketDataClient(Protocol):
    def get_market(self, ticker: str) -> dict[str, Any]: ...


class AccountClient(Protocol):
    def get_positions(
        self,
        *,
        ticker: str | None = None,
        event_ticker: str | None = None,
        count_filter: str | None = "position,total_traded",
        limit: int = 100,
    ) -> dict[str, Any]: ...

    def get_orders(
        self,
        *,
        ticker: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class MonitoredSymbol:
    ticker: str
    market_key: str
    side: str | None
    line_value: float | None
    player_id: str | None
    game_date: str | None
    title: str | None = None


@dataclass(frozen=True)
class QuoteSnapshot:
    ticker: str
    market_key: str
    side: str | None
    line_value: float | None
    player_id: str | None
    game_date: str | None
    title: str | None
    status: str | None
    yes_bid: float | None
    yes_ask: float | None
    no_bid: float | None
    no_ask: float | None
    last_price: float | None
    entry_price: float | None
    exit_price: float | None
    spread: float | None
    observed_at: datetime
    error: str | None = None


@dataclass(frozen=True)
class LivePositionSnapshot:
    market_symbol: str
    market_key: str
    side: str
    ticker: str | None
    open_stake: float
    contract_count: float
    avg_price: float
    current_exit_price: float | None
    current_value: float | None
    unrealized_pnl: float | None
    unrealized_pnl_pct: float | None
    realized_pnl: float
    updated_at: datetime
    quote: QuoteSnapshot | None = None


@dataclass(frozen=True)
class ExchangePositionSnapshot:
    ticker: str
    side: str
    contract_count: float
    net_position: float
    market_exposure: float | None
    fees_paid: float | None
    realized_pnl: float | None
    current_exit_price: float | None
    current_value: float | None
    updated_at: datetime | None
    quote: QuoteSnapshot | None = None


@dataclass(frozen=True)
class RestingOrderSnapshot:
    order_id: str
    client_order_id: str | None
    ticker: str | None
    side: str | None
    status: str | None
    remaining_count: float | None
    price: float | None
    created_at: datetime | None


@dataclass(frozen=True)
class TradingMonitorSnapshot:
    observed_at: datetime
    daily_realized_pnl: float
    daily_unrealized_pnl: float
    total_daily_pnl: float
    open_notional: float
    budget_used: float
    budget_remaining: float
    max_open_notional: float
    daily_loss_cap: float
    loss_progress: float
    kill_switch_active: bool
    positions: list[LivePositionSnapshot]
    quotes: list[QuoteSnapshot]
    account_positions: list[ExchangePositionSnapshot]
    resting_orders: list[RestingOrderSnapshot]
    errors: list[str]


class KalshiPublicMarketDataClient:
    def __init__(self, *, base_url: str, timeout_seconds: float = 10.0) -> None:
        self._base_url, self._api_root_path = _normalise_base_url(base_url)
        self._client = httpx.Client(base_url=self._base_url, timeout=timeout_seconds)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> KalshiPublicMarketDataClient:
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def get_market(self, ticker: str) -> dict[str, Any]:
        response = self._client.get(f"{self._api_root_path}/markets/{ticker}")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Kalshi market response must be a JSON object")
        return payload


def load_monitored_symbols(path: Path | str) -> list[MonitoredSymbol]:
    config_path = Path(path)
    if not config_path.is_file():
        return []
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    entries = _entries_from_payload(payload)
    symbols: list[MonitoredSymbol] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        ticker = entry.get("kalshi_ticker") or entry.get("ticker")
        if not ticker:
            continue
        symbols.append(
            MonitoredSymbol(
                ticker=str(ticker),
                market_key=str(entry.get("market_key", "")),
                side=_side_from_entry(entry),
                line_value=_float_or_none(entry.get("line_value")),
                player_id=(str(entry["player_id"]) if entry.get("player_id") is not None else None),
                game_date=(str(entry["game_date"]) if entry.get("game_date") is not None else None),
                title=(str(entry["title"]) if entry.get("title") else None),
            )
        )
    return symbols


def build_monitor_snapshot(
    *,
    ledger: PortfolioLedger,
    limits: RiskLimits,
    kill_switch_active: bool,
    monitored_symbols: Sequence[MonitoredSymbol],
    market_client: MarketDataClient,
    account_client: AccountClient | None = None,
) -> TradingMonitorSnapshot:
    observed_at = datetime.now(UTC)
    positions = ledger.open_positions()
    account_position_payloads: list[dict[str, Any]] = []
    account_order_payloads: list[dict[str, Any]] = []
    account_errors: list[str] = []
    if account_client is not None:
        try:
            account_position_payloads = _account_position_payloads(account_client)
        except Exception as exc:  # noqa: BLE001 - monitoring should stay partial
            account_errors.append(f"Kalshi account positions unavailable: {exc}")
        try:
            account_order_payloads = _account_order_payloads(account_client)
        except Exception as exc:  # noqa: BLE001 - monitoring should stay partial
            account_errors.append(f"Kalshi resting orders unavailable: {exc}")
    quote_symbols = _dedupe_symbols(
        [*_merge_symbols(monitored_symbols, positions), *_account_symbols(account_position_payloads)]
    )
    quotes = [_quote_for_symbol(symbol, market_client, observed_at) for symbol in quote_symbols]
    quote_by_ticker = {quote.ticker: quote for quote in quotes}
    symbol_by_position_key = _symbols_by_position_key(quote_symbols)
    live_positions = [
        _position_snapshot(position, quote_by_ticker, symbol_by_position_key)
        for position in positions
    ]
    account_positions = [
        _exchange_position_snapshot(payload, quote_by_ticker)
        for payload in account_position_payloads
    ]
    resting_orders = [
        _resting_order_snapshot(payload)
        for payload in account_order_payloads
    ]
    daily_unrealized = sum(
        position.unrealized_pnl
        for position in live_positions
        if position.unrealized_pnl is not None
    )
    daily_realized = ledger.daily_realized_pnl()
    total_daily = daily_realized + daily_unrealized
    open_notional = ledger.open_notional()
    daily_loss_cap = float(limits.daily_loss_cap)
    max_open_notional = float(limits.max_open_notional)
    return TradingMonitorSnapshot(
        observed_at=observed_at,
        daily_realized_pnl=round(daily_realized, 4),
        daily_unrealized_pnl=round(daily_unrealized, 4),
        total_daily_pnl=round(total_daily, 4),
        open_notional=round(open_notional, 4),
        budget_used=round(open_notional, 4),
        budget_remaining=round(max(0.0, max_open_notional - open_notional), 4),
        max_open_notional=round(max_open_notional, 4),
        daily_loss_cap=round(daily_loss_cap, 4),
        loss_progress=_bounded_progress(abs(min(total_daily, 0.0)), daily_loss_cap),
        kill_switch_active=kill_switch_active,
        positions=live_positions,
        quotes=quotes,
        account_positions=account_positions,
        resting_orders=resting_orders,
        errors=[quote.error for quote in quotes if quote.error] + account_errors,
    )


def _normalise_base_url(base_url: str) -> tuple[str, str]:
    url = httpx.URL(base_url.rstrip("/"))
    root_path = url.path.rstrip("/")
    api_path = root_path if root_path.endswith(_API_ROOT) else f"{root_path}{_API_ROOT}" if root_path else _API_ROOT
    origin = str(url.copy_with(path="/")).rstrip("/")
    return origin, api_path


def _entries_from_payload(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("symbols"), list):
        return cast(list[Any], payload["symbols"])
    return []


def _side_from_entry(entry: dict[str, Any]) -> str | None:
    value = str(entry.get("side", entry.get("recommendation", ""))).strip().lower()
    if value in {"over", "buy_yes", "yes"}:
        return "OVER"
    if value in {"under", "buy_no", "no"}:
        return "UNDER"
    return None


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _merge_symbols(
    configured: Sequence[MonitoredSymbol],
    positions: Sequence[Position],
) -> list[MonitoredSymbol]:
    merged: dict[str, MonitoredSymbol] = {symbol.ticker: symbol for symbol in configured}
    configured_by_key = _symbols_by_position_key(configured)
    for position in positions:
        key = _position_key(position)
        symbol = configured_by_key.get(key)
        if symbol is not None:
            merged.setdefault(symbol.ticker, symbol)
    return list(merged.values())


def _dedupe_symbols(symbols: Sequence[MonitoredSymbol]) -> list[MonitoredSymbol]:
    merged: dict[str, MonitoredSymbol] = {}
    for symbol in symbols:
        merged.setdefault(symbol.ticker, symbol)
    return list(merged.values())


def _symbols_by_position_key(symbols: Iterable[MonitoredSymbol]) -> dict[tuple[str, str, float, str | None], MonitoredSymbol]:
    out: dict[tuple[str, str, float, str | None], MonitoredSymbol] = {}
    for symbol in symbols:
        if symbol.side is None or symbol.line_value is None:
            continue
        key = (
            _normalise_market_key(symbol.market_key),
            symbol.side.upper(),
            round(float(symbol.line_value), 2),
            symbol.player_id,
        )
        out[key] = symbol
    return out


def _position_key(position: Position) -> tuple[str, str, float, str | None]:
    parsed = _parse_market_symbol(position.market_symbol)
    return (
        _normalise_market_key(position.market_key),
        position.side.upper(),
        round(float(parsed.get("line_value", 0.0)), 2),
        parsed.get("player_id"),
    )


def _normalise_market_key(value: str) -> str:
    text = value.strip().lower()
    return text.split(".")[-1] if "." in text else text


def _parse_market_symbol(symbol: str) -> dict[str, Any]:
    parts = symbol.split(":")
    out: dict[str, Any] = {}
    if len(parts) >= 6:
        out["exchange"] = parts[0]
        out["market_key"] = parts[1]
        out["side"] = parts[2].upper()
        out["line_value"] = _float_or_none(parts[3]) or 0.0
        out["game_id"] = parts[4][1:] if parts[4].startswith("g") else parts[4]
        out["player_id"] = parts[5][1:] if parts[5].startswith("p") else parts[5]
    return out


def _quote_for_symbol(
    symbol: MonitoredSymbol,
    market_client: MarketDataClient,
    observed_at: datetime,
) -> QuoteSnapshot:
    try:
        payload = market_client.get_market(symbol.ticker)
        market = payload.get("market") if isinstance(payload, dict) else None
        if not isinstance(market, dict):
            raise ValueError("market payload missing market object")
        yes_bid = _float_or_none(market.get("yes_bid_dollars"))
        if yes_bid is None:
            yes_bid = _cents_to_dollars(market.get("yes_bid"))
        yes_ask = _float_or_none(market.get("yes_ask_dollars"))
        if yes_ask is None:
            yes_ask = _cents_to_dollars(market.get("yes_ask"))
        no_bid = _float_or_none(market.get("no_bid_dollars"))
        no_ask = _float_or_none(market.get("no_ask_dollars"))
        if no_bid is None and yes_ask is not None:
            no_bid = max(0.0, 1.0 - yes_ask)
        if no_ask is None and yes_bid is not None:
            no_ask = max(0.0, 1.0 - yes_bid)
        entry_price = _side_ask(symbol.side, yes_ask, no_ask)
        exit_price = _side_bid(symbol.side, yes_bid, no_bid)
        spread = (
            round(entry_price - exit_price, 4)
            if entry_price is not None and exit_price is not None
            else None
        )
        return QuoteSnapshot(
            ticker=symbol.ticker,
            market_key=symbol.market_key,
            side=symbol.side,
            line_value=symbol.line_value,
            player_id=symbol.player_id,
            game_date=symbol.game_date,
            title=str(market.get("title") or symbol.title or ""),
            status=(str(market.get("status")) if market.get("status") is not None else None),
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            last_price=_price_from_market(market, "last_price_dollars", "last_price"),
            entry_price=entry_price,
            exit_price=exit_price,
            spread=spread,
            observed_at=observed_at,
        )
    except Exception as exc:  # noqa: BLE001 - monitoring should return partial snapshots
        return QuoteSnapshot(
            ticker=symbol.ticker,
            market_key=symbol.market_key,
            side=symbol.side,
            line_value=symbol.line_value,
            player_id=symbol.player_id,
            game_date=symbol.game_date,
            title=symbol.title,
            status=None,
            yes_bid=None,
            yes_ask=None,
            no_bid=None,
            no_ask=None,
            last_price=None,
            entry_price=None,
            exit_price=None,
            spread=None,
            observed_at=observed_at,
            error=f"{symbol.ticker}: {exc}",
        )


def fetch_quote_snapshot(
    symbol: MonitoredSymbol,
    market_client: MarketDataClient,
    observed_at: datetime | None = None,
) -> QuoteSnapshot:
    """Public helper: load a :class:`QuoteSnapshot` for one symbol via ``market_client``."""
    return _quote_for_symbol(symbol, market_client, observed_at or datetime.now(UTC))


def _cents_to_dollars(value: Any) -> float | None:
    cents = _float_or_none(value)
    return round(cents / 100.0, 4) if cents is not None else None


def _price_from_market(market: dict[str, Any], dollars_field: str, cents_field: str) -> float | None:
    dollars = _float_or_none(market.get(dollars_field))
    return dollars if dollars is not None else _cents_to_dollars(market.get(cents_field))


def _side_ask(side: str | None, yes_ask: float | None, no_ask: float | None) -> float | None:
    if side == "OVER":
        return yes_ask
    if side == "UNDER":
        return no_ask
    return None


def _side_bid(side: str | None, yes_bid: float | None, no_bid: float | None) -> float | None:
    if side == "OVER":
        return yes_bid
    if side == "UNDER":
        return no_bid
    return None


def _position_snapshot(
    position: Position,
    quote_by_ticker: dict[str, QuoteSnapshot],
    symbol_by_position_key: dict[tuple[str, str, float, str | None], MonitoredSymbol],
) -> LivePositionSnapshot:
    symbol = symbol_by_position_key.get(_position_key(position))
    quote = quote_by_ticker.get(symbol.ticker) if symbol else None
    contract_count = (
        float(position.open_stake) / float(position.avg_price)
        if float(position.avg_price) > 0
        else 0.0
    )
    current_exit = quote.exit_price if quote is not None else None
    current_value = (
        round(contract_count * current_exit, 4)
        if current_exit is not None
        else None
    )
    unrealized = (
        round(current_value - float(position.open_stake), 4)
        if current_value is not None
        else None
    )
    unrealized_pct = (
        round(unrealized / float(position.open_stake), 6)
        if unrealized is not None and float(position.open_stake) > 0
        else None
    )
    return LivePositionSnapshot(
        market_symbol=position.market_symbol,
        market_key=position.market_key,
        side=position.side,
        ticker=symbol.ticker if symbol else None,
        open_stake=round(float(position.open_stake), 4),
        contract_count=round(contract_count, 4),
        avg_price=round(float(position.avg_price), 4),
        current_exit_price=current_exit,
        current_value=current_value,
        unrealized_pnl=unrealized,
        unrealized_pnl_pct=unrealized_pct,
        realized_pnl=round(float(position.realized_pnl), 4),
        updated_at=position.updated_at,
        quote=quote,
    )


def _bounded_progress(value: float, cap: float) -> float:
    if cap <= 0:
        return 0.0
    return round(max(0.0, min(1.0, value / cap)), 6)


def _normalize_exchange_position_row(row: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    ticker = row.get("ticker") or row.get("market_ticker")
    if not ticker:
        return None
    merged = dict(row)
    merged["ticker"] = str(ticker)
    return merged


def _account_position_payloads(account_client: AccountClient) -> list[dict[str, Any]]:
    payload = account_client.get_positions(limit=100)
    raw_positions = payload.get("market_positions") or payload.get("positions") or []
    if not isinstance(raw_positions, list):
        return []
    out: list[dict[str, Any]] = []
    for row in raw_positions:
        normalized = _normalize_exchange_position_row(row) if isinstance(row, dict) else None
        if normalized is not None:
            out.append(normalized)
    return out


def _account_order_payloads(account_client: AccountClient) -> list[dict[str, Any]]:
    payload = account_client.get_orders(status="resting", limit=100)
    raw_orders = payload.get("orders") or []
    if not isinstance(raw_orders, list):
        return []
    return [row for row in raw_orders if isinstance(row, dict)]


def _account_symbols(position_payloads: Sequence[dict[str, Any]]) -> list[MonitoredSymbol]:
    symbols: list[MonitoredSymbol] = []
    for row in position_payloads:
        ticker = row.get("ticker")
        if not ticker:
            continue
        net_position = _position_count(row)
        symbols.append(
            MonitoredSymbol(
                ticker=str(ticker),
                market_key=str(row.get("market_key") or row.get("market_ticker") or ""),
                side="OVER" if net_position >= 0 else "UNDER",
                line_value=None,
                player_id=None,
                game_date=None,
                title=None,
            )
        )
    return symbols


def _position_count(row: dict[str, Any]) -> float:
    return _float_or_none(row.get("position_fp", row.get("position"))) or 0.0


def _first_float(row: dict[str, Any], *fields: str) -> float | None:
    for field in fields:
        value = _float_or_none(row.get(field))
        if value is not None:
            return value
    return None


def _first_price(row: dict[str, Any], *fields: str) -> float | None:
    for field in fields:
        if field.endswith("_dollars"):
            value = _float_or_none(row.get(field))
        else:
            raw = _float_or_none(row.get(field))
            value = raw / 100.0 if raw is not None and abs(raw) > 1 else raw
        if value is not None:
            return value
    return None


def _exchange_position_snapshot(
    row: dict[str, Any],
    quote_by_ticker: dict[str, QuoteSnapshot],
) -> ExchangePositionSnapshot:
    ticker = str(row.get("ticker", ""))
    net_position = _position_count(row)
    side = "YES" if net_position > 0 else "NO" if net_position < 0 else "FLAT"
    contracts = abs(net_position)
    quote = quote_by_ticker.get(ticker)
    current_exit = None
    if quote is not None:
        if net_position > 0:
            current_exit = quote.yes_bid
        elif net_position < 0:
            current_exit = quote.no_bid
    current_value = round(contracts * current_exit, 4) if current_exit is not None else None
    return ExchangePositionSnapshot(
        ticker=ticker,
        side=side,
        contract_count=round(contracts, 4),
        net_position=round(net_position, 4),
        market_exposure=_first_price(row, "market_exposure_dollars", "market_exposure"),
        fees_paid=_first_price(row, "fees_paid_dollars", "fees_paid"),
        realized_pnl=_first_price(row, "realized_pnl_dollars", "realized_pnl"),
        current_exit_price=current_exit,
        current_value=current_value,
        updated_at=_datetime_or_none(row.get("last_updated_ts", row.get("updated_at"))),
        quote=quote,
    )


def _resting_order_snapshot(row: dict[str, Any]) -> RestingOrderSnapshot:
    return RestingOrderSnapshot(
        order_id=str(row.get("order_id", "")),
        client_order_id=(str(row["client_order_id"]) if row.get("client_order_id") else None),
        ticker=(str(row["ticker"]) if row.get("ticker") else None),
        side=_resting_order_side(row),
        status=(str(row["status"]) if row.get("status") else None),
        remaining_count=_first_float(row, "remaining_count_fp", "remaining_count"),
        price=_resting_order_price(row),
        created_at=_datetime_or_none(row.get("created_time", row.get("created_at"))),
    )


def _resting_order_side(row: dict[str, Any]) -> str | None:
    book_side = row.get("book_side")
    if book_side:
        return str(book_side)
    action = row.get("action")
    side = row.get("side")
    if action and side:
        return f"{action} {side}".upper()
    return str(side) if side else None


def _resting_order_price(row: dict[str, Any]) -> float | None:
    side = str(row.get("side", "")).strip().lower()
    if side == "no":
        return _first_price(row, "no_price_dollars", "price_dollars", "no_price", "price")
    return _first_price(row, "yes_price_dollars", "price_dollars", "yes_price", "price")


def _datetime_or_none(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None
