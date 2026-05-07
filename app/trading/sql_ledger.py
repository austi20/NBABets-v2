from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.trading import (
    TradingDailyPnL,
    TradingFill,
    TradingOrder,
    TradingPosition,
)
from app.trading.types import Fill, MarketRef, OrderEvent, Position


class SqlPortfolioLedger:
    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    def record_order_event(self, event: OrderEvent) -> None:
        with self._session_factory() as session:
            order = session.get(TradingOrder, event.intent_id)
            now = event.timestamp
            if order is None:
                order = TradingOrder(
                    intent_id=event.intent_id,
                    kalshi_order_id=None,
                    market_symbol="",
                    market_key="",
                    side="",
                    stake=0.0,
                    status=event.status,
                    message=event.message,
                    created_at=now,
                    updated_at=now,
                )
                session.add(order)
            else:
                order.status = event.status
                order.message = event.message
                order.updated_at = now
            session.commit()

    def record_fill(self, fill: Fill) -> None:
        with self._session_factory() as session:
            if session.get(TradingFill, fill.fill_id) is not None:
                return  # idempotent
            # Ensure a parent TradingOrder row exists (FK requirement)
            if session.get(TradingOrder, fill.intent_id) is None:
                now = fill.timestamp
                session.add(
                    TradingOrder(
                        intent_id=fill.intent_id,
                        kalshi_order_id=None,
                        market_symbol=fill.market.symbol,
                        market_key=fill.market.market_key,
                        side=fill.side,
                        stake=float(fill.stake),
                        status="filled",
                        message="",
                        created_at=now,
                        updated_at=now,
                    )
                )
            self._upsert_position(session, fill)
            session.add(
                TradingFill(
                    fill_id=fill.fill_id,
                    intent_id=fill.intent_id,
                    market_symbol=fill.market.symbol,
                    market_key=fill.market.market_key,
                    side=fill.side,
                    stake=float(fill.stake),
                    price=float(fill.price),
                    fee=float(fill.fee),
                    realized_pnl=float(fill.realized_pnl),
                    kalshi_trade_id=None,
                    filled_at=fill.timestamp,
                )
            )
            session.commit()

    def _upsert_position(self, session: Session, fill: Fill) -> None:
        market: MarketRef = fill.market
        position = session.get(TradingPosition, market.symbol)
        fill_stake = float(fill.stake)
        computed_realized = 0.0

        if position is None:
            position = TradingPosition(
                market_symbol=market.symbol,
                market_key=market.market_key,
                side=market.side,
                open_stake=0.0,
                weighted_price_total=0.0,
                realized_pnl=0.0,
                updated_at=fill.timestamp,
            )
            session.add(position)

        if fill.side == "sell":
            avg_price = (
                position.weighted_price_total / position.open_stake
                if position.open_stake > 0
                else 0.0
            )
            closing = min(fill_stake, position.open_stake)
            if closing > 0:
                position.open_stake -= closing
                position.weighted_price_total = avg_price * position.open_stake
                computed_realized = (float(fill.price) - avg_price) * closing
        else:
            position.open_stake += fill_stake
            position.weighted_price_total += float(fill.price) * fill_stake

        realized = fill.realized_pnl + computed_realized - fill.fee
        position.realized_pnl += realized
        position.updated_at = fill.timestamp

        fill_day = fill.timestamp.astimezone(UTC).date()
        daily = session.get(TradingDailyPnL, fill_day)
        if daily is None:
            daily = TradingDailyPnL(date=fill_day, realized_pnl=0.0)
            session.add(daily)
        daily.realized_pnl += realized

    def open_positions(self) -> list[Position]:
        with self._session_factory() as session:
            rows = session.execute(
                select(TradingPosition).where(TradingPosition.open_stake > 0)
            ).scalars().all()
            results = [
                Position(
                    market_symbol=row.market_symbol,
                    market_key=row.market_key,
                    side=row.side,
                    open_stake=round(row.open_stake, 4),
                    avg_price=round(
                        row.weighted_price_total / row.open_stake if row.open_stake > 0 else 0.0,
                        6,
                    ),
                    realized_pnl=round(row.realized_pnl, 4),
                    updated_at=row.updated_at,
                )
                for row in rows
            ]
            results.sort(key=lambda r: r.updated_at, reverse=True)
            return results

    def recent_fills(self, limit: int = 20) -> list[Fill]:
        bounded = max(0, int(limit))
        if bounded == 0:
            return []
        with self._session_factory() as session:
            rows = session.execute(
                select(TradingFill).order_by(TradingFill.filled_at.desc()).limit(bounded)
            ).scalars().all()
            return [
                Fill(
                    fill_id=row.fill_id,
                    intent_id=row.intent_id,
                    market=MarketRef(
                        exchange="kalshi",
                        symbol=row.market_symbol,
                        market_key=row.market_key,
                        side=row.side,
                        line_value=0.0,
                    ),
                    side=row.side,  # type: ignore[arg-type]
                    stake=row.stake,
                    price=row.price,
                    fee=row.fee,
                    realized_pnl=row.realized_pnl,
                    timestamp=row.filled_at,
                )
                for row in rows
            ]

    def market_exposure(self, market_symbol: str) -> float:
        with self._session_factory() as session:
            row = session.get(TradingPosition, market_symbol)
            return float(row.open_stake) if row else 0.0

    def open_notional(self) -> float:
        with self._session_factory() as session:
            return float(
                sum(
                    session.execute(
                        select(TradingPosition.open_stake).where(TradingPosition.open_stake > 0)
                    ).scalars().all()
                )
            )

    def daily_realized_pnl(self) -> float:
        with self._session_factory() as session:
            today = datetime.now(UTC).date()
            row = session.get(TradingDailyPnL, today)
            return float(row.realized_pnl) if row else 0.0
