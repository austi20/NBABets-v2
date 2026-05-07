from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Boolean, CheckConstraint, Date, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class TradingOrder(Base):
    __tablename__ = "trading_orders"

    intent_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    kalshi_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    market_symbol: Mapped[str] = mapped_column(String(255), index=True)
    market_key: Mapped[str] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(16))
    stake: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(32))
    message: Mapped[str] = mapped_column(String(512), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TradingFill(Base):
    __tablename__ = "trading_fills"

    fill_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    intent_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("trading_orders.intent_id"), index=True
    )
    market_symbol: Mapped[str] = mapped_column(String(255), index=True)
    market_key: Mapped[str] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(16))
    stake: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)
    fee: Mapped[float] = mapped_column(Float, default=0.0)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    kalshi_trade_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TradingPosition(Base):
    __tablename__ = "trading_positions"

    market_symbol: Mapped[str] = mapped_column(String(255), primary_key=True)
    market_key: Mapped[str] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(16))
    open_stake: Mapped[float] = mapped_column(Float, default=0.0)
    weighted_price_total: Mapped[float] = mapped_column(Float, default=0.0)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TradingKillSwitch(Base):
    __tablename__ = "trading_kill_switch"
    __table_args__ = (CheckConstraint("id = 1", name="ck_kill_switch_singleton"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    killed: Mapped[bool] = mapped_column(Boolean, default=False)
    set_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    set_by: Mapped[str] = mapped_column(String(64), default="")


class TradingDailyPnL(Base):
    __tablename__ = "trading_daily_pnl"

    date: Mapped[date] = mapped_column(Date, primary_key=True)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
