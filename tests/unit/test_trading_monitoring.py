from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from app.trading.ledger import InMemoryPortfolioLedger
from app.trading.monitoring import MonitoredSymbol, build_monitor_snapshot, load_monitored_symbols
from app.trading.risk import RiskLimits
from app.trading.types import Fill, MarketRef


class _FakeMarketClient:
    def get_market(self, ticker: str) -> dict[str, Any]:
        assert ticker == "KXNBAAST-26MAY07LALOKC-OKCSGILGEOUSALEXANDER2-8"
        return {
            "market": {
                "ticker": ticker,
                "title": "Shai Gilgeous-Alexander: 8+ assists",
                "status": "open",
                "yes_bid_dollars": "0.4000",
                "yes_ask_dollars": "0.4200",
                "no_ask_dollars": "0.6000",
                "last_price_dollars": "0.4100",
            }
        }


class _FakeAccountClient:
    def get_positions(
        self,
        *,
        ticker: str | None = None,
        event_ticker: str | None = None,
        count_filter: str | None = "position,total_traded",
        limit: int = 100,
    ) -> dict[str, Any]:
        return {
            "market_positions": [
                {
                    "ticker": "KXNBAAST-26MAY07LALOKC-OKCSGILGEOUSALEXANDER2-8",
                    "position_fp": "-1.00",
                    "market_exposure_dollars": "0.6000",
                    "fees_paid_dollars": "0.0100",
                    "realized_pnl_dollars": "0.0000",
                    "last_updated_ts": "2026-05-07T23:00:00Z",
                }
            ]
        }

    def get_orders(
        self,
        *,
        ticker: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        return {
            "orders": [
                {
                    "order_id": "ord1",
                    "client_order_id": "client1",
                    "ticker": "KXNBAAST-26MAY07LALOKC-OKCSGILGEOUSALEXANDER2-8",
                    "book_side": "bid",
                    "status": "resting",
                    "remaining_count_fp": "1.00",
                    "price_dollars": "0.3500",
                    "created_time": "2026-05-07T23:00:01Z",
                }
            ]
        }


def test_load_monitored_symbols_ignores_unresolved(tmp_path: Path) -> None:
    path = tmp_path / "symbols.json"
    path.write_text(
        json.dumps(
            {
                "symbols": [
                    {
                        "market_key": "nba.player.assists",
                        "recommendation": "buy_no",
                        "line_value": 7.5,
                        "player_id": 270,
                        "game_date": "2026-05-07",
                        "kalshi_ticker": "KXNBAAST-26MAY07LALOKC-OKCSGILGEOUSALEXANDER2-8",
                    }
                ],
                "unresolved": [{"target_id": "still-safe"}],
            }
        ),
        encoding="utf-8",
    )

    symbols = load_monitored_symbols(path)

    assert len(symbols) == 1
    assert symbols[0].side == "UNDER"
    assert symbols[0].player_id == "270"


def test_monitor_snapshot_marks_configured_under_position() -> None:
    from app.trading.monitoring import MonitoredSymbol

    ledger = InMemoryPortfolioLedger()
    ledger.record_fill(
        Fill(
            fill_id="f1",
            intent_id="i1",
            market=MarketRef(
                exchange="kalshi",
                symbol="kalshi:assists:under:7.5:g823:p270",
                market_key="assists",
                side="UNDER",
                line_value=7.5,
            ),
            side="buy",
            stake=0.60,
            price=0.60,
            timestamp=datetime.now(UTC),
        )
    )

    snapshot = build_monitor_snapshot(
        ledger=ledger,
        limits=RiskLimits(max_open_notional=2.0, daily_loss_cap=10.0),
        kill_switch_active=False,
        monitored_symbols=[
            MonitoredSymbol(
                ticker="KXNBAAST-26MAY07LALOKC-OKCSGILGEOUSALEXANDER2-8",
                market_key="nba.player.assists",
                side="UNDER",
                line_value=7.5,
                player_id="270",
                game_date="2026-05-07",
            )
        ],
        market_client=_FakeMarketClient(),
    )

    assert snapshot.daily_unrealized_pnl == pytest.approx(-0.02)
    assert snapshot.total_daily_pnl == pytest.approx(-0.02)
    assert snapshot.budget_used == pytest.approx(0.60)
    assert snapshot.budget_remaining == pytest.approx(1.40)
    assert snapshot.positions[0].ticker == "KXNBAAST-26MAY07LALOKC-OKCSGILGEOUSALEXANDER2-8"
    assert snapshot.positions[0].current_exit_price == pytest.approx(0.58)


class _FakeAccountClientMarketTickerAlias(_FakeAccountClient):
    def get_positions(
        self,
        *,
        ticker: str | None = None,
        event_ticker: str | None = None,
        count_filter: str | None = "position,total_traded",
        limit: int = 100,
    ) -> dict[str, Any]:
        base = super().get_positions(
            ticker=ticker,
            event_ticker=event_ticker,
            count_filter=count_filter,
            limit=limit,
        )
        row = dict(base["market_positions"][0])
        row.pop("ticker", None)
        row["market_ticker"] = "KXNBAAST-26MAY07LALOKC-OKCSGILGEOUSALEXANDER2-8"
        return {"market_positions": [row]}


def test_monitor_snapshot_includes_authenticated_account_state() -> None:
    snapshot = build_monitor_snapshot(
        ledger=InMemoryPortfolioLedger(),
        limits=RiskLimits(max_open_notional=2.0, daily_loss_cap=10.0),
        kill_switch_active=False,
        monitored_symbols=[
            MonitoredSymbol(
                ticker="KXNBAAST-26MAY07LALOKC-OKCSGILGEOUSALEXANDER2-8",
                market_key="nba.player.assists",
                side="UNDER",
                line_value=7.5,
                player_id="270",
                game_date="2026-05-07",
            )
        ],
        market_client=_FakeMarketClient(),
        account_client=_FakeAccountClient(),
    )

    assert snapshot.account_positions[0].side == "NO"
    assert snapshot.account_positions[0].contract_count == pytest.approx(1.0)
    assert snapshot.account_positions[0].current_exit_price == pytest.approx(0.58)
    assert snapshot.resting_orders[0].order_id == "ord1"
    assert snapshot.resting_orders[0].price == pytest.approx(0.35)


def test_monitor_snapshot_normalizes_market_ticker_field() -> None:
    snapshot = build_monitor_snapshot(
        ledger=InMemoryPortfolioLedger(),
        limits=RiskLimits(max_open_notional=2.0, daily_loss_cap=10.0),
        kill_switch_active=False,
        monitored_symbols=[
            MonitoredSymbol(
                ticker="KXNBAAST-26MAY07LALOKC-OKCSGILGEOUSALEXANDER2-8",
                market_key="nba.player.assists",
                side="UNDER",
                line_value=7.5,
                player_id="270",
                game_date="2026-05-07",
            )
        ],
        market_client=_FakeMarketClient(),
        account_client=_FakeAccountClientMarketTickerAlias(),
    )
    assert len(snapshot.account_positions) == 1
    assert snapshot.account_positions[0].ticker == "KXNBAAST-26MAY07LALOKC-OKCSGILGEOUSALEXANDER2-8"
