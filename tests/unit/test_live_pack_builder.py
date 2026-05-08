from __future__ import annotations

from datetime import UTC, date, datetime

from app.trading.live_pack_builder import evaluate_gates_for_row, pick_executable_entries
from app.trading.monitoring import QuoteSnapshot
from app.trading.risk import RiskLimits


def _quote(
    *,
    entry: float | None,
    spread: float | None,
    err: str | None = None,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        ticker="T",
        market_key="m",
        side="OVER",
        line_value=1.0,
        player_id="p",
        game_date="2026-05-07",
        title=None,
        status="open",
        yes_bid=0.4,
        yes_ask=0.5,
        no_bid=0.5,
        no_ask=0.6,
        last_price=None,
        entry_price=entry,
        exit_price=0.4,
        spread=spread,
        observed_at=datetime.now(UTC),
        error=err,
    )


def test_evaluate_gates_all_pass() -> None:
    row = {"kalshi_ticker": "KX-1", "game_date": "2026-05-07"}
    limits = RiskLimits(per_order_cap=1.0)
    risk = {"contracts": "1.00", "max_price_dollars": "0.99"}
    q = _quote(entry=0.5, spread=0.05)
    g = evaluate_gates_for_row(row, limits=limits, risk=risk, quote=q, max_spread=0.2, today=date(2026, 5, 7))
    assert all(g.gates.values())


def test_evaluate_gates_spread_fail() -> None:
    row = {"kalshi_ticker": "KX-1", "game_date": "2026-05-07"}
    limits = RiskLimits(per_order_cap=1.0)
    risk = {"contracts": "1.00", "max_price_dollars": "0.99"}
    q = _quote(entry=0.5, spread=0.5)
    g = evaluate_gates_for_row(row, limits=limits, risk=risk, quote=q, max_spread=0.2, today=date(2026, 5, 7))
    assert g.gates["spread_within_limit"] is False


def test_evaluate_gates_price_fail() -> None:
    row = {"kalshi_ticker": "KX-1", "game_date": "2026-05-07"}
    limits = RiskLimits(per_order_cap=1.0)
    risk = {"contracts": "1.00", "max_price_dollars": "0.40"}
    q = _quote(entry=0.5, spread=0.02)
    g = evaluate_gates_for_row(row, limits=limits, risk=risk, quote=q, max_spread=0.2, today=date(2026, 5, 7))
    assert g.gates["price_within_limit"] is False


def test_evaluate_gates_blocks_finalized_or_stale_market() -> None:
    row = {"kalshi_ticker": "KX-1", "game_date": "2026-05-07"}
    limits = RiskLimits(per_order_cap=1.0)
    risk = {"contracts": "1.00", "max_price_dollars": "0.99"}
    q = QuoteSnapshot(
        **{
            **_quote(entry=0.5, spread=0.02).__dict__,
            "status": "finalized",
        }
    )

    g = evaluate_gates_for_row(row, limits=limits, risk=risk, quote=q, max_spread=0.2, today=date(2026, 5, 8))

    assert g.gates["market_open"] is False
    assert g.gates["event_not_stale"] is False


def test_pick_executable_skips_observe() -> None:
    rows = [
        {"recommendation": "observe_only", "kalshi_ticker": "A", "line_value": 1.0},
        {"recommendation": "buy_yes", "kalshi_ticker": "B", "line_value": 2.0},
    ]
    picked = pick_executable_entries(rows)
    assert len(picked) == 1
    assert picked[0]["kalshi_ticker"] == "B"
