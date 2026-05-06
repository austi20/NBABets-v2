from __future__ import annotations

import json
from pathlib import Path

from app.evaluation.prop_decision import PropDecision
from app.trading.ledger import InMemoryPortfolioLedger
from app.trading.loop import TradingLoop
from app.trading.paper_adapter import FakePaperAdapter
from app.trading.risk import ExposureRiskEngine, RiskLimits


def test_paper_loop_runs_ten_decisions_from_fixture() -> None:
    fixture_path = Path("tests/fixtures/sample_decisions.json")
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    assert len(payload) == 10

    decisions = [
        PropDecision(
            model_prob=float(row["model_prob"]),
            market_prob=float(row["market_prob"]),
            no_vig_market_prob=float(row["no_vig_market_prob"]),
            ev=float(row["ev"]),
            recommendation=str(row["recommendation"]),
            confidence=str(row["confidence"]),
            driver=str(row["driver"]),
            market_key=str(row["market_key"]),
            line_value=float(row["line_value"]),
            over_odds=int(row["over_odds"]) if row.get("over_odds") is not None else None,
            under_odds=int(row["under_odds"]) if row.get("under_odds") is not None else None,
        )
        for row in payload
        if isinstance(row, dict)
    ]
    ledger = InMemoryPortfolioLedger()
    risk = ExposureRiskEngine(
        RiskLimits(
            per_order_cap=50.0,
            per_market_cap=200.0,
            max_open_notional=1000.0,
            daily_loss_cap=500.0,
            reject_cooldown_seconds=0,
        )
    )
    loop = TradingLoop(
        risk_engine=risk,
        ledger=ledger,
        adapter=FakePaperAdapter(),
    )
    result = loop.run_decisions(decisions, exchange="paper", stake=10.0)

    assert result.accepted == 10
    assert result.rejected == 0
    assert result.fills == 10
    assert len(ledger.open_positions()) > 0
