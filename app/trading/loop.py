from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy.orm import Session

from app.db.models.trading import TradingKillSwitch
from app.evaluation.prop_decision import PropDecision
from app.trading.ledger import InMemoryPortfolioLedger
from app.trading.mapper import signal_to_market_ref
from app.trading.paper_adapter import FakePaperAdapter, RealisticPaperAdapter
from app.trading.pricing import american_to_prob, no_vig_over_probability
from app.trading.protocols import ExchangeAdapter, PortfolioLedger, RiskEngine
from app.trading.risk import ExposureRiskEngine
from app.trading.types import ExecutionIntent, MarketRef, OrderEvent, Signal


@dataclass(frozen=True)
class LoopRunResult:
    accepted: int
    rejected: int
    fills: int
    events: int


class TradingLoop:
    def __init__(
        self,
        *,
        risk_engine: RiskEngine,
        ledger: PortfolioLedger,
        adapter: ExchangeAdapter,
        market_mapper: Callable[[Signal, str], MarketRef] = signal_to_market_ref,
        session_factory: Callable[[], Session] | None = None,
    ) -> None:
        self._risk = risk_engine
        self._ledger = ledger
        self._adapter = adapter
        self._market_mapper = market_mapper
        self._sequence = 0
        self._session_factory = session_factory

    def _kill_switch_active(self) -> bool:
        if self._session_factory is None:
            return False
        with self._session_factory() as session:
            row = session.get(TradingKillSwitch, 1)
            return bool(row and row.killed)

    def run_signals(
        self,
        signals: list[Signal],
        *,
        exchange: str = "paper",
        stake: float = 10.0,
    ) -> LoopRunResult:
        accepted = 0
        rejected = 0
        fill_count = 0
        event_count = 0
        for signal in signals:
            if self._kill_switch_active():
                rejected += 1
                event = OrderEvent(
                    intent_id=f"{signal.signal_id}-intent",
                    event_type="rejected",
                    status="blocked",
                    message="kill switch active",
                    timestamp=datetime.now(UTC),
                )
                self._ledger.record_order_event(event)
                event_count += 1
                continue
            market_ref = self._market_mapper(signal, exchange)
            intent = ExecutionIntent(
                intent_id=f"{signal.signal_id}-intent",
                signal=signal,
                market=market_ref,
                side="buy",
                stake=float(stake),
            )
            ok, reason = self._risk.evaluate(intent, self._ledger)
            if not ok:
                rejected += 1
                event = OrderEvent(
                    intent_id=intent.intent_id,
                    event_type="rejected",
                    status="blocked",
                    message=reason,
                    timestamp=datetime.now(UTC),
                )
                self._ledger.record_order_event(event)
                event_count += 1
                continue
            accepted += 1
            events, fills = self._adapter.place_order(intent)
            for event in events:
                self._ledger.record_order_event(event)
                event_count += 1
            for fill in fills:
                self._ledger.record_fill(fill)
                fill_count += 1
        return LoopRunResult(
            accepted=accepted,
            rejected=rejected,
            fills=fill_count,
            events=event_count,
        )

    def run_decisions(
        self,
        decisions: list[PropDecision],
        *,
        exchange: str = "paper",
        stake: float = 10.0,
    ) -> LoopRunResult:
        signals = [self._decision_to_signal(decision) for decision in decisions]
        return self.run_signals(signals, exchange=exchange, stake=stake)

    def _decision_to_signal(self, decision: PropDecision) -> Signal:
        self._sequence += 1
        signal_id = f"decision-{self._sequence}"
        metadata: dict[str, object] = {
            "signal_id": signal_id,
            "market_prob": float(
                decision.market_prob
                if decision.market_prob > 0
                else american_to_prob(decision.over_odds if decision.recommendation.upper() == "OVER" else decision.under_odds)
            ),
            "no_vig_market_prob": float(
                decision.no_vig_market_prob
                if decision.no_vig_market_prob > 0
                else no_vig_over_probability(decision.over_odds, decision.under_odds)
            ),
            "driver": decision.driver,
        }
        if decision.game_id is not None:
            metadata["game_id"] = decision.game_id
        if decision.player_id is not None:
            metadata["player_id"] = int(decision.player_id)
        if decision.game_date is not None:
            metadata["game_date"] = (
                decision.game_date.isoformat()
                if hasattr(decision.game_date, "isoformat")
                else str(decision.game_date)
            )
        return Signal(
            signal_id=signal_id,
            created_at=datetime.now(UTC),
            market_key=decision.market_key,
            side=decision.recommendation.upper(),
            confidence=decision.confidence.lower(),
            edge=float(decision.ev),
            model_probability=float(decision.model_prob),
            line_value=float(decision.line_value),
            metadata=metadata,
        )


def set_kill_switch(
    session_factory: Callable[[], Session],
    *,
    killed: bool,
    set_by: str = "system",
) -> None:
    with session_factory() as session:
        row = session.get(TradingKillSwitch, 1)
        now = datetime.now(UTC)
        if row is None:
            row = TradingKillSwitch(id=1, killed=killed, set_at=now, set_by=set_by)
            session.add(row)
        else:
            row.killed = killed
            row.set_at = now
            row.set_by = set_by
        session.commit()


def _load_decisions(path: Path) -> list[PropDecision]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("decisions payload must be a list")
    decisions: list[PropDecision] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        decisions.append(
            PropDecision(
                model_prob=float(row.get("model_prob", 0.5)),
                market_prob=float(row.get("market_prob", 0.5)),
                no_vig_market_prob=float(row.get("no_vig_market_prob", 0.5)),
                ev=float(row.get("ev", 0.0)),
                recommendation=str(row.get("recommendation", "OVER")).upper(),
                confidence=str(row.get("confidence", "watch")).lower(),
                driver=str(row.get("driver", "synthetic")),
                market_key=str(row.get("market_key", "points")),
                line_value=float(row.get("line_value", 0.0)),
                over_odds=(int(row["over_odds"]) if row.get("over_odds") is not None else None),
                under_odds=(int(row["under_odds"]) if row.get("under_odds") is not None else None),
                player_id=(int(row["player_id"]) if row.get("player_id") is not None else None),
                game_id=row.get("game_id"),
                game_date=(str(row["game_date"]) if row.get("game_date") is not None else None),
            )
        )
    return decisions


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run paper trading loop over decision JSON payload.")
    parser.add_argument("--paper", action="store_true", help="Use deterministic paper adapter.")
    parser.add_argument("--realistic-paper", action="store_true", help="Use latency/slippage paper adapter.")
    parser.add_argument("--decisions", required=True, help="Path to a JSON list of PropDecision-like objects.")
    parser.add_argument("--stake", type=float, default=10.0, help="Fixed stake per accepted signal.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    decisions = _load_decisions(Path(args.decisions))
    ledger = InMemoryPortfolioLedger()
    risk = ExposureRiskEngine()
    adapter: ExchangeAdapter
    if args.realistic_paper:
        adapter = RealisticPaperAdapter()
    else:
        adapter = FakePaperAdapter()
    loop = TradingLoop(risk_engine=risk, ledger=ledger, adapter=adapter)
    result = loop.run_decisions(decisions, exchange="paper", stake=max(float(args.stake), 0.0))
    print(
        "paper-loop "
        f"accepted={result.accepted} rejected={result.rejected} "
        f"fills={result.fills} events={result.events} open_positions={len(ledger.open_positions())}"
    )


if __name__ == "__main__":
    main()
