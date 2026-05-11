# app/trading/live_snapshot.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from app.server.schemas.trading import (
    BetSlipModel,
    BetSlipPickModel,
    ControlBarStateModel,
    EventLogLineModel,
    KpiBudgetModel,
    KpiPicksModel,
    KpiPnlModel,
    KpiSystemModel,
    KpiTilesModel,
    PickKalshiModel,
    PickRowModel,
    PnlPointModel,
    SystemDiagnosticsModel,
    TradingLiveSnapshotModel,
)
from app.trading.allocation import AllocationPick, allocate_proportional_with_soft_cap
from app.trading.selections import SelectionStore

_GATE_FIELDS = (
    "symbol_resolved",
    "fresh_market_snapshot",
    "market_open",
    "event_not_stale",
    "spread_within_limit",
    "one_order_cap_ok",
    "price_within_limit",
)

_GATE_REASONS = {
    "symbol_resolved": "Kalshi symbol could not be resolved",
    "fresh_market_snapshot": "Market quote is stale",
    "market_open": "Kalshi market is closed",
    "event_not_stale": "Underlying event has started or ended",
    "spread_within_limit": "Spread too wide",
    "one_order_cap_ok": "One-order cap reached",
    "price_within_limit": "Quoted price exceeds policy max",
}


@dataclass
class LiveSnapshotInputs:
    decision_pack: dict[str, Any]
    market_book_snapshot: dict[str, dict[str, Any]]
    selections: SelectionStore
    board_date: date
    budget: float
    cap_fraction: float
    loop_state: str
    mode: str
    ws_connected: bool
    kill_switch_active: bool
    ledger_state: Any
    positions: list[Any]
    fills: list[Any]
    resting_orders: list[Any]
    event_log: list[EventLogLineModel]
    pnl_trend: list[PnlPointModel]
    readiness: Any | None
    brain_status: Any | None
    stream_cursor: int
    errors: list[str]


class TradingLiveSnapshotBuilder:
    """Pure builder. Given a snapshot of state, returns a TradingLiveSnapshotModel."""

    def build(self, inputs: LiveSnapshotInputs) -> TradingLiveSnapshotModel:
        rows = self._enrich_rows(inputs)
        bet_slip = self._build_bet_slip(rows, inputs.budget)
        kpis = self._build_kpis(rows, bet_slip, inputs)
        control = self._build_control(inputs, bet_slip)
        diagnostics = SystemDiagnosticsModel(
            readiness=inputs.readiness,
            brain=inputs.brain_status,
        )
        return TradingLiveSnapshotModel(
            observed_at=datetime.now(UTC),
            kpis=kpis,
            control=control,
            picks=rows,
            bet_slip=bet_slip,
            positions=inputs.positions,
            fills=inputs.fills,
            quotes=[],
            resting_orders=inputs.resting_orders,
            diagnostics=diagnostics,
            event_log=inputs.event_log,
            pnl_trend=inputs.pnl_trend,
            errors=inputs.errors,
            stream_cursor=inputs.stream_cursor,
        )

    def _enrich_rows(self, inputs: LiveSnapshotInputs) -> list[PickRowModel]:
        raw_rows = inputs.decision_pack.get("decisions") or []
        thresholds = inputs.selections.thresholds
        # First pass: build rows without allocation
        pre_rows: list[tuple[dict[str, Any], str, str, bool]] = []
        for _rank, raw in enumerate(raw_rows):
            candidate_id = str(
                raw.get("candidate_id")
                or raw.get("decision_id")
                or raw.get("market_key")
                or ""
            )
            if not candidate_id:
                continue
            gates = raw.get("gates") or {}
            blocker_reason = self._gate_blocker(gates, raw.get("mode"))
            below_thresholds = float(raw.get("model_prob") or 0.0) < thresholds.min_hit_pct
            user_selected = inputs.selections.is_selected(inputs.board_date, candidate_id)
            if blocker_reason is not None:
                state: str = "blocked"
                selected = False
            elif not user_selected or below_thresholds:
                state = "excluded"
                selected = False
            else:
                state = "queued"
                selected = True
            pre_rows.append((raw, candidate_id, state, selected))

        # Compute allocations for selected rows
        selected_picks = [
            AllocationPick(
                candidate_id=candidate_id,
                model_prob=float(raw.get("model_prob") or 0.0),
            )
            for raw, candidate_id, state, selected in pre_rows
            if selected
        ]
        stakes = allocate_proportional_with_soft_cap(
            selected_picks, budget=inputs.budget, cap_fraction=inputs.cap_fraction
        )

        # Second pass: build PickRowModel with allocations
        rows: list[PickRowModel] = []
        for rank, (raw, candidate_id, state, selected) in enumerate(pre_rows):
            blocker_reason = self._gate_blocker(raw.get("gates") or {}, raw.get("mode"))
            ticker = (raw.get("kalshi") or {}).get("ticker")
            book_entry = inputs.market_book_snapshot.get(ticker) if ticker else None
            kalshi = PickKalshiModel(
                ticker=ticker,
                yes_bid=book_entry.get("yes_bid") if book_entry else None,
                yes_ask=book_entry.get("yes_ask") if book_entry else None,
                spread=book_entry.get("spread") if book_entry else None,
                last_quote_at=book_entry.get("ts") if book_entry else None,
            )
            stake = stakes.get(candidate_id, 0.0) if selected else 0.0
            model_prob = float(raw.get("model_prob") or 0.0)
            market_prob = raw.get("market_prob")
            payout = self._payout(kalshi, market_prob)
            est = self._estimated_profit(model_prob, stake, payout)
            rows.append(
                PickRowModel(
                    candidate_id=candidate_id,
                    rank=rank,
                    prop_label=self._prop_label(raw),
                    game_label=raw.get("game_label"),
                    hit_pct=model_prob,
                    edge_bps=int(raw.get("edge_bps") or 0),
                    model_prob=model_prob,
                    market_prob=market_prob,
                    alloc=round(stake, 4),
                    est_profit=round(est, 4),
                    state=state,  # type: ignore[arg-type]
                    selected=selected,
                    blocker_reason=blocker_reason,
                    kalshi=kalshi,
                )
            )
        return rows

    def _gate_blocker(self, gates: dict[str, Any], mode: Any) -> str | None:
        if str(mode or "").lower() != "live":
            return None
        for field in _GATE_FIELDS:
            if gates.get(field) is not True:
                return _GATE_REASONS.get(field, f"gate {field} failed")
        return None

    def _prop_label(self, raw: dict[str, Any]) -> str:
        title = raw.get("market_label") or raw.get("title")
        if title and (raw.get("player_name") or "?") in str(title):
            return str(title)
        player = raw.get("player_name") or raw.get("player_id") or "?"
        market = title or raw.get("market_key") or "?"
        line = raw.get("line_value")
        side_raw = str(raw.get("side") or raw.get("recommendation") or "").lower()
        side = "o" if ("over" in side_raw or side_raw.startswith("buy_yes")) else "u"
        if line is not None:
            return f"{player} {market} {side}{line}"
        return f"{player} {market}"

    def _payout(self, kalshi: PickKalshiModel, market_prob: float | None) -> float:
        entry = kalshi.yes_ask or market_prob or 0.5
        return max(0.0, 1.0 - entry)

    def _estimated_profit(self, prob: float, stake: float, payout_per_unit: float) -> float:
        if stake <= 0:
            return 0.0
        win = prob * (stake * payout_per_unit / max(0.01, 1.0 - payout_per_unit))
        lose = (1 - prob) * stake
        return win - lose

    def _build_bet_slip(self, rows: list[PickRowModel], cap_total: float) -> BetSlipModel:
        selected = [
            BetSlipPickModel(
                candidate_id=row.candidate_id,
                prop_label=row.prop_label,
                hit_pct=row.hit_pct,
                edge_bps=row.edge_bps,
                alloc=row.alloc,
                est_profit=row.est_profit,
            )
            for row in rows
            if row.selected
        ]
        total_stake = round(sum(p.alloc for p in selected), 4)
        est_total = round(sum(p.est_profit for p in selected), 4)
        return BetSlipModel(
            selected=selected,
            total_stake=total_stake,
            cap_total=cap_total,
            est_total_profit=est_total,
            unused_budget=round(max(cap_total - total_stake, 0.0), 4),
        )

    def _build_kpis(
        self,
        rows: list[PickRowModel],
        bet_slip: BetSlipModel,
        inputs: LiveSnapshotInputs,
    ) -> KpiTilesModel:
        ledger = inputs.ledger_state
        realized = float(getattr(ledger, "realized", 0.0))
        unrealized = float(getattr(ledger, "unrealized", 0.0))
        loss_cap = float(getattr(ledger, "daily_loss_cap", 0.0))
        daily = realized + unrealized
        loss_progress = (
            min(max(abs(min(daily, 0.0)) / loss_cap, 0.0), 1.0) if loss_cap > 0 else 0.0
        )
        excluded = sum(1 for r in rows if r.state == "excluded")
        blocked = sum(1 for r in rows if r.state == "blocked")
        gates_total = 0
        gates_passed = 0
        if inputs.readiness is not None:
            checks = getattr(inputs.readiness, "checks", []) or []
            gates_total = len(checks)
            gates_passed = sum(1 for c in checks if getattr(c, "status", "") == "pass")
        return KpiTilesModel(
            pnl=KpiPnlModel(
                daily_pnl=round(daily, 4),
                realized=round(realized, 4),
                unrealized=round(unrealized, 4),
                loss_cap=loss_cap,
                loss_progress=loss_progress,
            ),
            budget=KpiBudgetModel(
                max_open_notional=inputs.budget,
                allocated=bet_slip.total_stake,
                free=bet_slip.unused_budget,
                usage_progress=(bet_slip.total_stake / inputs.budget) if inputs.budget > 0 else 0.0,
            ),
            picks=KpiPicksModel(
                available=len(rows),
                selected=len(bet_slip.selected),
                excluded=excluded,
                blocked=blocked,
                est_total_profit=bet_slip.est_total_profit,
            ),
            system=KpiSystemModel(
                status="ready" if gates_total == 0 or gates_passed == gates_total else "blocked",
                mode=inputs.mode,  # type: ignore[arg-type]
                gates_passed=gates_passed,
                gates_total=gates_total,
                ws_connected=inputs.ws_connected,
                summary=f"{inputs.mode} · {gates_passed}/{gates_total} gates · ws {'ok' if inputs.ws_connected else 'down'}",
            ),
        )

    def _build_control(
        self, inputs: LiveSnapshotInputs, bet_slip: BetSlipModel
    ) -> ControlBarStateModel:
        can_start = (
            inputs.mode == "supervised-live"
            and inputs.loop_state in {"idle", "exited", "killed", "failed"}
            and len(bet_slip.selected) > 0
            and not inputs.kill_switch_active
        )
        start_label = (
            f"Start Auto-Bet ({len(bet_slip.selected)} picks · ${bet_slip.total_stake:.2f})"
            if can_start
            else "Start Auto-Bet"
        )
        return ControlBarStateModel(
            mode=inputs.mode,  # type: ignore[arg-type]
            loop_state=inputs.loop_state,  # type: ignore[arg-type]
            can_start=can_start,
            start_label=start_label,
            kill_switch_active=inputs.kill_switch_active,
        )
