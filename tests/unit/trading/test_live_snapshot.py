# tests/unit/trading/test_live_snapshot.py
from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.server.schemas.trading import TradingReadinessModel
from app.trading.live_snapshot import (
    LiveSnapshotInputs,
    TradingLiveSnapshotBuilder,
)
from app.trading.selections import SelectionStore


@pytest.fixture()
def builder() -> TradingLiveSnapshotBuilder:
    return TradingLiveSnapshotBuilder()


@pytest.fixture()
def selections(tmp_path: Path) -> SelectionStore:
    return SelectionStore.load(tmp_path / "trading_selections.json")


def _decision_pack(*, candidates: list[dict]) -> dict:
    return {"board_date": "2026-05-11", "decisions": candidates}


def _row(candidate_id: str, **overrides) -> dict:
    base = {
        "candidate_id": candidate_id,
        "market_key": candidate_id,
        "recommendation": "buy_yes",
        "model_prob": 0.6,
        "edge_bps": 150,
        "ev": 0.08,
        "line_value": 22.5,
        "player_id": 12345,
        "game_id": 67890,
        "game_date": "2026-05-11",
        "player_name": "Cunningham",
        "market_label": "PTS",
        "kalshi": {"ticker": "KX-CUNN-PTS"},
        "execution": {"allow_live_submit": True},
        "gates": {
            "symbol_resolved": True,
            "fresh_market_snapshot": True,
            "market_open": True,
            "event_not_stale": True,
            "spread_within_limit": True,
            "one_order_cap_ok": True,
            "price_within_limit": True,
        },
        "mode": "live",
    }
    base.update(overrides)
    return base


def _readiness(state: str) -> TradingReadinessModel:
    return TradingReadinessModel(
        observed_at=datetime.now(UTC),
        state=state,
        summary=state,
        live_trading_enabled=True,
        credentials_configured=True,
        account_sync_enabled=True,
        decisions_path="decisions.json",
        symbols_path="symbols.json",
        decision_id="a",
        ticker="KX-CUNN-PTS",
        game_date="2026-05-11",
        market_status="open",
        executable_symbol_count=1,
        unresolved_symbol_count=0,
        checks=[],
    )


def _inputs(
    builder,
    selections,
    decision_pack=None,
    **overrides,
) -> LiveSnapshotInputs:
    defaults = dict(
        decision_pack=decision_pack or {"decisions": []},
        market_book_snapshot={},
        selections=selections,
        board_date=date(2026, 5, 11),
        budget=10.0,
        cap_fraction=0.35,
        loop_state="idle",
        mode="observe",
        ws_connected=True,
        kill_switch_active=False,
        ledger_state=SimpleNamespace(realized=0.0, unrealized=0.0, daily_loss_cap=2.0),
        positions=[],
        fills=[],
        resting_orders=[],
        event_log=[],
        pnl_trend=[],
        readiness=None,
        brain_status=None,
        stream_cursor=0,
        errors=[],
    )
    defaults.update(overrides)
    return LiveSnapshotInputs(**defaults)


def test_empty_decision_pack_yields_empty_picks(builder, selections) -> None:
    inputs = _inputs(builder, selections)
    snapshot = builder.build(inputs)
    assert snapshot.picks == []
    assert snapshot.bet_slip.selected == []
    assert snapshot.kpis.picks.available == 0


def test_selected_picks_get_proportional_allocation(builder, selections) -> None:
    pack = _decision_pack(
        candidates=[
            _row("a", model_prob=0.6),
            _row("b", model_prob=0.4),
        ]
    )
    inputs = _inputs(builder, selections, decision_pack=pack, mode="supervised-live")
    snapshot = builder.build(inputs)
    allocs = {p.candidate_id: p.alloc for p in snapshot.picks}
    assert allocs["a"] > 0
    assert allocs["b"] > 0
    assert allocs["a"] == pytest.approx(3.5, abs=0.01)


def test_excluded_picks_have_zero_alloc(builder, selections) -> None:
    selections.set_selection(date(2026, 5, 11), "a", False)
    pack = _decision_pack(candidates=[_row("a")])
    inputs = _inputs(builder, selections, decision_pack=pack)
    snapshot = builder.build(inputs)
    assert snapshot.picks[0].selected is False
    assert snapshot.picks[0].alloc == 0.0
    assert snapshot.picks[0].state == "excluded"


def test_blocked_pick_cannot_be_selected(builder, selections) -> None:
    row = _row("a")
    row["gates"]["spread_within_limit"] = False
    pack = _decision_pack(candidates=[row])
    inputs = _inputs(builder, selections, decision_pack=pack, mode="supervised-live")
    snapshot = builder.build(inputs)
    pick = snapshot.picks[0]
    assert pick.state == "blocked"
    assert pick.selected is False
    assert pick.alloc == 0.0
    assert pick.blocker_reason is not None
    assert "spread" in pick.blocker_reason.lower()


def test_threshold_force_excludes_picks_below_min_hit(builder, selections) -> None:
    selections.update_thresholds(min_hit_pct=0.55, min_edge_bps=0)
    pack = _decision_pack(
        candidates=[
            _row("a", model_prob=0.50),
            _row("b", model_prob=0.70),
        ]
    )
    inputs = _inputs(builder, selections, decision_pack=pack, mode="supervised-live")
    snapshot = builder.build(inputs)
    picks_by_id = {p.candidate_id: p for p in snapshot.picks}
    assert picks_by_id["a"].selected is False
    assert picks_by_id["a"].state == "excluded"
    assert picks_by_id["b"].selected is True


def test_threshold_force_excludes_picks_below_min_edge(builder, selections) -> None:
    selections.update_thresholds(min_hit_pct=0.0, min_edge_bps=450)
    pack = _decision_pack(
        candidates=[
            _row("a", edge_bps=400),
            _row("b", edge_bps=500),
        ]
    )
    inputs = _inputs(builder, selections, decision_pack=pack, mode="supervised-live")
    snapshot = builder.build(inputs)
    picks_by_id = {p.candidate_id: p for p in snapshot.picks}
    assert picks_by_id["a"].selected is False
    assert picks_by_id["a"].state == "excluded"
    assert picks_by_id["b"].selected is True


def test_control_requires_ready_readiness_for_live_start(builder, selections) -> None:
    pack = _decision_pack(candidates=[_row("a")])
    blocked = _readiness("blocked")
    ready = _readiness("ready")

    blocked_snapshot = builder.build(
        _inputs(
            builder,
            selections,
            decision_pack=pack,
            mode="supervised-live",
            readiness=blocked,
        )
    )
    ready_snapshot = builder.build(
        _inputs(
            builder,
            selections,
            decision_pack=pack,
            mode="supervised-live",
            readiness=ready,
        )
    )

    assert blocked_snapshot.control.can_start is False
    assert blocked_snapshot.kpis.system.status == "blocked"
    assert ready_snapshot.control.can_start is True
    assert ready_snapshot.kpis.system.status == "ready"


def test_observe_rows_stay_visible_but_do_not_allocate_or_start(builder, selections) -> None:
    live_row = _row("live")
    observe_row = _row(
        "observe",
        mode="observe",
        recommendation="observe_only",
        execution={"allow_live_submit": False},
    )
    pack = _decision_pack(candidates=[live_row, observe_row])

    snapshot = builder.build(
        _inputs(
            builder,
            selections,
            decision_pack=pack,
            mode="supervised-live",
            readiness=_readiness("ready"),
        )
    )

    picks = {pick.candidate_id: pick for pick in snapshot.picks}
    assert picks["live"].selected is True
    assert picks["observe"].selected is False
    assert picks["observe"].state == "excluded"
    assert picks["observe"].blocker_reason == "observe-only decision"
    assert snapshot.bet_slip.total_stake == pytest.approx(picks["live"].alloc)
