from __future__ import annotations

import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from app.trading.live_pack_builder import (
    _read_market_profile,
    build_primary_decision_row,
    evaluate_gates_for_row,
    pick_executable_entries,
)
from app.trading.monitoring import QuoteSnapshot
from app.trading.risk import RiskLimits

_PROFILES_SUBPATH = "05 Knowledge and Skills/Data Analysis/NBA Prop Engine Learning/Market Profiles"


def _write_profile(vault_root: Path, market_key: str, body: str) -> None:
    profile_dir = vault_root / _PROFILES_SUBPATH
    profile_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / f"{market_key.title()} Profile.md").write_text(body, encoding="utf-8")


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


def test_selected_observe_row_can_pack_but_never_live_arms() -> None:
    row = {
        "target_id": "target",
        "market_key": "points",
        "recommendation": "observe_only",
        "original_recommendation": "buy_yes",
        "candidate_status": "selected_observe_only",
        "kalshi_ticker": "KX-1",
        "line_value": 25.5,
        "player_id": "237",
        "game_date": "2026-05-07",
    }

    picked = pick_executable_entries([row])
    assert picked == [row]

    decision = build_primary_decision_row(
        row,
        defaults={},
        gate_result=evaluate_gates_for_row(
            row,
            limits=RiskLimits(per_order_cap=1.0),
            risk={"contracts": "1.00", "max_price_dollars": "0.99"},
            quote=_quote(entry=0.5, spread=0.02),
            max_spread=0.2,
            today=date(2026, 5, 7),
        ),
        arm_live=True,
    )
    assert decision["mode"] == "observe"
    assert decision["recommendation"] == "observe_only"
    assert decision["execution"]["allow_live_submit"] is False


# --- _read_market_profile tests ---

_GOOD_PROFILE = """\
## Metadata
- **Calibration strategy**: isotonic regression
- **Corrections applied**: 4
- **Average ECE improvement**: 0.012

## ECE History
- 2026-04-01: 0.082
- 2026-05-01: 0.063

## Known Failure Modes
- Back-to-back games
- Blowout garbage time
"""


def test_read_market_profile_happy_path() -> None:
    with tempfile.TemporaryDirectory() as td:
        vault = Path(td)
        _write_profile(vault, "points", _GOOD_PROFILE)
        result = _read_market_profile("points", vault)
    assert result is not None
    assert result["calibration_strategy"] == "isotonic regression"
    assert result["corrections_applied"] == 4
    assert result["recent_ece"] == pytest.approx(0.063)
    assert "Back-to-back games" in result["failure_modes"]


def test_read_market_profile_missing_file() -> None:
    with tempfile.TemporaryDirectory() as td:
        assert _read_market_profile("assists", Path(td)) is None


def test_read_market_profile_no_parseable_content() -> None:
    with tempfile.TemporaryDirectory() as td:
        vault = Path(td)
        _write_profile(vault, "rebounds", "# empty\nno structured content here\n")
        assert _read_market_profile("rebounds", vault) is None


# --- brain_context wiring in build_primary_decision_row ---

def _base_row() -> dict:
    return {
        "target_id": "t1",
        "market_key": "points",
        "recommendation": "buy_yes",
        "kalshi_ticker": "KX-1",
        "line_value": 25.5,
        "player_id": "237",
        "game_date": "2026-05-10",
    }


def _base_gate_result(today: date = date(2026, 5, 10)):
    return evaluate_gates_for_row(
        _base_row(),
        limits=RiskLimits(per_order_cap=1.0),
        risk={"contracts": "1.00", "max_price_dollars": "0.99"},
        quote=_quote(entry=0.5, spread=0.02),
        max_spread=0.2,
        today=today,
    )


def test_brain_block_populated_in_output() -> None:
    ctx = {"market": "points", "calibration_strategy": "isotonic", "recent_ece": 0.05}
    d = build_primary_decision_row(
        _base_row(), defaults={}, gate_result=_base_gate_result(), arm_live=False, brain_context=ctx
    )
    assert d["brain"]["calibration_strategy"] == "isotonic"
    assert "market" not in d["brain"]


def test_brain_block_empty_when_no_context() -> None:
    d = build_primary_decision_row(
        _base_row(), defaults={}, gate_result=_base_gate_result(), arm_live=False, brain_context=None
    )
    assert d["brain"] == {}


def test_brain_health_ok_no_threshold() -> None:
    ctx = {"market": "points", "recent_ece": 0.99}
    d = build_primary_decision_row(
        _base_row(), defaults={}, gate_result=_base_gate_result(), arm_live=True,
        brain_context=ctx, ece_threshold=None,
    )
    assert d["gates"]["brain_health_ok"] is True
    assert d["mode"] == "live"


def test_brain_health_ok_under_threshold() -> None:
    ctx = {"market": "points", "recent_ece": 0.05}
    d = build_primary_decision_row(
        _base_row(), defaults={}, gate_result=_base_gate_result(), arm_live=True,
        brain_context=ctx, ece_threshold=0.10,
    )
    assert d["gates"]["brain_health_ok"] is True
    assert d["mode"] == "live"


def test_brain_health_fail_blocks_live() -> None:
    ctx = {"market": "points", "recent_ece": 0.20}
    d = build_primary_decision_row(
        _base_row(), defaults={}, gate_result=_base_gate_result(), arm_live=True,
        brain_context=ctx, ece_threshold=0.10,
    )
    assert d["gates"]["brain_health_ok"] is False
    assert d["mode"] == "observe"
    assert d["execution"]["allow_live_submit"] is False


def test_brain_health_fail_open_when_context_none() -> None:
    d = build_primary_decision_row(
        _base_row(), defaults={}, gate_result=_base_gate_result(), arm_live=True,
        brain_context=None, ece_threshold=0.10,
    )
    assert d["gates"]["brain_health_ok"] is True
    assert d["mode"] == "live"
