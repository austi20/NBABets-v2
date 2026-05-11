from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

from app.config.settings import Settings
from app.trading.decision_brain import TradingBrainSyncResult
from app.trading.loop_controller import TradingLoopController


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        APP_DATA_DIR=str(tmp_path / "app"),
        LOGS_DIR=str(tmp_path / "logs"),
        SNAPSHOT_DIR=str(tmp_path / "snapshots"),
        KALSHI_DECISIONS_PATH=str(tmp_path / "decisions.json"),
        KALSHI_SYMBOLS_PATH=str(tmp_path / "symbols.json"),
        KALSHI_RESOLUTION_TARGETS_PATH=str(tmp_path / "targets.json"),
        TRADING_LIMITS_PATH=str(tmp_path / "limits.json"),
    )


def _brain_result(*, state: str) -> TradingBrainSyncResult:
    return TradingBrainSyncResult(
        state=state,
        policy_version="2026-05-08-a",
        policy_hash="hash",
        board_date="2026-05-08",
        mode="supervised-live",
        generated_candidate_count=1,
        manual_candidate_count=0,
        exported_target_count=1,
        resolved_symbol_count=1,
        unresolved_symbol_count=0,
        selected_candidate_id="candidate",
        selected_ticker="KX-TEST",
        targets_path="targets.json",
        symbols_path="symbols.json",
        decisions_path="decisions.json",
        snapshot_dir=None,
        checks=[],
        synced_at=datetime.now(UTC),
    )


def test_loop_start_blocks_when_brain_is_not_live_ready(monkeypatch, tmp_path: Path) -> None:
    controller = TradingLoopController()
    settings = _settings(tmp_path)
    board_entry = SimpleNamespace(board_date=date(2026, 5, 8))

    monkeypatch.setattr("app.trading.loop_controller.sync_decision_brain", lambda **_kwargs: _brain_result(state="observe_only"))

    status = controller.start(
        settings=settings,
        board_entry=board_entry,
        session_factory=lambda: None,
    )

    assert status.state == "blocked"
    assert "live-ready" in status.message
    assert status.selected_ticker == "KX-TEST"
