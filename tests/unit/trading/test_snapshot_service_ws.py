"""Tests for TradingSnapshotService._is_connected wiring to KalshiWebSocketConsumer."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

from app.trading.snapshot_service import TradingSnapshotService


def _make_service(tmp_path: Path, ws_consumer=None) -> TradingSnapshotService:
    settings = SimpleNamespace(
        kalshi_decisions_path=None,
        trading_limits_path=None,
    )
    market_book = MagicMock()
    market_book.snapshot.return_value = {}

    from app.trading.stream_publisher import TradingStreamPublisher
    publisher = TradingStreamPublisher()

    return TradingSnapshotService(
        settings=settings,
        market_book=market_book,
        selections_path=tmp_path / "sel.json",
        publisher=publisher,
        ws_consumer=ws_consumer,
    )


def _stub_ws(*, connected: bool) -> Any:
    ws = MagicMock()
    ws.is_connected = connected
    return ws


def test_is_connected_returns_true_when_ws_reports_connected(tmp_path: Path) -> None:
    service = _make_service(tmp_path, ws_consumer=_stub_ws(connected=True))
    assert service._is_connected() is True


def test_is_connected_returns_false_when_ws_reports_disconnected(tmp_path: Path) -> None:
    service = _make_service(tmp_path, ws_consumer=_stub_ws(connected=False))
    assert service._is_connected() is False


def test_is_connected_returns_false_when_no_ws_consumer(tmp_path: Path) -> None:
    service = _make_service(tmp_path, ws_consumer=None)
    assert service._is_connected() is False


def test_build_propagates_ws_connected_kpi(tmp_path: Path) -> None:
    """ws_connected KPI in built snapshot reflects actual WS state."""
    service = _make_service(tmp_path, ws_consumer=_stub_ws(connected=True))
    ledger = SimpleNamespace(
        daily_realized_pnl=0.0,
        daily_unrealized_pnl=0.0,
        daily_loss_cap=0.0,
        realized=0.0,
        unrealized=0.0,
    )
    snapshot = service.build(
        board_date=date(2026, 5, 11),
        ledger_state=ledger,
        positions=[],
        fills=[],
        resting_orders=[],
        loop_state="idle",
        mode="observe",
        kill_switch_active=False,
        readiness=None,
        brain_status=None,
    )
    assert snapshot.kpis.system.ws_connected is True
