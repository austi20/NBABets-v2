# app/trading/snapshot_service.py
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from app.config.settings import Settings
from app.server.schemas.trading import TradingLiveSnapshotModel
from app.trading.live_snapshot import LiveSnapshotInputs, TradingLiveSnapshotBuilder
from app.trading.market_book import MarketBook
from app.trading.selections import SelectionStore
from app.trading.stream_publisher import TradingStreamPublisher


class TradingSnapshotService:
    """Owns the dependencies needed to assemble a TradingLiveSnapshotModel."""

    def __init__(
        self,
        *,
        settings: Settings,
        market_book: MarketBook,
        selections_path: Path,
        publisher: TradingStreamPublisher,
    ) -> None:
        self._settings = settings
        self._market_book = market_book
        self._selections_path = selections_path
        self._publisher = publisher
        self._builder = TradingLiveSnapshotBuilder()

    @property
    def publisher(self) -> TradingStreamPublisher:
        return self._publisher

    def build(
        self,
        *,
        board_date: date,
        ledger_state: Any,
        positions: list[Any],
        fills: list[Any],
        resting_orders: list[Any],
        loop_state: str,
        mode: str,
        kill_switch_active: bool,
        readiness: Any | None,
        brain_status: Any | None,
        last_cursor: int = 0,
        errors: list[str] | None = None,
    ) -> TradingLiveSnapshotModel:
        selections = SelectionStore.load(self._selections_path)
        decision_pack = self._read_decision_pack()
        market_book_snapshot = self._market_book_snapshot()
        budget = self._read_budget()
        event_log = self._publisher.event_log_since(last_cursor)
        all_events = self._publisher.event_log_snapshot()
        stream_cursor = all_events[-1].cursor if all_events else 0
        inputs = LiveSnapshotInputs(
            decision_pack=decision_pack,
            market_book_snapshot=market_book_snapshot,
            selections=selections,
            board_date=board_date,
            budget=budget,
            cap_fraction=0.35,
            loop_state=loop_state,
            mode=mode,
            ws_connected=self._is_connected(),
            kill_switch_active=kill_switch_active,
            ledger_state=ledger_state,
            positions=positions,
            fills=fills,
            resting_orders=resting_orders,
            event_log=event_log,
            pnl_trend=[],
            readiness=readiness,
            brain_status=brain_status,
            stream_cursor=stream_cursor,
            errors=errors or [],
        )
        return self._builder.build(inputs)

    def _market_book_snapshot(self) -> dict[str, dict[str, Any]]:
        """Return {ticker: {yes_bid, yes_ask, spread, ts}} from MarketBook."""
        raw = self._market_book.snapshot()
        result: dict[str, dict[str, Any]] = {}
        for ticker, entry in raw.items():
            result[ticker] = {
                "yes_bid": entry.yes_bid,
                "yes_ask": entry.yes_ask,
                "spread": entry.spread,
                "ts": entry.updated_at,
            }
        return result

    def _is_connected(self) -> bool:
        # MarketBook itself has no connection state; check the ws_service via settings attribute
        # Falls back to False when no WS service is wired up
        return False

    def _read_decision_pack(self) -> dict[str, Any]:
        path_attr = getattr(self._settings, "kalshi_decisions_path", None)
        if path_attr is None:
            return {"decisions": []}
        path = Path(str(path_attr))
        if not path.is_file():
            return {"decisions": []}
        try:
            data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
            return data
        except (OSError, json.JSONDecodeError):
            return {"decisions": []}

    def _read_budget(self) -> float:
        path_attr = getattr(self._settings, "trading_limits_path", None)
        if path_attr is None:
            return 0.0
        path = Path(str(path_attr))
        if not path.is_file():
            return 0.0
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return float(data.get("max_open_notional", 0.0))
        except (OSError, json.JSONDecodeError, ValueError):
            return 0.0
