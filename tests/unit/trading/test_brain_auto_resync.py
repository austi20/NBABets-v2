# tests/unit/trading/test_brain_auto_resync.py
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from app.trading.brain_auto_resync import BrainAutoResync


@pytest.mark.asyncio
async def test_runs_on_interval_in_live_mode() -> None:
    sync_fn = MagicMock()
    sync_fn.return_value = MagicMock(state="synced", selected_ticker="KX-A")
    publisher = MagicMock()
    resync = BrainAutoResync(
        interval_seconds=0.05,
        sync_fn=sync_fn,
        mode_fn=lambda: "supervised-live",
        publisher=publisher,
    )
    await resync.start()
    await asyncio.sleep(0.15)
    await resync.stop()
    assert sync_fn.call_count >= 2
    assert publisher.log_event.called


@pytest.mark.asyncio
async def test_pauses_in_observe_mode() -> None:
    sync_fn = MagicMock(return_value=MagicMock(state="observe_only"))
    resync = BrainAutoResync(
        interval_seconds=0.05,
        sync_fn=sync_fn,
        mode_fn=lambda: "observe",
        publisher=MagicMock(),
    )
    await resync.start()
    await asyncio.sleep(0.15)
    await resync.stop()
    sync_fn.assert_not_called()


@pytest.mark.asyncio
async def test_logs_errors_without_crashing() -> None:
    sync_fn = MagicMock(side_effect=RuntimeError("boom"))
    publisher = MagicMock()
    resync = BrainAutoResync(
        interval_seconds=0.05,
        sync_fn=sync_fn,
        mode_fn=lambda: "supervised-live",
        publisher=publisher,
    )
    await resync.start()
    await asyncio.sleep(0.10)
    await resync.stop()
    error_calls = [c for c in publisher.log_event.call_args_list if c.kwargs.get("level") == "error"]
    assert len(error_calls) >= 1
