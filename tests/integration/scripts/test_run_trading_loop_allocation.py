# tests/integration/scripts/test_run_trading_loop_allocation.py
from __future__ import annotations

from pathlib import Path

import pytest

from app.trading.allocation import AllocationPick, allocate_proportional_with_soft_cap


def test_run_trading_loop_uses_shared_allocation(tmp_path: Path) -> None:
    """Verifies the loop runner pulls from the shared allocation helper rather than
    doing its own math. Smoke test only — full live flow is covered elsewhere."""
    import scripts.run_trading_loop as runner

    source = Path(runner.__file__).read_text(encoding="utf-8")
    assert "from app.trading.allocation import" in source, (
        "run_trading_loop.py should import the shared allocation helper"
    )
    assert "allocate_proportional_with_soft_cap" in source
