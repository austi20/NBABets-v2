"""Tests for BrainStore concurrent write safety — Task 6.5."""
from __future__ import annotations

import threading
from pathlib import Path

from app.services.brain.contracts import CorrectionRecord
from app.services.brain.store import BrainStore


def test_concurrent_record_correction_no_locked_error(tmp_path: Path) -> None:
    store = BrainStore(tmp_path / "brain.db")
    errors: list[Exception] = []

    def worker() -> None:
        try:
            for _ in range(10):
                record = CorrectionRecord(
                    signal_type="overfit",
                    action_type="weight_adjust",
                    market="points",
                    confidence=0.7,
                )
                store.store_correction(record)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Concurrent writes raised: {errors}"
    rows = store._conn.execute("SELECT COUNT(*) FROM correction_log").fetchone()[0]
    assert rows == 80
    store.close()
