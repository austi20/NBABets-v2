# tests/unit/trading/test_selections.py
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from app.trading.selections import (
    SelectionStore,
    Thresholds,
)


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "trading_selections.json"


def test_load_missing_file_returns_empty_store(store_path: Path) -> None:
    store = SelectionStore.load(store_path)
    assert store.thresholds == Thresholds(min_hit_pct=0.0, min_edge_bps=0)
    assert store.selections == {}


def test_save_then_load_roundtrip(store_path: Path) -> None:
    store = SelectionStore.load(store_path)
    store.set_selection(date(2026, 5, 11), "cunningham-pts", True)
    store.set_selection(date(2026, 5, 11), "hardaway-3pt", False)
    store.save()

    reloaded = SelectionStore.load(store_path)
    assert reloaded.is_selected(date(2026, 5, 11), "cunningham-pts") is True
    assert reloaded.is_selected(date(2026, 5, 11), "hardaway-3pt") is False


def test_default_for_unknown_candidate_is_true(store_path: Path) -> None:
    store = SelectionStore.load(store_path)
    assert store.is_selected(date(2026, 5, 11), "never-toggled") is True


def test_threshold_update(store_path: Path) -> None:
    store = SelectionStore.load(store_path)
    store.update_thresholds(min_hit_pct=0.55, min_edge_bps=50)
    store.save()
    reloaded = SelectionStore.load(store_path)
    assert reloaded.thresholds.min_hit_pct == 0.55
    assert reloaded.thresholds.min_edge_bps == 50


def test_selections_older_than_7_days_pruned_on_save(store_path: Path) -> None:
    today = date(2026, 5, 11)
    old = today - timedelta(days=8)
    store = SelectionStore.load(store_path)
    store.set_selection(old, "old-candidate", False)
    store.set_selection(today, "today-candidate", False)
    store.save(today=today)

    reloaded = SelectionStore.load(store_path)
    assert old.isoformat() not in reloaded.selections
    assert today.isoformat() in reloaded.selections


def test_bulk_update_replaces_selections_for_date(store_path: Path) -> None:
    store = SelectionStore.load(store_path)
    today = date(2026, 5, 11)
    store.set_selection(today, "a", True)
    store.set_selection(today, "b", True)
    store.bulk_set(today, {"a": False, "b": False})
    assert store.is_selected(today, "a") is False
    assert store.is_selected(today, "b") is False
