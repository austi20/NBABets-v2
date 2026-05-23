from __future__ import annotations

from app.services.brain.actions.selective_retrain import build_retrain_market_list


def test_uses_per_market_floor_when_provided() -> None:
    signals = [
        {"market": "assists", "ece": 0.055},
        {"market": "threes", "ece": 0.030},
    ]
    floors = {"assists": 0.045, "threes": 0.040}
    assert build_retrain_market_list(signals, market_floors=floors) == ["assists"]


def test_falls_back_to_default_threshold() -> None:
    assert build_retrain_market_list([{"market": "points", "ece": 0.12}]) == ["points"]
    assert build_retrain_market_list([{"market": "points", "ece": 0.08}]) == []
