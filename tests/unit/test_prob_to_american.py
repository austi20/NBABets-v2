"""Unit tests for the prob_to_american helper in scripts/retrain_and_predict.py."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "retrain_and_predict.py"
_spec = importlib.util.spec_from_file_location("retrain_and_predict", _MODULE_PATH)
assert _spec is not None and _spec.loader is not None
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
prob_to_american = _module.prob_to_american


@pytest.mark.parametrize("prob", [0.0, 1.0, -0.1, 1.5])
def test_returns_na_when_out_of_range(prob: float) -> None:
    assert prob_to_american(prob) == "N/A"


def test_even_money_is_plus_100() -> None:
    assert prob_to_american(0.5) == "+100"


def test_favorite_yields_negative_odds() -> None:
    # 0.75 over -> -300 favorite
    assert prob_to_american(0.75) == "-300"


def test_underdog_yields_positive_odds() -> None:
    # 0.25 over -> +300 underdog
    assert prob_to_american(0.25) == "+300"


def test_favorite_has_no_plus_sign() -> None:
    result = prob_to_american(0.6)
    assert result.startswith("-")
