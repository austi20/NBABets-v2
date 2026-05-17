"""Unit tests for the 2026-05-16 backtest-tuning filters added to /api/props.

The filter logic is a pure function (`_passes_backtest_tuning_filters`) so we
can verify it without spinning up a board cache fixture. The end-to-end path
is exercised in the integration suite via the existing volatility_api tests.
"""
from __future__ import annotations

import pytest

from app.server.routers.props import _passes_backtest_tuning_filters


@pytest.mark.parametrize(
    ("market", "hit_p", "disabled", "max_edge", "expected"),
    [
        # Disabled market drops regardless of probability.
        ("threes", 0.55, frozenset({"threes"}), 0.30, False),
        ("THREES", 0.55, frozenset({"threes"}), 0.30, False),  # case insensitive
        ("threes", 0.55, frozenset(), 0.30, True),  # disabled set empty -> allowed
        # Overconfident pick drops once |p-0.5| crosses max_edge.
        ("points", 0.82, frozenset(), 0.30, False),  # edge 0.32 > cap
        ("points", 0.79, frozenset(), 0.30, True),  # edge 0.29 < cap
        ("points", 0.18, frozenset(), 0.30, False),  # edge 0.32 on under side
        ("points", 0.21, frozenset(), 0.30, True),  # edge 0.29 < cap
        # max_edge=0 disables the edge filter.
        ("points", 0.95, frozenset(), 0.0, True),
        # Both filters together.
        ("threes", 0.50, frozenset({"threes"}), 0.30, False),  # blocked by market
        ("points", 0.50, frozenset({"threes"}), 0.30, True),  # passes both
    ],
)
def test_passes_backtest_tuning_filters(
    market: str,
    hit_p: float,
    disabled: frozenset[str],
    max_edge: float,
    expected: bool,
) -> None:
    assert (
        _passes_backtest_tuning_filters(
            market, hit_p, disabled_markets=disabled, max_edge=max_edge
        )
        is expected
    )
