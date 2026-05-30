from __future__ import annotations

from app.server.routers.props import _passes_backtest_tuning_filters, _passes_confidence_filter


def test_confidence_filter_is_inclusive_at_boundary() -> None:
    # "Solid+" requires tier rank >= 2; "Solid" is exactly rank 2 and must pass.
    assert _passes_confidence_filter("Solid", "Solid+") is True
    # "Watch" is rank 1, below the Solid+ threshold, and must be dropped.
    assert _passes_confidence_filter("Watch", "Solid+") is False


def test_max_edge_filter_uses_strict_greater_than_boundary() -> None:
    # Boundary semantics: a prop is dropped only when |hit - 0.5| is STRICTLY
    # greater than max_edge. Distance exactly at the ceiling is kept.
    disabled: frozenset[str] = frozenset()
    max_edge = 0.25
    # |0.75 - 0.5| == 0.25 == max_edge -> kept (not strictly greater).
    assert _passes_backtest_tuning_filters(
        "points", 0.75, disabled_markets=disabled, max_edge=max_edge
    ) is True
    # |0.80 - 0.5| == 0.30 > 0.25 -> dropped.
    assert _passes_backtest_tuning_filters(
        "points", 0.80, disabled_markets=disabled, max_edge=max_edge
    ) is False


def test_max_edge_filter_disabled_when_zero() -> None:
    # max_edge <= 0 disables the edge ceiling entirely.
    assert _passes_backtest_tuning_filters(
        "points", 0.99, disabled_markets=frozenset(), max_edge=0.0
    ) is True
