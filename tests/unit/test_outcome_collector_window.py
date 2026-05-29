"""Tests for outcome_collector date window fix — Task 6.4."""
from __future__ import annotations

from datetime import date

from app.services.brain.outcome_collector import _next_day


def test_window_uses_timedelta_day_plus_1() -> None:
    d = date(2026, 1, 31)
    assert _next_day(d) == date(2026, 2, 1)


def test_window_feb_28_advances_to_mar_1() -> None:
    d = date(2026, 2, 28)
    assert _next_day(d) == date(2026, 3, 1)


def test_window_feb_29_leap_year() -> None:
    d = date(2024, 2, 29)
    assert _next_day(d) == date(2024, 3, 1)
