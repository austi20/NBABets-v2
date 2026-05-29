"""Tests for _grade_finding real grading logic."""

from __future__ import annotations

from datetime import date

from app.services.examiner.contracts import LabeledPropDataset, LabeledPropExample
from app.services.examiner.feedback import _grade_finding

GAME_DATE = date(2026, 1, 15)
MARKET = "points"
LINE = 25.5


def _make_dataset(*examples: LabeledPropExample) -> LabeledPropDataset:
    return LabeledPropDataset(
        examples=examples,
        real_count=len(examples),
        synthetic_count=0,
        earliest_date=GAME_DATE,
        latest_date=GAME_DATE,
        source_path="test.csv",
    )


def _example(**overrides) -> LabeledPropExample:
    defaults = dict(
        game_date=GAME_DATE,
        player_name="LeBron James",
        team="LAL",
        opponent="GSW",
        market=MARKET,
        sportsbook="draftkings",
        line_value=LINE,
        over_odds=-110.0,
        under_odds=-110.0,
        actual=28.0,
        hit_over=True,
        hit_under=False,
        push=False,
        minutes=35.0,
        source="real",
    )
    defaults.update(overrides)
    return LabeledPropExample(**defaults)


def test_grade_over_hit() -> None:
    ds = _make_dataset(_example(hit_over=True, hit_under=False, push=False))
    result = _grade_finding({"market": MARKET, "line_value": LINE}, ds, GAME_DATE)
    assert result == "over_hit"


def test_grade_under_hit() -> None:
    ds = _make_dataset(_example(hit_over=False, hit_under=True, push=False, actual=22.0))
    result = _grade_finding({"market": MARKET, "line_value": LINE}, ds, GAME_DATE)
    assert result == "under_hit"


def test_grade_push() -> None:
    ds = _make_dataset(_example(push=True, hit_over=False, hit_under=False, actual=25.5))
    result = _grade_finding({"market": MARKET, "line_value": LINE}, ds, GAME_DATE)
    assert result == "push"


def test_grade_missing() -> None:
    ds = _make_dataset(_example(actual=None, hit_over=None, hit_under=None, push=False))
    result = _grade_finding({"market": MARKET, "line_value": LINE}, ds, GAME_DATE)
    assert result == "missing"


def test_grade_no_match() -> None:
    ds = _make_dataset(_example(market="rebounds"))
    result = _grade_finding({"market": MARKET, "line_value": LINE}, ds, GAME_DATE)
    assert result == "neutral"


def test_grade_no_dataset() -> None:
    result = _grade_finding({"market": MARKET, "line_value": LINE}, None, GAME_DATE)
    assert result == "neutral"


def test_grade_line_too_far_skips_example() -> None:
    # line_value=30.0, example has 25.5 — difference 4.5 > 0.5, no match
    ds = _make_dataset(_example(hit_over=True))
    result = _grade_finding({"market": MARKET, "line_value": 30.0}, ds, GAME_DATE)
    assert result == "neutral"
