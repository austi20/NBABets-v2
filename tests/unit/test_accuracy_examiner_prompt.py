"""Unit tests for ``app.services.examiner.prompt_builder``."""

from __future__ import annotations

from datetime import UTC, date, datetime

from app.services.examiner.contracts import (
    ExaminerPromptContext,
    ExaminerRetrievalResult,
    LabeledPropExample,
    LessonCard,
)
from app.services.examiner.prompt_builder import build_examiner_prompt


def test_prompt_contains_json_contract_and_mix_warning() -> None:
    ctx = ExaminerPromptContext(
        report_date=date(2026, 4, 1),
        markets=("points",),
        line_bucket=None,
        confidence_bucket=None,
        latest_model_metrics={"ece": 0.11},
        latest_backtest_metrics={},
        trend_alerts=("alert1",),
    )
    ex = LabeledPropExample(
        game_date=date(2026, 3, 1),
        player_name="X",
        team="",
        opponent="",
        market="points",
        sportsbook="",
        line_value=20.0,
        over_odds=None,
        under_odds=None,
        actual=None,
        hit_over=None,
        hit_under=None,
        push=False,
        minutes=None,
        source="real",
    )
    lc = LessonCard(
        lesson_id=1,
        created_at=datetime.now(UTC),
        market="points",
        line_bucket="20.0",
        confidence_bucket="mid",
        signal="overfit",
        headline="head",
        body="b" * 300,
        ece_before=None,
        ece_after=None,
        outcome="neutral",
    )
    retrieved = ExaminerRetrievalResult(
        examples=(ex,),
        lesson_cards=(lc,),
        debug_hints=("d" * 100,),
        mix_ratio_real_vs_synthetic=0.02,
    )
    text = build_examiner_prompt(
        ctx,
        retrieved,
        csv_mix_warning="Mostly synthetic.",
        overfit_block={"risk": 0.5},
    )
    assert "STRICT JSON" in text
    assert "retrain_recommendation" in text
    assert "DATA_QUALITY_WARNING" in text
    assert "Mostly synthetic." in text
    assert "0.0200" in text or "0.02" in text
