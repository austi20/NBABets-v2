"""Unit tests for ``app.services.examiner.retrieval``."""

from __future__ import annotations

from datetime import date

from app.services.brain.store import BrainStore
from app.services.examiner.contracts import ExaminerPromptContext, LabeledPropDataset, LabeledPropExample
from app.services.examiner.retrieval import ExaminerRetriever
from app.services.examiner.store import ExaminerStore


def _ex(
    gd: date,
    player: str,
    market: str,
    line: float,
    *,
    source: str = "real",
) -> LabeledPropExample:
    return LabeledPropExample(
        game_date=gd,
        player_name=player,
        team="A",
        opponent="B",
        market=market,
        sportsbook="fd",
        line_value=line,
        over_odds=-110.0,
        under_odds=-110.0,
        actual=25.0,
        hit_over=True,
        hit_under=False,
        push=False,
        minutes=30.0,
        source=source,  # type: ignore[arg-type]
    )


def test_retriever_empty_store_returns_dataset_examples(tmp_path) -> None:
    brain_path = tmp_path / "brain.sqlite"
    BrainStore(brain_path)
    store = ExaminerStore(brain_path)
    try:
        ds = LabeledPropDataset(
            examples=(
                _ex(date(2026, 3, 1), "A", "points", 22.5),
                _ex(date(2026, 3, 2), "B", "rebounds", 8.5, source="synthetic"),
            ),
            real_count=1,
            synthetic_count=1,
            earliest_date=date(2026, 3, 1),
            latest_date=date(2026, 3, 2),
            source_path="mem",
        )
        ctx = ExaminerPromptContext(
            report_date=date(2026, 3, 3),
            markets=("points",),
            line_bucket="22.5",
            confidence_bucket=None,
            latest_model_metrics={},
            latest_backtest_metrics={},
            trend_alerts=(),
        )
        r = ExaminerRetriever(store).retrieve(
            ctx,
            ds,
            top_k=4,
            debug_hint_lines=("hint1", "hint2"),
        )
        assert r.mix_ratio_real_vs_synthetic == 0.5
        assert len(r.examples) >= 1
        assert r.examples[0].market == "points"
        assert "hint1" in r.debug_hints
    finally:
        store.close()


def test_retriever_lesson_cards_from_store(tmp_path) -> None:
    brain_path = tmp_path / "brain.sqlite"
    BrainStore(brain_path)
    store = ExaminerStore(brain_path)
    try:
        store.insert_lesson_card(
            market="points",
            line_bucket="22.5",
            confidence_bucket="high",
            signal="overfit",
            headline="test",
            body="body",
            ece_before=0.1,
            ece_after=None,
            outcome="neutral",
        )
        ds = LabeledPropDataset(
            examples=(),
            real_count=0,
            synthetic_count=0,
            earliest_date=None,
            latest_date=None,
            source_path="mem",
        )
        ctx = ExaminerPromptContext(
            report_date=date(2026, 3, 3),
            markets=("points",),
            line_bucket="22.5",
            confidence_bucket="high",
            latest_model_metrics={},
            latest_backtest_metrics={},
            trend_alerts=(),
        )
        r = ExaminerRetriever(store).retrieve(
            ctx,
            ds,
            top_k=2,
            debug_hint_lines=(),
        )
        assert len(r.lesson_cards) == 1
        assert r.lesson_cards[0].signal == "overfit"
    finally:
        store.close()
