"""Frozen DTOs for the accuracy examiner subsystem.

Everything else in ``app.services.examiner`` and
``app.services.agents.accuracy_examiner`` imports from this module. Keep it
dependency-light — no ORM, no orchestrator, no settings imports — so tests can
build fixtures without touching the database or the network.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal

# Canonical market vocabulary — must match ``app.training.pipeline``.
CanonicalMarket = Literal[
    "points",
    "rebounds",
    "assists",
    "threes",
    "turnovers",
    "pra",
]

# Confidence buckets used by retrieval and lesson-card keying.
ConfidenceBucket = Literal["low", "mid", "high", "extreme"]

# Signal taxonomy the examiner may raise. Intentionally narrower than
# ``app.services.brain.contracts.SignalType`` so findings map cleanly back to
# brain corrections without inventing new classes.
ExaminerSignal = Literal[
    "dnp_contamination",
    "projection_divergence",
    "extreme_probability",
    "calibration_drift",
    "overfit",
    "synthetic_leakage",
    "calculation_error",
    "data_quality_degraded",
]

EXAMINER_SIGNAL_TAXONOMY: frozenset[str] = frozenset(
    {
        "dnp_contamination",
        "projection_divergence",
        "extreme_probability",
        "calibration_drift",
        "overfit",
        "synthetic_leakage",
        "calculation_error",
        "data_quality_degraded",
    }
)


@dataclass(frozen=True)
class LabeledPropExample:
    """Single graded prop row lifted from the examiner CSV.

    Attributes map 1:1 to the canonical examiner schema. ``source`` reflects
    the CSV column of the same name; ``market`` is always the canonical form,
    not the raw CSV token.
    """

    game_date: date
    player_name: str
    team: str
    opponent: str
    market: str
    sportsbook: str
    line_value: float
    over_odds: float | None
    under_odds: float | None
    actual: float | None
    hit_over: bool | None
    hit_under: bool | None
    push: bool
    minutes: float | None
    source: Literal["real", "synthetic"]


@dataclass(frozen=True)
class LabeledPropDataset:
    """Container for a batch of ``LabeledPropExample`` rows.

    ``mix_ratio_real_vs_synthetic`` is surfaced in the examiner prompt so the
    model understands its retrieval pool is skewed toward synthetic data.
    """

    examples: tuple[LabeledPropExample, ...]
    real_count: int
    synthetic_count: int
    earliest_date: date | None
    latest_date: date | None
    source_path: str

    @property
    def total(self) -> int:
        return self.real_count + self.synthetic_count

    @property
    def mix_ratio_real_vs_synthetic(self) -> float:
        """Fraction of rows flagged as ``source='real'``.

        Returns ``0.0`` for an empty dataset so prompt builders can safely
        format it without a branch.
        """

        total = self.total
        if total <= 0:
            return 0.0
        return self.real_count / total


@dataclass(frozen=True)
class LessonCard:
    """A graded observation the examiner should internalize over time.

    Lesson cards are written by ``feedback.capture_daily_feedback`` once the
    examiner's prior-day findings have been compared to ground truth. They are
    retrieved alongside labeled examples when the prompt is built.
    """

    lesson_id: int | None
    created_at: datetime
    market: str | None
    line_bucket: str | None
    confidence_bucket: ConfidenceBucket | None
    signal: str
    headline: str
    body: str
    ece_before: float | None
    ece_after: float | None
    outcome: Literal["improved", "worsened", "neutral", "pending"]


@dataclass(frozen=True)
class ExaminerFinding:
    """A single structured finding returned by Qwen after prompt execution."""

    signal: str  # one of EXAMINER_SIGNAL_TAXONOMY (validated at parse time)
    headline: str
    detail: str
    market: str | None
    confidence: float
    recommended_action: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExaminerRetrievalResult:
    """What ``ExaminerRetriever.retrieve`` hands to the prompt builder."""

    examples: tuple[LabeledPropExample, ...]
    lesson_cards: tuple[LessonCard, ...]
    debug_hints: tuple[str, ...]
    mix_ratio_real_vs_synthetic: float


@dataclass(frozen=True)
class ExaminerPromptContext:
    """Everything the prompt builder needs from the agent handler.

    ``markets`` is the set of canonical markets that the examiner should focus
    on this run — usually derived from latest backtest metrics or the active
    prop board. ``line_bucket`` and ``confidence_bucket`` are optional hints
    used by the retriever to narrow the search.
    """

    report_date: date
    markets: tuple[str, ...]
    line_bucket: str | None
    confidence_bucket: ConfidenceBucket | None
    latest_model_metrics: dict[str, Any]
    latest_backtest_metrics: dict[str, Any]
    trend_alerts: tuple[str, ...]


def bucket_confidence(value: float | None) -> ConfidenceBucket | None:
    """Map a raw confidence score to the examiner bucket vocabulary."""

    if value is None:
        return None
    if value < 0.55:
        return "low"
    if value < 0.70:
        return "mid"
    if value < 0.85:
        return "high"
    return "extreme"


def bucket_line(line_value: float | None, *, step: float = 0.5) -> str | None:
    """Quantize a raw line value into a coarse retrieval bucket.

    ``None`` in → ``None`` out. The step defaults to 0.5 so buckets line up
    with typical sportsbook half-point lines.
    """

    if line_value is None:
        return None
    bucket = round(line_value / step) * step
    return f"{bucket:.1f}"
