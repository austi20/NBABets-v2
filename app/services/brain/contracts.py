"""Immutable data contracts for the Brain learning system."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

CorrectionOutcome = Literal["improved", "worsened", "neutral", "pending"]
CorrectionActionType = Literal[
    "weight_adjust",
    "selective_retrain",
    "calibration_patch",
    "dnp_filter",
    "feature_dampen",
    "feature_amplify",
]
SignalType = Literal[
    "dnp_contamination",
    "overfit",
    "calibration_drift",
    "projection_divergence",
    "extreme_probability",
    "empty_backtest",
    "data_quality_degraded",
]


@dataclass(frozen=True)
class DiagnosticSignal:
    """A structured problem signal extracted from an automation report."""

    signal_type: SignalType
    severity: Literal["critical", "high", "medium"]
    market: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    affected_players: tuple[str, ...] = ()
    raw_text: str = ""


@dataclass(frozen=True)
class CorrectionRecord:
    """A single correction action attempted by the brain."""

    correction_id: int | None = None
    signal_type: SignalType = "overfit"
    action_type: CorrectionActionType = "weight_adjust"
    market: str | None = None
    params_before: dict[str, Any] = field(default_factory=dict)
    params_after: dict[str, Any] = field(default_factory=dict)
    ece_before: float | None = None
    ece_after: float | None = None
    outcome: CorrectionOutcome = "pending"
    confidence: float = 0.0
    created_at: datetime | None = None
    resolved_at: datetime | None = None
    notes: str = ""


@dataclass(frozen=True)
class MarketProfile:
    """Learned per-market state accumulated over multiple correction cycles."""

    market: str
    optimal_weights: dict[str, float] = field(default_factory=dict)
    calibration_strategy: str = "auto"
    failure_modes: tuple[str, ...] = ()
    ece_history: tuple[tuple[str, float], ...] = ()  # (date_iso, ece)
    correction_count: int = 0
    avg_improvement: float = 0.0


@dataclass(frozen=True)
class StrategyMemory:
    """A higher-level lesson: what action works for what problem type."""

    strategy_id: int | None = None
    problem_type: SignalType = "overfit"
    action_template: CorrectionActionType = "weight_adjust"
    market: str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    success_rate: float = 0.0
    avg_ece_improvement: float = 0.0
    sample_count: int = 0
    last_used_at: datetime | None = None


@dataclass(frozen=True)
class PredictionOutcome:
    """Actual result vs predicted for a single prediction."""

    outcome_id: int | None = None
    prediction_id: int | None = None
    player_name: str = ""
    market: str = ""
    line_value: float = 0.0
    predicted_probability: float = 0.0
    calibrated_probability: float = 0.0
    actual_value: float | None = None
    hit: bool | None = None  # None = not yet resolved
    game_date: str = ""


@dataclass(frozen=True)
class PlannedCorrection:
    """A correction the brain plans to execute, with safety metadata."""

    signal: DiagnosticSignal
    action_type: CorrectionActionType
    market: str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    strategy_source: StrategyMemory | None = None
    expected_improvement: float = 0.0
    confidence: float = 0.0
    safety_notes: str = ""


@dataclass(frozen=True)
class CorrectionPlan:
    """A set of corrections to execute in a single cycle, with safety caps."""

    corrections: tuple[PlannedCorrection, ...] = ()
    max_corrections_per_run: int = 3
    max_weight_change_pct: float = 0.20
    revert_after_runs: int = 2
    dry_run: bool = True
