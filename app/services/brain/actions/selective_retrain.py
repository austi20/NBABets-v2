"""Selective retrain: retrain only markets that are drifting.

Instead of retraining the full pipeline, this loads the existing
TrainedBundle and replaces only the stat models + calibrators for
the specified markets.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from app.services.brain.contracts import CorrectionRecord, PlannedCorrection

logger = logging.getLogger(__name__)


def execute_selective_retrain(
    correction: PlannedCorrection,
    session: Any,
) -> CorrectionRecord:
    """Trigger a retrain for specific markets only.

    This creates a CorrectionRecord but delegates actual retraining
    to the training pipeline via ``TrainingPipeline.retrain_markets()``.
    The caller (correction executor) is responsible for invoking the
    pipeline and comparing pre/post ECE.
    """
    market = correction.market
    params = correction.parameters
    markets_to_retrain = [market] if market else params.get("markets", [])

    ece_before = correction.signal.metrics.get("ece")

    record = CorrectionRecord(
        signal_type=correction.signal.signal_type,
        action_type="selective_retrain",
        market=correction.market,
        params_before={"markets": markets_to_retrain, "triggered_by": correction.signal.signal_type},
        params_after={"markets": markets_to_retrain, "retrain_requested": True},
        ece_before=ece_before,
        outcome="pending",
        confidence=correction.confidence,
        created_at=datetime.now(UTC),
        notes=f"Selective retrain for: {', '.join(markets_to_retrain)}",
    )
    return record


def build_retrain_market_list(
    signals: list[dict[str, Any]],
    ece_threshold: float = 0.10,
) -> list[str]:
    """Determine which markets need retraining from overfit signals."""
    markets: list[str] = []
    for sig in signals:
        market = sig.get("market")
        ece = sig.get("ece", 0.0)
        if market and ece > ece_threshold:
            markets.append(market)
    return sorted(set(markets))
