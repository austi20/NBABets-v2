"""DNP anomaly guard for rolling window computation.

The training data loader already filters ``pgl.minutes >= 5``, but
rolling windows computed on historical data for *inference* can still
be contaminated by games where a player logged near-zero production
(injury exit, coach's decision DNP that slipped through).

This module adds a rolling-average-level guard: if a player's stat
value drops more than ``anomaly_drop_threshold`` relative to their
recent rolling average, that game is treated as an anomaly and
excluded from the window.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import numpy as np
import pandas as pd

from app.services.brain.contracts import CorrectionRecord, PlannedCorrection

logger = logging.getLogger(__name__)

DEFAULT_ANOMALY_DROP_THRESHOLD = 0.50
DEFAULT_MIN_MINUTES_GUARD = 5.0
DEFAULT_LOOKBACK_FOR_ANOMALY = 5


def execute_dnp_filter(
    correction: PlannedCorrection,
) -> CorrectionRecord:
    """Record a DNP filter correction (the actual filtering is applied
    via ``apply_dnp_anomaly_guard`` in the rolling window builder)."""
    params = correction.parameters
    record = CorrectionRecord(
        signal_type=correction.signal.signal_type,
        action_type="dnp_filter",
        market=correction.market,
        params_before={"anomaly_guard": False},
        params_after={
            "anomaly_guard": True,
            "threshold": params.get("anomaly_drop_threshold", DEFAULT_ANOMALY_DROP_THRESHOLD),
            "min_minutes": params.get("min_minutes_guard", DEFAULT_MIN_MINUTES_GUARD),
        },
        ece_before=correction.signal.metrics.get("ece"),
        outcome="pending",
        confidence=correction.confidence,
        created_at=datetime.now(UTC),
        notes=f"DNP anomaly guard enabled for {len(correction.signal.affected_players)} flagged players",
    )
    return record


def apply_dnp_anomaly_guard(
    frame: pd.DataFrame,
    stat_columns: list[str] | None = None,
    anomaly_drop_threshold: float = DEFAULT_ANOMALY_DROP_THRESHOLD,
    min_minutes_guard: float = DEFAULT_MIN_MINUTES_GUARD,
    lookback: int = DEFAULT_LOOKBACK_FOR_ANOMALY,
) -> pd.DataFrame:
    """Mark anomalous games so rolling windows can exclude them.

    Adds a boolean column ``_dnp_anomaly`` to the frame.  A row is
    flagged when:

    1. The player's ``minutes`` in that game are below ``min_minutes_guard``, OR
    2. For any stat in ``stat_columns``, the value drops more than
       ``anomaly_drop_threshold`` relative to the player's prior
       rolling mean over ``lookback`` games.

    The caller (rolling window builder) should exclude flagged rows
    from the rolling computation.
    """
    if stat_columns is None:
        stat_columns = ["points", "rebounds", "assists", "threes", "turnovers"]

    frame = frame.copy()

    # Guard 1: low minutes
    if "minutes" in frame.columns:
        low_minutes = frame["minutes"].fillna(0) < min_minutes_guard
    else:
        low_minutes = pd.Series(False, index=frame.index)

    # Guard 2: anomalous stat drops
    anomaly_flags = pd.Series(False, index=frame.index)
    for stat in stat_columns:
        if stat not in frame.columns:
            continue
        grouped = frame.groupby("player_id")[stat]
        rolling_mean = grouped.transform(
            lambda s: s.shift(1).rolling(lookback, min_periods=2).mean()
        )
        # Flag where current value < (1 - threshold) * rolling_mean
        # Only flag when rolling_mean is meaningful (> 1.0 to avoid division noise)
        threshold_value = (1.0 - anomaly_drop_threshold) * rolling_mean
        is_anomaly = (frame[stat] < threshold_value) & (rolling_mean > 1.0)
        anomaly_flags = anomaly_flags | is_anomaly

    frame["_dnp_anomaly"] = low_minutes | anomaly_flags

    flagged_count = int(frame["_dnp_anomaly"].sum())
    if flagged_count > 0:
        logger.info(
            "DNP anomaly guard flagged %d/%d rows (%.1f%%)",
            flagged_count,
            len(frame),
            100 * flagged_count / max(len(frame), 1),
        )

    return frame


def filter_anomalies_from_rolling(
    series: pd.Series,
    anomaly_mask: pd.Series,
) -> pd.Series:
    """Replace anomalous values with NaN so rolling windows skip them."""
    cleaned = series.copy()
    cleaned[anomaly_mask] = np.nan
    return cleaned
