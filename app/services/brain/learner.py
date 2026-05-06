"""Learning engine: analyzes outcomes and updates brain strategy memory.

Runs after outcome collection to close the feedback loop:
correction -> outcome -> learn -> update strategies -> better future corrections.
"""

from __future__ import annotations

import logging
from collections import defaultdict

import numpy as np

from app.services.brain.brain import Brain

logger = logging.getLogger(__name__)


def learn_from_outcomes(brain: Brain) -> dict[str, int]:
    """Analyze recent outcomes and update the brain's understanding.

    1. For each pending correction, check if enough outcomes exist to evaluate.
    2. Compute realized ECE per market from recent outcomes.
    3. Mark corrections as improved/worsened/neutral.
    4. Update market profiles with new ECE data.
    5. Sync strategies to vault.

    Returns a summary dict of actions taken.
    """
    stats = {"corrections_resolved": 0, "markets_updated": 0, "strategies_synced": 0}

    # Step 1: Evaluate pending corrections
    pending = brain._store.pending_corrections()
    if pending:
        market_eces = _compute_market_eces(brain)
        for correction in pending:
            if correction.market and correction.market in market_eces:
                realized_ece = market_eces[correction.market]
                ece_before = correction.ece_before or 0.0
                if realized_ece < ece_before - 0.005:
                    brain.learn(correction.correction_id, "improved", realized_ece)
                    stats["corrections_resolved"] += 1
                    logger.info(
                        "Correction %d IMPROVED %s ECE: %.4f -> %.4f",
                        correction.correction_id,
                        correction.market,
                        ece_before,
                        realized_ece,
                    )
                elif realized_ece > ece_before + 0.005:
                    brain.learn(correction.correction_id, "worsened", realized_ece)
                    stats["corrections_resolved"] += 1
                    logger.info(
                        "Correction %d WORSENED %s ECE: %.4f -> %.4f",
                        correction.correction_id,
                        correction.market,
                        ece_before,
                        realized_ece,
                    )
                else:
                    brain.learn(correction.correction_id, "neutral", realized_ece)
                    stats["corrections_resolved"] += 1

    # Step 2: Update market profiles with latest ECE
    market_eces = _compute_market_eces(brain)
    for market, ece in market_eces.items():
        brain._update_market_profile(market, ece, "neutral")
        stats["markets_updated"] += 1

    # Step 3: Sync strategies to vault
    stats["strategies_synced"] = brain.sync_strategies_to_vault()

    logger.info(
        "Learning complete: %d corrections resolved, %d markets updated, %d strategies synced",
        stats["corrections_resolved"],
        stats["markets_updated"],
        stats["strategies_synced"],
    )
    return stats


def _compute_market_eces(brain: Brain) -> dict[str, float]:
    """Compute realized ECE per market from recent outcomes."""
    outcomes = brain.recall_outcomes(limit=2000)
    if not outcomes:
        return {}

    # Group by market
    by_market: dict[str, list[tuple[float, bool]]] = defaultdict(list)
    for o in outcomes:
        if o.hit is not None:
            by_market[o.market].append((o.calibrated_probability, o.hit))

    result: dict[str, float] = {}
    for market, pairs in by_market.items():
        if len(pairs) < 20:
            continue
        probs = np.array([p for p, _ in pairs])
        hits = np.array([1.0 if h else 0.0 for _, h in pairs])
        result[market] = _expected_calibration_error(probs, hits)

    return result


def expected_calibration_error(
    probabilities: list[float] | np.ndarray,
    outcomes: list[bool] | np.ndarray,
    *,
    n_bins: int = 10,
) -> float:
    """Public ECE helper for examiner feedback and tests (wraps binning logic)."""

    probs = np.asarray(probabilities, dtype=float)
    hits = np.asarray([1.0 if bool(h) else 0.0 for h in outcomes], dtype=float)
    return _expected_calibration_error(probs, hits, n_bins=n_bins)


def _expected_calibration_error(
    probabilities: np.ndarray,
    outcomes: np.ndarray,
    n_bins: int = 10,
) -> float:
    """Compute ECE (Expected Calibration Error) over n_bins."""
    bin_edges = np.linspace(0, 1, n_bins + 1)
    ece = 0.0
    total = len(probabilities)
    if total == 0:
        return 0.0

    for i in range(n_bins):
        mask = (probabilities >= bin_edges[i]) & (probabilities < bin_edges[i + 1])
        if i == n_bins - 1:
            mask = mask | (probabilities == bin_edges[i + 1])
        bin_count = mask.sum()
        if bin_count == 0:
            continue
        avg_prob = probabilities[mask].mean()
        avg_outcome = outcomes[mask].mean()
        ece += (bin_count / total) * abs(avg_prob - avg_outcome)

    return float(ece)
