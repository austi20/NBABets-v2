"""Collects actual game results and matches them against predictions.

After games complete, this module fetches actual box scores and compares
them to the predictions made for that game date, storing PredictionOutcome
records in the brain for learning.
"""

from __future__ import annotations

import logging
from datetime import date

from sqlalchemy.orm import Session

from app.models.all import PlayerGameLog, Prediction, PropMarket
from app.services.brain.brain import Brain
from app.services.brain.contracts import PredictionOutcome

logger = logging.getLogger(__name__)


def collect_outcomes(
    session: Session,
    brain: Brain,
    target_date: date | None = None,
) -> int:
    """Match predictions against actual results for a given date.

    Returns the number of outcomes stored.
    """
    effective_date = target_date or date.today()

    # Get predictions for the target date
    predictions = (
        session.query(Prediction)
        .filter(Prediction.predicted_at >= str(effective_date))
        .filter(Prediction.predicted_at < str(date(effective_date.year, effective_date.month, effective_date.day + 1) if effective_date.day < 28 else effective_date))
        .all()
    )
    if not predictions:
        # Fallback: get predictions by joining to games on the target date
        from app.models.all import Game
        predictions = (
            session.query(Prediction)
            .join(Game, Prediction.game_id == Game.game_id)
            .filter(Game.game_date == effective_date)
            .all()
        )

    if not predictions:
        logger.info("No predictions found for %s", effective_date)
        return 0

    # Build market key lookup
    market_map = {m.market_id: m.key for m in session.query(PropMarket).all()}

    # Get all player game logs for the target date
    game_ids = {p.game_id for p in predictions}
    logs = (
        session.query(PlayerGameLog)
        .filter(PlayerGameLog.game_id.in_(game_ids))
        .all()
    )

    # Index by (player_id, game_id) for fast lookup
    log_index: dict[tuple[int, int], PlayerGameLog] = {}
    for log in logs:
        log_index[(log.player_id, log.game_id)] = log

    stored = 0
    for pred in predictions:
        log = log_index.get((pred.player_id, pred.game_id))
        if log is None:
            continue  # Game not yet completed

        market_key = market_map.get(pred.market_id, "unknown")
        actual_value = _extract_actual_value(log, market_key)
        if actual_value is None:
            continue

        line_value = pred.projected_mean or 0.0
        # Use the line_snapshot line_value if available
        if hasattr(pred, "line_snapshot") and pred.line_snapshot:
            line_value = getattr(pred.line_snapshot, "line_value", line_value)

        hit = actual_value > line_value

        outcome = PredictionOutcome(
            prediction_id=pred.prediction_id,
            player_name=str(getattr(log, "player_name", f"player_{pred.player_id}")),
            market=market_key,
            line_value=float(line_value),
            predicted_probability=float(pred.over_probability or 0.0),
            calibrated_probability=float(pred.calibration_adjusted_probability or 0.0),
            actual_value=float(actual_value),
            hit=hit,
            game_date=str(effective_date),
        )
        brain.record_outcome(outcome)
        stored += 1

    logger.info("Collected %d outcomes for %s", stored, effective_date)
    return stored


def _extract_actual_value(log: PlayerGameLog, market_key: str) -> float | None:
    """Extract the actual stat value from a player game log for a given market."""
    mapping = {
        "points": "points",
        "rebounds": "rebounds",
        "assists": "assists",
        "threes": "threes",
        "turnovers": "turnovers",
        "pra": None,  # computed
    }
    if market_key == "pra":
        pts = getattr(log, "points", None)
        reb = getattr(log, "rebounds", None)
        ast = getattr(log, "assists", None)
        if pts is not None and reb is not None and ast is not None:
            return float(pts) + float(reb) + float(ast)
        return None

    attr = mapping.get(market_key)
    if attr is None:
        return None
    val = getattr(log, attr, None)
    return float(val) if val is not None else None
