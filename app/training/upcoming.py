"""Upcoming game feature frame builder.

Standalone module for building the inference-ready feature frame for scheduled
games. Combines BallDontLie schedule data with DB historical logs and runs the
feature engineering pipeline.  Consumers (pipeline, scripts, desktop) all route
through here instead of duplicating the data-loading + feature-build sequence.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date

import pandas as pd
from sqlalchemy.orm import Session

from app.providers.live import BallDontLieStatsProvider
from app.schemas.domain import GamePayload
from app.training.data import DatasetLoader
from app.training.data_sufficiency import annotate_tiers
from app.training.features import FeatureEngineer

_log = logging.getLogger(__name__)


def build_upcoming_feature_frame(
    target_date: date,
    session: Session,
    historical: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Return an inference-ready feature frame for scheduled games on target_date.

    Combines BallDontLie schedule context with DB player lines and builds
    features via FeatureEngineer. Returns the same column contract the model expects.

    Args:
        target_date: Date to build features for.
        session: DB session used to load historical data and upcoming lines.
        historical: Pre-loaded historical frame; loaded from DB when None.
    """
    bdl_count = _log_bdl_game_count(target_date)
    if bdl_count == 0:
        _log.info("BDL reports no games scheduled for %s.", target_date)

    loader = DatasetLoader(session)
    if historical is None or historical.empty:
        historical = loader.load_historical_player_games()
    upcoming = loader.load_upcoming_player_lines(target_date)
    if upcoming.empty:
        return pd.DataFrame()

    upcoming = annotate_tiers(upcoming=upcoming, historical=historical)
    feature_set = FeatureEngineer().build_inference_frame(historical, upcoming)
    return feature_set.frame


def load_upcoming_scoped(
    target_date: date,
    session: Session,
    max_game_count: int = 0,
) -> tuple[pd.DataFrame, set[int] | None]:
    """Load upcoming player lines and optionally cap the game set.

    Returns ``(upcoming_frame, game_ids | None)``.  Used by smoke mode to scope
    predictions to a small number of games without repeating the DB query.

    Args:
        target_date: Board date for the upcoming lines query.
        session: Active DB session.
        max_game_count: Cap to this many distinct games (0 = no cap).
    """
    loader = DatasetLoader(session)
    upcoming = loader.load_upcoming_player_lines(target_date)
    if max_game_count <= 0 or upcoming.empty or "game_id" not in upcoming.columns:
        return upcoming, None
    limited_ids = set(upcoming["game_id"].dropna().astype(int).head(max_game_count).tolist())
    return upcoming, limited_ids or None


def fetch_bdl_schedule(target_date: date) -> list[GamePayload]:
    """Return scheduled games from BallDontLie for target_date. Empty on failure."""
    try:
        return asyncio.run(_async_fetch_schedule(target_date))
    except Exception:
        _log.warning("BDL schedule fetch failed for %s.", target_date, exc_info=True)
        return []


def _log_bdl_game_count(target_date: date) -> int:
    games = fetch_bdl_schedule(target_date)
    count = len(games)
    _log.info("BDL schedule: %d game(s) on %s.", count, target_date)
    return count


async def _async_fetch_schedule(target_date: date) -> list[GamePayload]:
    provider = BallDontLieStatsProvider()
    _, games = await provider.fetch_schedule(target_date)
    return games
