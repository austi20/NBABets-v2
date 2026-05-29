"""Tests for extreme sentinel switch from Prediction to StatProjection."""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.db.base import Base
from app.services.local_autonomy.debug_copilot import build_debug_hints


def _make_session() -> Session:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    return Session(engine)


def _seed_stat_projection(session: Session, *, dnp_risk: float, predicted_at: datetime) -> None:
    # Insert directly via core to avoid FK enforcement (SQLite FK off by default).
    # Must supply all NOT NULL columns that lack a DDL-level default.
    session.execute(
        text(
            "INSERT INTO stat_projections "
            "(model_run_id, game_id, player_id, market_id, predicted_at, "
            "projected_mean, projected_variance, projected_median, "
            "confidence_interval_low, confidence_interval_high, "
            "percentile_25, percentile_75, dnp_risk, "
            "boom_probability, bust_probability, availability_branches, "
            "data_sufficiency_tier, data_confidence_score, "
            "feature_attribution_summary, meta) "
            "VALUES (1, 1, 1, 1, :at, 20.0, 4.0, 20.0, 15.0, 25.0, "
            "18.0, 22.0, :dnp, 0.0, 0.0, 1, 'A', 1.0, '{}', '{}')"
        ),
        {"at": predicted_at.isoformat(), "dnp": dnp_risk},
    )
    session.commit()


def test_debug_copilot_fires_on_high_dnp_risk() -> None:
    session = _make_session()
    now = datetime.now(UTC)
    _seed_stat_projection(session, dnp_risk=0.97, predicted_at=now)

    hints = build_debug_hints(session=session, report_date=date.today(), lookback_hours=24)

    data_hints = [h for h in hints if h.category == "data"]
    assert data_hints, "Expected a 'data' hint when dnp_risk > 0.95"
    assert any("DNP" in h.summary or "extreme" in h.summary.lower() for h in data_hints)


def test_debug_copilot_no_hint_when_dnp_risk_low() -> None:
    session = _make_session()
    now = datetime.now(UTC)
    _seed_stat_projection(session, dnp_risk=0.50, predicted_at=now)

    hints = build_debug_hints(session=session, report_date=date.today(), lookback_hours=24)

    # Should not fire extreme sentinel — no data hint from sentinel
    sentinel_hints = [
        h for h in hints
        if h.category == "data" and ("DNP" in h.summary or "extreme" in h.summary.lower())
    ]
    assert not sentinel_hints
