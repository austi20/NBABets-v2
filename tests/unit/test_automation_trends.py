from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models import all as _models  # noqa: F401
from app.models.all import ModelRun
from app.services.automation_trends import (
    ECE_DETERIORATION_DELTA,
    WATCH_MARKET_ECE_DELTAS,
    build_automation_trend_snapshot,
    parse_api_tier_from_report_markdown,
)


def test_parse_api_tier_from_report() -> None:
    md = "## API Coverage Tier\n- tier: B\n- summary: x\n"
    assert parse_api_tier_from_report_markdown(md) == "B"


def test_ece_deterioration_alert() -> None:
    reports_dir = Path(__file__).resolve().parents[2] / "temp" / "automation_trend_reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    t1 = datetime(2026, 1, 2, 12, 0, tzinfo=UTC)
    diag_low = {"points": {"ece": 0.04}}
    diag_high = {"points": {"ece": 0.04 + ECE_DETERIORATION_DELTA + 0.005}}
    session.add_all(
        [
            ModelRun(
                model_version="v1",
                feature_version="v1",
                started_at=t1,
                completed_at=t1,
                metrics={"calibration_diagnostics": diag_high},
            ),
            ModelRun(
                model_version="v1",
                feature_version="v1",
                started_at=t0,
                completed_at=t0,
                metrics={"calibration_diagnostics": diag_low},
            ),
        ]
    )
    session.commit()
    snap = build_automation_trend_snapshot(session, reports_dir, report_date=t1.date())
    assert any("ECE worsened" in a for a in snap.alerts)


def test_data_quality_degraded_alert() -> None:
    reports_dir = Path(__file__).resolve().parents[2] / "temp" / "automation_trend_reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    t1 = datetime(2026, 1, 2, 12, 0, tzinfo=UTC)
    session.add_all(
        [
            ModelRun(
                model_version="v1",
                feature_version="v1",
                started_at=t1,
                completed_at=t1,
                metrics={"training_data_quality": {"status": "degraded"}},
            ),
            ModelRun(
                model_version="v1",
                feature_version="v1",
                started_at=t0,
                completed_at=t0,
                metrics={"training_data_quality": {"status": "ok"}},
            ),
        ]
    )
    session.commit()
    snap = build_automation_trend_snapshot(session, reports_dir, report_date=t1.date())
    assert any("data quality" in a.lower() for a in snap.alerts)


def test_watch_market_ece_deterioration_alert() -> None:
    reports_dir = Path(__file__).resolve().parents[2] / "temp" / "automation_trend_reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    t1 = datetime(2026, 1, 2, 12, 0, tzinfo=UTC)
    delta = WATCH_MARKET_ECE_DELTAS["turnovers"] + 0.005
    session.add_all(
        [
            ModelRun(
                model_version="v1",
                feature_version="v1",
                started_at=t1,
                completed_at=t1,
                metrics={"calibration_diagnostics": {"turnovers": {"ece": 0.05 + delta}}},
            ),
            ModelRun(
                model_version="v1",
                feature_version="v1",
                started_at=t0,
                completed_at=t0,
                metrics={"calibration_diagnostics": {"turnovers": {"ece": 0.05}}},
            ),
        ]
    )
    session.commit()
    snap = build_automation_trend_snapshot(session, reports_dir, report_date=t1.date())
    assert any("TURNOVERS ECE worsened" in a for a in snap.alerts)
