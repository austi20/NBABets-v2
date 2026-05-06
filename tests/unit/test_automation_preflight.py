from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models import all as _models  # noqa: F401
from app.services.automation_preflight import verify_agent_run_events_table


def test_preflight_ok_when_table_exists() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    ok, msg = verify_agent_run_events_table(session)
    assert ok is True
    assert "present" in msg


def test_preflight_fails_when_inspector_reports_missing() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    inspector = MagicMock()
    inspector.has_table.return_value = False
    with patch("app.services.automation_preflight.inspect", return_value=inspector):
        ok, msg = verify_agent_run_events_table(session)
    assert ok is False
    assert "missing table" in msg
