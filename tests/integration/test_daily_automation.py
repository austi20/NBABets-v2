from __future__ import annotations

import os
import subprocess
import sys
import uuid
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models import all as _models  # noqa: F401
from app.models.all import ModelRun, RawPayload

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def work_dir() -> Path:
    root = PROJECT_ROOT / "temp" / f"pytest_daily_automation_{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    return root


@pytest.fixture
def clear_settings_cache():
    from app.config.settings import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _configure_env(monkeypatch: pytest.MonkeyPatch, reports_dir: Path, db_url: str) -> None:
    monkeypatch.setenv("DATABASE_URL", db_url)
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("AI_LOCAL_ENDPOINT", "http://127.0.0.1:9/v1/chat/completions")
    monkeypatch.setenv("INJURY_PROVIDER", "balldontlie")
    monkeypatch.setenv("BALLDONTLIE_API_KEY", "unit-bdl")
    monkeypatch.setenv("LOCAL_AGENT_POLICY_STATE_PATH", str(reports_dir / "local_agent_policy.json"))
    monkeypatch.setenv("LOCAL_AUTONOMY_ENABLED", "false")


def test_generate_report_recommend_includes_trends_and_preflight(
    clear_settings_cache,
    work_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.config.settings import get_settings
    from app.services.automation import generate_daily_automation_report

    db_path = work_dir / "auto.sqlite"
    db_url = f"sqlite:///{db_path.resolve().as_posix()}"
    _configure_env(monkeypatch, work_dir, db_url)
    get_settings.cache_clear()

    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    report_date = date(2026, 4, 5)
    day_start = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)
    session.add_all(
        [
            RawPayload(
                provider_type="stats",
                provider_name="nba_api",
                endpoint="/t",
                fetched_at=day_start,
                content_hash="a",
                payload={},
            ),
            RawPayload(
                provider_type="odds",
                provider_name="nba_api",
                endpoint="/o",
                fetched_at=day_start,
                content_hash="b",
                payload={},
            ),
            RawPayload(
                provider_type="injuries",
                provider_name="balldontlie",
                endpoint="/i",
                fetched_at=day_start,
                content_hash="inj-a",
                payload={},
            ),
            ModelRun(
                model_version="v1",
                feature_version="v1",
                started_at=day_start,
                completed_at=day_start,
                metrics={"calibration_diagnostics": {"points": {"ece": 0.05}}},
            ),
        ]
    )
    session.commit()

    path = generate_daily_automation_report(
        session,
        target_date=report_date,
        agent_mode="recommend",
        dry_run=True,
    )
    text = path.read_text(encoding="utf-8")
    assert "## Automation Health Trends" in text
    assert "### Deterioration alerts" in text
    assert "- tier: A" in text
    assert "preflight_ok: True" in text
    assert "## Local Autonomy" in text


def test_generate_report_recommend_forces_dry_run_when_caller_passes_false(
    clear_settings_cache,
    work_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Programmatic callers cannot bypass advisory-only recommend mode with dry_run=False."""
    from app.config.settings import get_settings
    from app.services.automation import generate_daily_automation_report

    db_path = work_dir / "auto_recommend_guard.sqlite"
    db_url = f"sqlite:///{db_path.resolve().as_posix()}"
    _configure_env(monkeypatch, work_dir, db_url)
    get_settings.cache_clear()

    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    report_date = date(2026, 4, 5)
    day_start = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)
    session.add_all(
        [
            RawPayload(
                provider_type="stats",
                provider_name="nba_api",
                endpoint="/t",
                fetched_at=day_start,
                content_hash="a",
                payload={},
            ),
            RawPayload(
                provider_type="odds",
                provider_name="nba_api",
                endpoint="/o",
                fetched_at=day_start,
                content_hash="b",
                payload={},
            ),
            RawPayload(
                provider_type="injuries",
                provider_name="balldontlie",
                endpoint="/i",
                fetched_at=day_start,
                content_hash="inj-b",
                payload={},
            ),
            ModelRun(
                model_version="v1",
                feature_version="v1",
                started_at=day_start,
                completed_at=day_start,
                metrics={"calibration_diagnostics": {"points": {"ece": 0.05}}},
            ),
        ]
    )
    session.commit()

    path = generate_daily_automation_report(
        session,
        target_date=report_date,
        agent_mode="recommend",
        dry_run=False,
    )
    text = path.read_text(encoding="utf-8")
    assert "- mode: recommend" in text
    assert "- dry_run: True" in text


def test_generate_report_auto_dry_run_flag_in_output(
    clear_settings_cache,
    work_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.config.settings import get_settings
    from app.services.automation import generate_daily_automation_report

    db_path = work_dir / "auto2.sqlite"
    db_url = f"sqlite:///{db_path.resolve().as_posix()}"
    _configure_env(monkeypatch, work_dir, db_url)
    get_settings.cache_clear()

    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    report_date = date(2026, 4, 5)
    day_start = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)
    session.add_all(
        [
            RawPayload(
                provider_type="stats",
                provider_name="nba_api",
                endpoint="/t",
                fetched_at=day_start,
                content_hash="a",
                payload={},
            ),
            RawPayload(
                provider_type="odds",
                provider_name="nba_api",
                endpoint="/o",
                fetched_at=day_start,
                content_hash="b",
                payload={},
            ),
            RawPayload(
                provider_type="injuries",
                provider_name="balldontlie",
                endpoint="/i",
                fetched_at=day_start,
                content_hash="inj-c",
                payload={},
            ),
            ModelRun(
                model_version="v1",
                feature_version="v1",
                started_at=day_start,
                completed_at=day_start,
                metrics={},
            ),
        ]
    )
    session.commit()

    path = generate_daily_automation_report(
        session,
        target_date=report_date,
        agent_mode="auto",
        dry_run=True,
    )
    text = path.read_text(encoding="utf-8")
    assert "dry_run: True" in text
    assert "## Agent Control Plane" in text


def test_run_daily_automation_script_recommend_smoke(
    clear_settings_cache,
    work_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.config.settings import get_settings

    db_path = work_dir / "cli.sqlite"
    db_url = f"sqlite:///{db_path.resolve().as_posix()}"
    _configure_env(monkeypatch, work_dir, db_url)
    get_settings.cache_clear()

    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    engine.dispose()

    env = os.environ.copy()
    env["DATABASE_URL"] = db_url
    env["REPORTS_DIR"] = str(work_dir)
    env["AI_LOCAL_ENDPOINT"] = "http://127.0.0.1:9/v1/chat/completions"
    env["INJURY_PROVIDER"] = "balldontlie"
    env["BALLDONTLIE_API_KEY"] = "unit-bdl"

    subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "run_daily_automation.py"),
            "--agent-mode",
            "recommend",
        ],
        cwd=str(PROJECT_ROOT),
        env=env,
        check=True,
        timeout=120,
    )
    assert list(work_dir.glob("automation_daily_*.md"))


def test_run_daily_automation_script_auto_dry_run_smoke(
    clear_settings_cache,
    work_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.config.settings import get_settings

    db_path = work_dir / "cli2.sqlite"
    db_url = f"sqlite:///{db_path.resolve().as_posix()}"
    _configure_env(monkeypatch, work_dir, db_url)
    get_settings.cache_clear()

    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    engine.dispose()

    env = os.environ.copy()
    env["DATABASE_URL"] = db_url
    env["REPORTS_DIR"] = str(work_dir)
    env["AI_LOCAL_ENDPOINT"] = "http://127.0.0.1:9/v1/chat/completions"
    env["INJURY_PROVIDER"] = "balldontlie"
    env["BALLDONTLIE_API_KEY"] = "unit-bdl"

    subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "run_daily_automation.py"),
            "--agent-mode",
            "auto",
            "--dry-run",
        ],
        cwd=str(PROJECT_ROOT),
        env=env,
        check=True,
        timeout=120,
    )
    reports = list(work_dir.glob("automation_daily_*.md"))
    assert reports
    assert "dry_run: True" in reports[-1].read_text(encoding="utf-8")
