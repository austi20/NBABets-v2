from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

from app.services import startup_cache as startup_cache_module
from app.services.startup_cache import StartupComputationCacheService


@dataclass
class _FakeResult:
    rows: list[tuple]

    def all(self) -> list[tuple]:
        return self.rows


class _FakeUrl:
    def render_as_string(self, **_: object) -> str:
        return "sqlite:///test.db"


class _FakeBind:
    url = _FakeUrl()


class _FakeSession:
    bind = _FakeBind()

    def __init__(self, rows: list[tuple] | None = None) -> None:
        self.rows = rows or []
        self.last_statement = None

    def execute(self, statement: object) -> _FakeResult:
        self.last_statement = statement
        return _FakeResult(self.rows)


def _paths(root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        minutes_model=root / "minutes_model.joblib",
        stat_models=root / "stat_models.joblib",
        calibrators=root / "calibrators.joblib",
        metadata=root / "metadata.joblib",
    )


def test_training_decide_rejects_incomplete_artifacts(monkeypatch) -> None:
    session = _FakeSession()
    service = StartupComputationCacheService(session)
    temp_root = Path(tempfile.mkdtemp(prefix="startup-cache-test-", dir="."))
    try:
        artifact_paths = _paths(temp_root)
        artifact_paths.metadata.write_text("placeholder", encoding="utf-8")

        monkeypatch.setattr(startup_cache_module, "artifact_paths", lambda *_args, **_kwargs: artifact_paths)
        monkeypatch.setattr(startup_cache_module, "artifact_exists", lambda path: path == artifact_paths.metadata)

        decision = service.training_decide()

        assert decision.use_cached_result is False
        assert "Incomplete cached model artifacts" in decision.reason
        assert decision.metrics["artifacts_found"] == 1
        assert decision.metrics["artifacts_expected"] == 4
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_predictions_decide_scopes_to_current_model_run(monkeypatch) -> None:
    today = date.today()
    session = _FakeSession(
        rows=[
            (
                1,
                datetime.now(UTC),
                123,
                today,
                datetime.now(UTC),
            )
        ]
    )
    service = StartupComputationCacheService(session)
    temp_root = Path(tempfile.mkdtemp(prefix="startup-cache-test-", dir="."))
    try:
        artifact_paths = _paths(temp_root)

        monkeypatch.setattr(startup_cache_module, "artifact_paths", lambda *_args, **_kwargs: artifact_paths)
        monkeypatch.setattr(startup_cache_module, "load_artifact", lambda _path: {"model_run_id": 99})
        monkeypatch.setattr(service, "_expected_prediction_rows", lambda _target_date: 1)

        decision = service.predictions_decide(today)

        assert decision.use_cached_result is True
        assert decision.metrics["model_run_id"] == 99
        assert "model_run_id" in str(session.last_statement)
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)

