from __future__ import annotations

import time
from pathlib import Path

from app.config import settings as settings_module
from app.services.startup import StartupCoordinator, StartupStep


def test_calculate_progress_blends_completed_and_running_steps() -> None:
    coordinator = StartupCoordinator()
    now = time.perf_counter()
    steps = [
        StartupStep("a", "A", weight=30, estimated_seconds=10.0, status="completed", progress_fraction=1.0),
        StartupStep(
            "b",
            "B",
            weight=40,
            estimated_seconds=10.0,
            status="running",
            progress_fraction=0.5,
            started_at=now - 5,
        ),
        StartupStep("c", "C", weight=30, estimated_seconds=10.0, status="pending"),
    ]

    progress = coordinator._calculate_progress(steps)
    assert progress == 50.0


def test_calculate_eta_returns_none_for_zero_progress() -> None:
    coordinator = StartupCoordinator()
    steps = [StartupStep("a", "A", weight=100, estimated_seconds=10.0, status="pending")]
    assert coordinator._calculate_eta(steps, progress_percent=0.0) is None


def test_startup_coordinator_includes_optional_local_ai_step() -> None:
    coordinator = StartupCoordinator()

    step_keys = [step.key for step in coordinator._steps]

    assert step_keys[:4] == ["discover_db", "initialize_db", "start_local_ai", "refresh_data"]
    assert "start_local_ai" in coordinator._optional_steps


def test_startup_coordinator_queues_full_refresh_when_running() -> None:
    coordinator = StartupCoordinator()

    class _AliveThread:
        def is_alive(self) -> bool:
            return True

    class _RunnerStub:
        def __init__(self) -> None:
            self.messages: list[str] = []

        def _append_log_line(self, message: str) -> None:
            self.messages.append(message)

    runner_stub = _RunnerStub()
    coordinator._thread = _AliveThread()  # type: ignore[assignment]
    coordinator._runner = runner_stub  # type: ignore[assignment]

    coordinator.start(full_refresh=True)

    assert coordinator._pending_full_refresh is True
    assert any("Full refresh requested while startup is running" in msg for msg in runner_stub.messages)


def test_resolve_env_files_includes_parent_chain(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "repo_root"
    nested = root / "desktop_tauri" / "src-tauri"
    nested.mkdir(parents=True)
    (root / ".env").write_text("AI_LOCAL_ENDPOINT=http://127.0.0.1:8080/v1/chat/completions\n", encoding="utf-8")
    monkeypatch.chdir(nested)

    env_files = settings_module._resolve_env_files()

    assert str(root / ".env") in env_files
