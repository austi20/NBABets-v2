from __future__ import annotations

import time

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
