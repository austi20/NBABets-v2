from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

from httpx import ASGITransport, AsyncClient

from app.server.main import create_app
from app.services.startup import StartupSnapshot, StartupStep


class _FakeStartupCoordinator:
    def __init__(self) -> None:
        self._run_id = "run-123"
        self._snapshots = [
            StartupSnapshot(
                progress_percent=10.0,
                eta_seconds=12.0,
                current_step="Refresh data feeds",
                current_detail="Pulling lines",
                database_message="db selected",
                board_date_message="Board date: pending",
                started_at=datetime.now(UTC),
                completed=False,
                failed=False,
                steps=[
                    StartupStep(
                        key="refresh_data",
                        label="Refresh data feeds",
                        weight=35,
                        estimated_seconds=15.0,
                        status="running",
                        message="Pulling lines",
                        progress_fraction=0.25,
                    )
                ],
                metrics={},
                opportunities=[],
                log_lines=["[12:00:00] Refresh data feeds: started"],
            ),
            StartupSnapshot(
                progress_percent=100.0,
                eta_seconds=0.0,
                current_step="Startup complete",
                current_detail="All startup steps completed",
                database_message="db selected",
                board_date_message="Board date: 2026-05-05",
                started_at=datetime.now(UTC),
                completed=True,
                failed=False,
                steps=[
                    StartupStep(
                        key="refresh_data",
                        label="Refresh data feeds",
                        weight=35,
                        estimated_seconds=15.0,
                        status="completed",
                        message="Completed",
                        progress_fraction=1.0,
                    )
                ],
                metrics={"predictions_generated": 42},
                opportunities=[],
                log_lines=["[12:00:03] Startup complete"],
            ),
        ]
        self._snapshot_calls = 0

    def run_async(self) -> str:
        return self._run_id

    def snapshot(self) -> StartupSnapshot:
        idx = min(self._snapshot_calls, len(self._snapshots) - 1)
        self._snapshot_calls += 1
        return self._snapshots[idx]


def test_startup_run_and_snapshot_endpoints() -> None:
    async def _run() -> None:
        app = create_app(startup_coordinator=_FakeStartupCoordinator())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            run_response = await client.post("/api/startup/run")
            assert run_response.status_code == 202
            assert run_response.json() == {"run_id": "run-123"}

            snapshot_response = await client.get("/api/startup/snapshot")
            assert snapshot_response.status_code == 200
            body = snapshot_response.json()
            assert body["current_step"] == "Refresh data feeds"
            assert body["steps"][0]["status"] == "running"

    asyncio.run(_run())


def test_startup_stream_emits_snapshot_diffs() -> None:
    async def _run() -> None:
        app = create_app(startup_coordinator=_FakeStartupCoordinator())
        transport = ASGITransport(app=app)
        events: list[dict[str, object]] = []
        async with AsyncClient(transport=transport, base_url="http://testserver", timeout=5.0) as client:
            async with client.stream("GET", "/api/startup/stream") as response:
                assert response.status_code == 200
                async for line in response.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    payload = json.loads(line.removeprefix("data: "))
                    events.append(payload)
                    if payload.get("completed"):
                        break
        assert len(events) >= 2
        assert events[0]["completed"] is False
        assert events[-1]["completed"] is True

    asyncio.run(_run())

