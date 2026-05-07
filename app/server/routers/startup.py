from __future__ import annotations

import asyncio
from typing import Annotated, Any, cast

from fastapi import APIRouter, Body, Request, Response, status
from sse_starlette.sse import EventSourceResponse

from app.server.schemas.startup import StartupRunRequest, StartupSnapshotModel
from app.services.startup import StartupCoordinator

router = APIRouter(prefix="/api/startup", tags=["startup"])


def _coordinator_from_request(request: Request) -> StartupCoordinator:
    return cast(StartupCoordinator, request.app.state.startup_coordinator)


@router.post("/run", status_code=status.HTTP_202_ACCEPTED)
def run_startup(
    request: Request,
    body: Annotated[StartupRunRequest | None, Body()] = None,
) -> dict[str, str]:
    coordinator = _coordinator_from_request(request)
    effective = body if body is not None else StartupRunRequest()
    run_id = coordinator.run_async(full_refresh=bool(effective.full_refresh))
    response = {"run_id": run_id}
    return response


@router.get("/snapshot", response_model=StartupSnapshotModel)
def startup_snapshot(request: Request) -> StartupSnapshotModel:
    coordinator = _coordinator_from_request(request)
    snapshot = coordinator.snapshot()
    if (
        not snapshot.completed
        and not snapshot.failed
        and snapshot.current_step == "Waiting to start"
    ):
        coordinator.run_async()
        snapshot = coordinator.snapshot()
    return StartupSnapshotModel.from_dataclass(snapshot)


@router.get("/stream")
async def startup_stream(request: Request) -> Response:
    coordinator = _coordinator_from_request(request)

    async def event_generator() -> Any:
        previous_payload = ""
        while True:
            if await request.is_disconnected():
                break

            snapshot = coordinator.snapshot()
            payload = StartupSnapshotModel.from_dataclass(snapshot)
            serialized = payload.model_dump_json()
            if serialized != previous_payload:
                previous_payload = serialized
                yield {"event": "snapshot", "data": serialized}

            if snapshot.completed or snapshot.failed:
                break
            await asyncio.sleep(0.2)

    return EventSourceResponse(event_generator())

