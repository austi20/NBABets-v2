from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from app.config.settings import get_settings
from app.db.session import session_scope
from app.server.schemas.local_agent import LocalAgentPolicyRequest, LocalAgentStatusModel
from app.services.insights import build_local_ai_terminal_text, load_local_agent_status
from app.services.local_autonomy.policy_state import update_local_agent_policy_state

router = APIRouter(prefix="/api/local-agent", tags=["local-agent"])


@router.get("/status", response_model=LocalAgentStatusModel)
def local_agent_status() -> LocalAgentStatusModel:
    with session_scope() as session:
        status = load_local_agent_status(session)
    return LocalAgentStatusModel.from_dataclass(status)


@router.get("/terminal")
async def local_agent_terminal_stream(request: Request) -> EventSourceResponse:
    settings = get_settings()

    async def event_generator():
        previous_text = ""
        while True:
            if await request.is_disconnected():
                break
            with session_scope() as session:
                text = build_local_ai_terminal_text(
                    session,
                    endpoint=settings.ai_local_endpoint,
                    model=settings.ai_local_model,
                )
            if text != previous_text:
                previous_text = text
                yield {"event": "terminal", "data": text}
            await asyncio.sleep(1.0)

    return EventSourceResponse(event_generator())


@router.post("/policy", response_model=LocalAgentStatusModel)
def set_local_agent_policy(payload: LocalAgentPolicyRequest) -> LocalAgentStatusModel:
    if payload.policy == "enable":
        update_local_agent_policy_state(
            enabled=True,
            updated_by="server_api",
            note="policy_enable",
        )
    elif payload.policy == "disable":
        update_local_agent_policy_state(
            enabled=False,
            updated_by="server_api",
            note="policy_disable",
        )
    elif payload.policy == "safe_auto_enable":
        update_local_agent_policy_state(
            auto_execute_safe=True,
            updated_by="server_api",
            note="safe_auto_enable",
        )
    else:
        update_local_agent_policy_state(
            auto_execute_safe=False,
            updated_by="server_api",
            note="safe_auto_disable",
        )

    with session_scope() as session:
        status = load_local_agent_status(session)
    return LocalAgentStatusModel.from_dataclass(status)

