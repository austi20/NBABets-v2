from __future__ import annotations

from sqlalchemy import inspect
from sqlalchemy.orm import Session


def verify_agent_run_events_table(session: Session) -> tuple[bool, str]:
    """Return whether ``agent_run_events`` exists on the bound database.

    Agent telemetry and API coverage tier logic depend on this table. Older
    deployments that have not run ``create_all`` / migrations must add it
    before ``AGENT_MODE`` is set to ``recommend`` or ``auto``.
    """
    bind = session.get_bind()
    if bind is None:
        return False, "session has no database bind"
    try:
        has_table = inspect(bind).has_table("agent_run_events")
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"schema inspection failed: {exc}"
    if has_table:
        return True, "agent_run_events present"
    return False, "missing table agent_run_events (run DB bootstrap / migrations before agent mode)"
