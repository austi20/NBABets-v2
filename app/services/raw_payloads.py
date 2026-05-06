from __future__ import annotations

import hashlib
from datetime import date, datetime
from typing import Any

from sqlalchemy.orm import Session

from app.models.reference import RawPayload


def _make_json_safe(obj: Any) -> Any:
    """Recursively convert any non-JSON-serializable values to safe types.

    Python's stdlib json.dumps (used by SQLAlchemy's JSON column type) does not
    handle datetime/date objects.  This walks the structure and converts them to
    ISO-format strings so the payload can always be stored without errors.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_json_safe(item) for item in obj]
    return obj


class RawPayloadService:
    def __init__(self, session: Session) -> None:
        self._session = session

    def store(
        self,
        provider_type: str,
        provider_name: str,
        endpoint: str,
        fetched_at: datetime,
        payload: dict[str, Any],
    ) -> RawPayload:
        safe_payload = _make_json_safe(payload)
        content_hash = hashlib.sha256(str(safe_payload).encode("utf-8")).hexdigest()
        existing = (
            self._session.query(RawPayload)
            .filter(RawPayload.provider_name == provider_name, RawPayload.content_hash == content_hash)
            .one_or_none()
        )
        if existing is not None:
            return existing
        raw_payload = RawPayload(
            provider_type=provider_type,
            provider_name=provider_name,
            endpoint=endpoint,
            fetched_at=fetched_at,
            content_hash=content_hash,
            payload=safe_payload,
        )
        self._session.add(raw_payload)
        self._session.flush()
        return raw_payload

