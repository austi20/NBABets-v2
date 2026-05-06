from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models.all import RawPayload


class DbMaintenanceService:
    def __init__(self, session: Session) -> None:
        self._session = session

    def prune_old_raw_payloads(self, retention_days: int) -> int:
        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        deleted = (
            self._session.query(RawPayload)
            .filter(RawPayload.fetched_at < cutoff)
            .delete(synchronize_session=False)
        )
        self._session.commit()
        return int(deleted or 0)

    def vacuum_and_analyze(self) -> bool:
        # SQLite maintenance; safe no-op on unsupported engines.
        try:
            self._session.execute(text("VACUUM"))
            self._session.execute(text("ANALYZE"))
            self._session.commit()
            return True
        except Exception:
            self._session.rollback()
            return False
