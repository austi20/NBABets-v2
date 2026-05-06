from __future__ import annotations

from app.db.base import Base
from app.db.session import get_engine
from app.models import all as _models  # noqa: F401


def create_all() -> None:
    Base.metadata.create_all(bind=get_engine())
