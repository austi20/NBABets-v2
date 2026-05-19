from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text

from app.db.session import configure_engine


@pytest.fixture()
def tmp_db(tmp_path: Path):
    db_file = tmp_path / "test_pragmas.sqlite"
    yield f"sqlite:///{db_file.as_posix()}"


def test_wal_journal_mode(tmp_db: str) -> None:
    configure_engine(tmp_db)
    from app.db.session import get_engine

    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("PRAGMA journal_mode")).scalar()
    assert result == "wal"


def test_synchronous_normal(tmp_db: str) -> None:
    configure_engine(tmp_db)
    from app.db.session import get_engine

    engine = get_engine()
    with engine.connect() as conn:
        # PRAGMA synchronous returns 1 for NORMAL
        result = conn.execute(text("PRAGMA synchronous")).scalar()
    assert result == 1
