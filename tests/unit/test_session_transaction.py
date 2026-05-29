from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text

from app.db.session import configure_engine, session_scope, session_transaction


@pytest.fixture()
def tmp_db(tmp_path: Path):
    db_file = tmp_path / "test_txn.sqlite"
    url = f"sqlite:///{db_file.as_posix()}"
    configure_engine(url)
    with session_scope() as session:
        session.execute(text("CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT)"))
        session.commit()
    yield url


def _count_widgets() -> int:
    with session_scope() as session:
        return session.execute(text("SELECT COUNT(*) FROM widget")).scalar_one()


def test_session_transaction_commits_on_success(tmp_db: str) -> None:
    with session_transaction() as session:
        session.execute(text("INSERT INTO widget (name) VALUES ('a')"))

    assert _count_widgets() == 1


def test_session_transaction_rolls_back_on_exception(tmp_db: str) -> None:
    with pytest.raises(RuntimeError):
        with session_transaction() as session:
            session.execute(text("INSERT INTO widget (name) VALUES ('b')"))
            raise RuntimeError("boom")

    assert _count_widgets() == 0
