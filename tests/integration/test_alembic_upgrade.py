from __future__ import annotations

import sqlite3


def test_alembic_upgrade_creates_projection_tables(tmp_path) -> None:
    db = tmp_path / "x.sqlite"

    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db}")

    command.upgrade(cfg, "head")

    connection = sqlite3.connect(db)
    try:
        cur = connection.cursor()
        tables = {
            row[0]
            for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "stat_projections" in tables
        assert "prop_line_evaluations" in tables

        cur.execute("PRAGMA table_info(trading_kill_switch)")
        cols = {row[1] for row in cur.fetchall()}
        assert "flatten" in cols

        kill_switch = cur.execute(
            "SELECT id, killed, flatten FROM trading_kill_switch WHERE id=1"
        ).fetchone()
        assert kill_switch == (1, 0, 0)
    finally:
        connection.close()


def _upgrade_fresh_db(tmp_path):
    db = tmp_path / "parity.sqlite"

    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db}")
    command.upgrade(cfg, "head")
    return db


def _orm_metadata():
    import app.models.all  # noqa: F401
    from app.db.base import Base
    from app.db.models import trading  # noqa: F401

    return Base.metadata


def test_alembic_upgrade_has_all_orm_tables(tmp_path) -> None:
    db = _upgrade_fresh_db(tmp_path)
    metadata = _orm_metadata()

    connection = sqlite3.connect(db)
    try:
        cur = connection.cursor()
        reflected = {
            row[0]
            for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    finally:
        connection.close()

    missing = set(metadata.tables) - reflected
    assert missing == set(), f"ORM tables missing from migrated DB: {sorted(missing)}"


def test_alembic_upgrade_column_parity(tmp_path) -> None:
    db = _upgrade_fresh_db(tmp_path)
    metadata = _orm_metadata()

    connection = sqlite3.connect(db)
    drift: dict[str, list[str]] = {}
    try:
        cur = connection.cursor()
        for table_name, table in metadata.tables.items():
            cur.execute(f"PRAGMA table_info({table_name})")
            reflected_cols = {row[1] for row in cur.fetchall()}
            expected_cols = {column.name for column in table.columns}
            missing_cols = expected_cols - reflected_cols
            if missing_cols:
                drift[table_name] = sorted(missing_cols)
    finally:
        connection.close()

    assert drift == {}, f"ORM columns missing from migrated DB: {drift}"
