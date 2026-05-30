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
