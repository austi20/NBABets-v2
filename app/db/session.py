from __future__ import annotations

import sqlite3
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.config.settings import get_settings
from app.db.discovery import DatabaseSelection, select_best_database


# Apply performance pragmas to every SQLite connection at the driver level.
# WAL allows concurrent readers + one writer without blocking each other.
# synchronous=NORMAL is crash-safe while avoiding the full fsync on each commit.
# cache_size=-262144 grants ~256 MB page cache per connection (negative = KiB).
# mmap_size enables OS-level memory-mapping of the DB file for read-heavy paths.
@event.listens_for(Engine, "connect")
def _sqlite_pragmas(dbapi_conn: object, _connection_record: object) -> None:
    if not isinstance(dbapi_conn, sqlite3.Connection):
        return
    cursor = dbapi_conn.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA cache_size=-262144")
        cursor.execute("PRAGMA temp_store=MEMORY")
        cursor.execute("PRAGMA mmap_size=8589934592")
    finally:
        cursor.close()


SessionLocal = sessionmaker(autoflush=False, autocommit=False, future=True)
engine: Engine | None = None
database_selection: DatabaseSelection | None = None


def configure_engine(database_url: str | None = None) -> DatabaseSelection:
    global engine, database_selection
    database_selection = select_best_database(database_url or get_settings().database_url)
    connect_args = {"check_same_thread": False} if database_selection.url.startswith("sqlite") else {}
    engine = create_engine(
        database_selection.url,
        future=True,
        pool_pre_ping=True,
        connect_args=connect_args,
    )
    SessionLocal.configure(bind=engine)
    return database_selection


def get_engine() -> Engine:
    if engine is None:
        configure_engine()
    assert engine is not None
    return engine


def get_database_selection() -> DatabaseSelection:
    if database_selection is None:
        return configure_engine()
    return database_selection


def get_db_session() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def session_transaction() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
