from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.config.settings import get_settings
from app.db.discovery import DatabaseSelection, select_best_database

settings = get_settings()
SessionLocal = sessionmaker(autoflush=False, autocommit=False, future=True)
engine: Engine | None = None
database_selection: DatabaseSelection | None = None


def configure_engine(database_url: str | None = None) -> DatabaseSelection:
    global engine, database_selection
    database_selection = select_best_database(database_url or settings.database_url)
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


configure_engine()
