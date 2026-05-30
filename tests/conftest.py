from __future__ import annotations

import pytest

from app.config import settings as settings_module
from app.db import session as db_session
from app.services import player_bias


def _reset_settings_cache() -> None:
    """Drop the lru_cached Settings so each test reads fresh env / monkeypatches."""
    settings_module.get_settings.cache_clear()
    # Per-player offset lookups cache a DB-derived value; clear so a result
    # computed against one test's database can't leak into another.
    player_bias.reset_caches()


def _reset_db_globals() -> None:
    """Reset the module-global engine/SessionLocal to the unconfigured baseline.

    Without this, a test that calls configure_engine() leaves SessionLocal bound
    to its database; later tests that open their own session via SessionLocal
    (e.g. app.services.player_bias) then read from the wrong database.
    """
    if db_session.engine is not None:
        db_session.engine.dispose()
    db_session.engine = None
    db_session.database_selection = None
    db_session.SessionLocal.configure(bind=None)


@pytest.fixture(autouse=True)
def _isolate_shared_state():
    """Reset process-global caches/bindings around every test.

    The settings lru_cache and the app.db.session module globals are shared for
    the whole test session; without this fixture a test that mutates env vars or
    configures a database engine leaks that state into later tests.
    """
    _reset_settings_cache()
    _reset_db_globals()
    try:
        yield
    finally:
        _reset_db_globals()
        _reset_settings_cache()
