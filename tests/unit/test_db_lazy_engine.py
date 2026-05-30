from __future__ import annotations

import pytest


def test_import_does_not_create_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    import app.db.session as session_module

    # The autouse isolation fixture (tests/conftest.py) resets the module
    # globals to the unconfigured baseline before each test, so no engine
    # should exist yet. Reloading the module here would fork SessionLocal and
    # leak a divergent sessionmaker into other tests.
    assert session_module.engine is None

    # The engine is created lazily on first use.
    engine = session_module.get_engine()
    assert engine is not None
    assert session_module.engine is engine
