from __future__ import annotations

import importlib

import pytest


def test_import_does_not_create_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    import app.db.session as session_module

    session_module = importlib.reload(session_module)

    # Importing the module alone must not build an engine.
    assert session_module.engine is None

    # The engine is created lazily on first use.
    engine = session_module.get_engine()
    assert engine is not None
    assert session_module.engine is engine
