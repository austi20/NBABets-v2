from __future__ import annotations

from pathlib import Path

from app.config.settings import get_settings
from app.server.main import create_app


def _point_app_data_dir(monkeypatch, tmp_path: Path) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "app_data_dir", tmp_path, raising=False)


def test_token_persists_across_create_app(monkeypatch, tmp_path: Path) -> None:
    _point_app_data_dir(monkeypatch, tmp_path)

    app1 = create_app()
    app2 = create_app()

    assert app1.state.app_token == app2.state.app_token
    assert app1.state.app_token
    assert (tmp_path / ".app_token").exists()
    assert (tmp_path / ".app_token").read_text(encoding="utf-8").strip() == app1.state.app_token


def test_rotate_regenerates_token(monkeypatch, tmp_path: Path) -> None:
    _point_app_data_dir(monkeypatch, tmp_path)

    original = create_app().state.app_token
    rotated = create_app(rotate_token=True).state.app_token

    assert rotated != original
    # The rotated token is now the persisted one.
    assert (tmp_path / ".app_token").read_text(encoding="utf-8").strip() == rotated
