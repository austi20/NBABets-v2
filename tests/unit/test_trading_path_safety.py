from __future__ import annotations

from pathlib import Path

import pytest

from app.server.routers import trading


def test_resolve_under_app_data_allows_inside(tmp_path: Path) -> None:
    result = trading._resolve_under_app_data(tmp_path, "trading_selections.json")
    assert result == (tmp_path / "trading_selections.json").resolve()


def test_resolve_under_app_data_allows_base_itself(tmp_path: Path) -> None:
    result = trading._resolve_under_app_data(tmp_path, ".")
    assert result == tmp_path.resolve()


def test_resolve_under_app_data_rejects_escape(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        trading._resolve_under_app_data(tmp_path, "../../etc/passwd")


def test_resolve_under_app_data_rejects_absolute_outside(tmp_path: Path) -> None:
    outside = (tmp_path.parent / "elsewhere" / "secrets.json").resolve()
    with pytest.raises(ValueError):
        trading._resolve_under_app_data(tmp_path, outside)


def test_resolve_under_app_data_allows_absolute_inside(tmp_path: Path) -> None:
    inside = tmp_path / "sub" / "file.json"
    result = trading._resolve_under_app_data(tmp_path, inside)
    assert result == inside.resolve()


def test_selections_path_anchored_to_app_data_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _FakeSettings:
        app_data_dir = tmp_path

    monkeypatch.setattr(trading, "get_settings", lambda: _FakeSettings())

    class _FakeRequest:
        pass

    result = trading._selections_path(_FakeRequest())  # type: ignore[arg-type]
    assert result == (tmp_path / "trading_selections.json").resolve()


def test_limits_path_allows_configured_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # trading_limits_path is resolved absolute by settings and lives outside
    # app_data_dir by design (under the runtime root). It must still be allowed.
    limits_file = (tmp_path / "config" / "trading_limits.json").resolve()

    class _FakeSettings:
        trading_limits_path = str(limits_file)

    monkeypatch.setattr(trading, "get_settings", lambda: _FakeSettings())

    result = trading._limits_path()
    assert result == limits_file


def test_limits_path_rejects_traversal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _FakeSettings:
        trading_limits_path = str(tmp_path / "config" / ".." / ".." / "etc" / "passwd")

    monkeypatch.setattr(trading, "get_settings", lambda: _FakeSettings())

    with pytest.raises(ValueError):
        trading._limits_path()
