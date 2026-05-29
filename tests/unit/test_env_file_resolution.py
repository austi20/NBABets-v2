from __future__ import annotations

from pathlib import Path

from app.config.settings import _discover_env_files


def test_env_walk_stops_at_project_root(tmp_path: Path) -> None:
    outer = tmp_path / "outer"
    project = outer / "project"
    subdir = project / "subdir"
    subdir.mkdir(parents=True)

    outer_env = outer / ".env"
    outer_env.write_text("SENTINEL=outer\n", encoding="utf-8")
    (project / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    project_env = project / ".env"
    project_env.write_text("SENTINEL=project\n", encoding="utf-8")

    found = _discover_env_files(subdir)

    assert project_env in found
    assert outer_env not in found
