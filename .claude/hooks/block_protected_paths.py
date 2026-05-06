#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

PROTECTED_PATTERNS = (
    ".env",
    ".env.*",
    "secrets/**",
    "*.sqlite",
    "*.db",
    "data/**/*.sqlite",
    "data/**/*.db",
    "reports/**",
    "dist/**",
    "build/**",
    ".venv/**",
)

ALLOW_PREFIXES = (".claude/",)


def _project_root() -> Path:
    project_dir = os.environ.get("CLAUDE_PROJECT_DIR")
    if project_dir:
        return Path(project_dir).resolve()
    return Path(__file__).resolve().parents[2]


def _extract_paths(payload: dict[str, object]) -> list[Path]:
    tool_input = payload.get("tool_input")
    if not isinstance(tool_input, dict):
        return []
    file_path = tool_input.get("file_path")
    if isinstance(file_path, str) and file_path:
        return [Path(file_path)]
    return []


def _relative_text(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root).as_posix()
    except Exception:
        return path.as_posix()


def _is_protected(relative_path: str) -> bool:
    for prefix in ALLOW_PREFIXES:
        if relative_path.startswith(prefix):
            return False
    for pattern in PROTECTED_PATTERNS:
        if Path(relative_path).match(pattern):
            return True
    return False


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0

    root = _project_root()
    paths = _extract_paths(payload)
    if not paths:
        return 0

    protected: list[str] = []
    for path in paths:
        relative_path = _relative_text(path, root)
        if _is_protected(relative_path):
            protected.append(relative_path)

    if not protected:
        return 0

    result = {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": (
                "Blocked edit to protected path. Do not modify secrets, local databases, generated reports, "
                "or virtualenv files directly."
            ),
            "additionalContext": "Blocked paths: " + ", ".join(protected),
        }
    }
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
