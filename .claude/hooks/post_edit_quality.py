#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def _project_root() -> Path:
	project_dir = os.environ.get("CLAUDE_PROJECT_DIR")
	if project_dir:
		return Path(project_dir).resolve()
	return Path(__file__).resolve().parents[2]


def _extract_python_files(payload: dict[str, object], root: Path) -> list[str]:
	tool_input = payload.get("tool_input")
	if not isinstance(tool_input, dict):
		return []
	file_path = tool_input.get("file_path")
	if not isinstance(file_path, str) or not file_path.endswith(".py"):
		return []
	path = Path(file_path)
	try:
		rel = path.resolve().relative_to(root).as_posix()
	except Exception:
		rel = path.as_posix()
	return [rel]


def main() -> int:
	try:
		payload = json.load(sys.stdin)
	except json.JSONDecodeError:
		return 0

	root = _project_root()
	files = _extract_python_files(payload, root)
	if not files:
		return 0

	command = [sys.executable, "-m", "ruff", "check", *files]
	try:
		completed = subprocess.run(
			command,
			cwd=root,
			capture_output=True,
			text=True,
			timeout=25,
			check=False,
		)
	except Exception:
		return 0

	if completed.returncode == 0:
		return 0

	stdout = (completed.stdout or "").strip()
	stderr = (completed.stderr or "").strip()
	output = stdout or stderr
	lines = [line for line in output.splitlines() if line.strip()][:20]
	snippet = "\n".join(lines)
	result = {
		"hookSpecificOutput": {
			"hookEventName": "PostToolUse",
			"additionalContext": (
				"Targeted ruff check failed on edited Python files. Fix these issues before stopping.\n"
				+ snippet
			),
		}
	}
	print(json.dumps(result))
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
