"""Guard that eval_daily.ps1 short-circuits when a python step fails.

A live powershell-invocation test is impractical here (it would run the real
training pipeline). Instead we assert the script source guards $LASTEXITCODE
after every python invocation, so a non-zero exit halts the chain.
"""
from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT = _REPO_ROOT / "scripts" / "eval_daily.ps1"


def test_every_python_call_is_followed_by_exitcode_guard() -> None:
    lines = [ln.strip() for ln in _SCRIPT.read_text(encoding="utf-8").splitlines() if ln.strip()]
    python_indices = [i for i, ln in enumerate(lines) if re.match(r"^python\s", ln)]
    assert python_indices, "expected at least one python invocation"
    guard = re.compile(r"\$LASTEXITCODE\s*-ne\s*0")
    for i in python_indices:
        following = lines[i + 1] if i + 1 < len(lines) else ""
        assert guard.search(following), (
            f"python call '{lines[i]}' is not immediately followed by a "
            f"$LASTEXITCODE guard (got '{following}')"
        )
