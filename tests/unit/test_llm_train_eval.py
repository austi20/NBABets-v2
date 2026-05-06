from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from llm_train.eval.metrics import summarize_jsonl


@pytest.fixture
def work_dir() -> Path:
    base = Path(__file__).resolve().parents[2] / "temp"
    base.mkdir(parents=True, exist_ok=True)
    d = base / f"pytest_llm_eval_{uuid.uuid4().hex}"
    d.mkdir()
    return d


def test_summarize_jsonl_on_seeds(work_dir: Path) -> None:
    p = work_dir / "val.jsonl"
    autonomy = {
        "messages": [
            {"role": "user", "content": "u"},
            {"role": "assistant", "content": '{"status": "hold", "confidence": 0.5, "summary": "x", "actions": []}'},
        ],
        "meta": {"curriculum": "local_autonomy"},
    }
    bad_action = {
        "messages": [
            {"role": "user", "content": "u"},
            {
                "role": "assistant",
                "content": (
                    '{"status": "advisory", "confidence": 0.5, "summary": "x", '
                    '"actions": [{"action_type": "unknown_action", "reason": "r", "confidence": 0.5}]}'
                ),
            },
        ],
        "meta": {"curriculum": "local_autonomy"},
    }
    with p.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(autonomy) + "\n")
        handle.write(json.dumps(bad_action) + "\n")
    s = summarize_jsonl(p)
    assert s.total == 2
    assert s.autonomy_parse_ok == 2
    assert s.autonomy_action_violations == 1
