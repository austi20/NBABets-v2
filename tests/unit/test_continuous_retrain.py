from __future__ import annotations

import json
import uuid
from pathlib import Path

import pandas as pd
import pytest

from llm_train.scripts.continuous_retrain import main


@pytest.fixture
def work_dir() -> Path:
    base = Path(__file__).resolve().parents[2] / "temp"
    base.mkdir(parents=True, exist_ok=True)
    d = base / f"pytest_continuous_{uuid.uuid4().hex}"
    d.mkdir()
    return d


def test_continuous_retrain_execute_writes_state(work_dir: Path) -> None:
    csv_path = work_dir / "p.csv"
    df = pd.DataFrame(
        [
            {
                "game_date": "2026-01-01",
                "game_id": 1,
                "player_name": "A",
                "player_team": "X",
                "opponent": "Y",
                "home_team": "X",
                "away_team": "Y",
                "market": "Player Points",
                "sportsbook": "s",
                "line_value": 10.0,
                "over_odds": "",
                "under_odds": "",
                "actual": 11.0,
                "hit_over": True,
                "hit_under": False,
                "push": False,
                "minutes": 20.0,
                "source": "t",
            },
        ]
    )
    df.to_csv(csv_path, index=False)
    state = work_dir / "state.json"
    out_dir = work_dir / "out"
    rc = main(
        [
            "--csv",
            str(csv_path),
            "--state-file",
            str(state),
            "--out-dir",
            str(out_dir),
            "--val-from",
            "2026-03-01",
            "--curricula",
            "csv_qa",
            "--execute",
        ]
    )
    assert rc == 0
    assert state.is_file()
    body = json.loads(state.read_text(encoding="utf-8"))
    assert "csv_mtime_utc" in body
    assert (out_dir / "train.jsonl").is_file()
