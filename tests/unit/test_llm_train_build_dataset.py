from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from llm_train.dataset.build_dataset import append_seeds, build_from_csv


@pytest.fixture
def work_dir() -> Path:
    base = Path(__file__).resolve().parents[2] / "temp"
    base.mkdir(parents=True, exist_ok=True)
    d = base / f"pytest_llm_train_{uuid.uuid4().hex}"
    d.mkdir()
    return d


def test_build_from_csv_date_split(work_dir: Path) -> None:
    csv_path = work_dir / "t.csv"
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
            {
                "game_date": "2026-04-10",
                "game_id": 2,
                "player_name": "B",
                "player_team": "X",
                "opponent": "Y",
                "home_team": "X",
                "away_team": "Y",
                "market": "Player Points",
                "sportsbook": "s",
                "line_value": 10.0,
                "over_odds": "",
                "under_odds": "",
                "actual": 9.0,
                "hit_over": False,
                "hit_under": True,
                "push": False,
                "minutes": 20.0,
                "source": "t",
            },
        ]
    )
    df.to_csv(csv_path, index=False)
    train, val = build_from_csv(
        csv_path,
        val_from=date.fromisoformat("2026-03-01"),
        curricula={"csv_qa"},
        max_rows=None,
    )
    assert len(train) == 1
    assert len(val) == 1
    assert val[0]["meta"]["game_date"] == "2026-04-10"


def test_append_seeds_adds_rows(work_dir: Path) -> None:
    seed = work_dir / "seed_autonomy.jsonl"
    seed.write_text(
        '{"messages": [{"role": "user", "content": "x"}, {"role": "assistant", "content": "{}"}], '
        '"meta": {"curriculum": "local_autonomy"}}\n',
        encoding="utf-8",
    )
    train: list = []
    append_seeds(train, work_dir, {"local_autonomy"})
    assert len(train) == 1
