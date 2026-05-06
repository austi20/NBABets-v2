from __future__ import annotations

import json

import pandas as pd
import pytest

from llm_train.dataset.projectors import build_csv_qa_gold, csv_row_to_messages, recalculate_hit_flags


@pytest.mark.parametrize(
    ("actual", "line", "expected"),
    [
        (10.0, 9.5, {"hit_over": True, "hit_under": False, "push": False}),
        (8.0, 9.5, {"hit_over": False, "hit_under": True, "push": False}),
        (9.5, 9.5, {"hit_over": False, "hit_under": False, "push": True}),
    ],
)
def test_recalculate_hit_flags(actual: float, line: float, expected: dict[str, bool]) -> None:
    assert recalculate_hit_flags(actual=actual, line=line) == expected


def test_build_csv_qa_gold_agrees() -> None:
    row = pd.Series(
        {
            "game_date": "2026-01-01",
            "line_value": 20.5,
            "actual": 22.0,
            "hit_over": True,
            "hit_under": False,
            "push": False,
            "over_odds": "-110",
            "under_odds": "-110",
            "minutes": 30.0,
        }
    )
    gold = build_csv_qa_gold(row)
    assert gold["agrees_with_file"] is True
    assert gold["recalculated"]["hit_over"] is True


def test_build_csv_qa_gold_mismatch() -> None:
    row = pd.Series(
        {
            "game_date": "2026-01-01",
            "line_value": 20.5,
            "actual": 22.0,
            "hit_over": False,
            "hit_under": True,
            "push": False,
            "over_odds": "",
            "under_odds": "",
            "minutes": 30.0,
        }
    )
    gold = build_csv_qa_gold(row)
    assert gold["agrees_with_file"] is False
    codes = {i["code"] for i in gold["issues"]}
    assert "hit_mismatch" in codes


def test_csv_row_to_messages_roundtrip_json() -> None:
    row = pd.Series(
        {
            "game_date": "2026-01-01",
            "game_id": 1,
            "player_name": "Test Player",
            "player_team": "TST",
            "opponent": "OPP",
            "home_team": "TST",
            "away_team": "OPP",
            "market": "Player Points",
            "sportsbook": "book",
            "line_value": 10.0,
            "actual": 12.0,
            "hit_over": True,
            "hit_under": False,
            "push": False,
            "over_odds": "-105",
            "under_odds": "-115",
            "minutes": 28.0,
            "source": "test",
        }
    )
    record = csv_row_to_messages(row, row_index=0)
    assistant = record["messages"][1]["content"]
    parsed = json.loads(assistant)
    assert "recalculated" in parsed
    assert record["meta"]["curriculum"] == "csv_qa"
