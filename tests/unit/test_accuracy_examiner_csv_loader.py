"""Unit tests for ``app.services.examiner.csv_loader``."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from textwrap import dedent

import pytest

from app.services.examiner.csv_loader import (
    _normalize_market,
    filter_by_markets,
    load_examiner_dataset,
    recent_real_examples,
)

_CSV_HEADER = (
    "game_date,game_id,player_name,player_team,opponent,home_team,away_team,"
    "market,sportsbook,line_value,over_odds,under_odds,actual,hit_over,hit_under,"
    "push,minutes,source"
)


def _write_csv(tmp_path: Path, rows: list[str]) -> Path:
    csv_path = tmp_path / "props.csv"
    csv_path.write_text("\n".join([_CSV_HEADER, *rows]) + "\n", encoding="utf-8")
    return csv_path


def test_normalize_market_maps_csv_tokens_to_canonical() -> None:
    assert _normalize_market("Player Points") == "points"
    assert _normalize_market("Player PRA") == "pra"
    assert _normalize_market("Player Threes") == "threes"
    assert _normalize_market("Player Assists") == "assists"
    assert _normalize_market("Player Rebounds") == "rebounds"
    assert _normalize_market("Player Turnovers") == "turnovers"
    # Already-canonical passes through.
    assert _normalize_market("points") == "points"
    # Unknown passes through lowercased so downstream filters can still drop it.
    assert _normalize_market("Player Blocks") == "player blocks"
    assert _normalize_market("") == ""


def test_load_examiner_dataset_real_only_default(tmp_path: Path) -> None:
    rows = [
        "2026-03-28,1,Real McCoy,NYK,PHI,NYK,PHI,Player Points,fanduel,22.5,-110,-110,24,True,False,False,33.1,real",
        "2026-01-03,2,Synth One,NYK,PHI,NYK,PHI,Player Points,synthetic,19.5,,,18,False,True,False,25.0,synthetic",
        "2026-03-29,3,Real Two,NYK,BOS,NYK,BOS,Player Assists,draftkings,5.5,-115,-105,6,True,False,False,30.4,real",
    ]
    csv_path = _write_csv(tmp_path, rows)

    dataset = load_examiner_dataset(csv_path)

    assert dataset.total == 3
    assert dataset.real_count == 2
    assert dataset.synthetic_count == 1
    # Only real rows retained because ``real_only=True`` is the default.
    assert len(dataset.examples) == 2
    assert all(example.source == "real" for example in dataset.examples)
    assert dataset.earliest_date == date(2026, 3, 28)
    assert dataset.latest_date == date(2026, 3, 29)
    # Mix ratio reflects the whole file, not the filtered slice.
    assert dataset.mix_ratio_real_vs_synthetic == pytest.approx(2 / 3)


def test_load_examiner_dataset_include_synthetic(tmp_path: Path) -> None:
    rows = [
        "2026-03-28,1,Real McCoy,NYK,PHI,NYK,PHI,Player Points,fanduel,22.5,-110,-110,24,True,False,False,33.1,real",
        "2026-01-03,2,Synth One,NYK,PHI,NYK,PHI,Player Points,synthetic,19.5,,,18,False,True,False,25.0,synthetic",
    ]
    csv_path = _write_csv(tmp_path, rows)

    dataset = load_examiner_dataset(csv_path, real_only=False)

    assert len(dataset.examples) == 2
    sources = {example.source for example in dataset.examples}
    assert sources == {"real", "synthetic"}
    # Markets were canonicalised even on synthetic rows.
    assert all(example.market == "points" for example in dataset.examples)


def test_load_examiner_dataset_skips_malformed_rows(tmp_path: Path) -> None:
    rows = [
        ",,,,,,,,,,,,,,,,,",  # empty player + date
        "2026-03-28,1,Player A,NYK,PHI,NYK,PHI,Player Points,fanduel,,-110,-110,24,True,False,False,33.1,real",  # missing line
        "2026-03-28,2,Player B,NYK,PHI,NYK,PHI,Player Points,fanduel,22.5,-110,-110,24,True,False,False,33.1,real",  # good
    ]
    csv_path = _write_csv(tmp_path, rows)

    dataset = load_examiner_dataset(csv_path)

    assert len(dataset.examples) == 1
    assert dataset.examples[0].player_name == "Player B"


def test_load_examiner_dataset_missing_columns_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.csv"
    bad.write_text("game_date,player_name\n2026-03-28,Foo\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing required columns"):
        load_examiner_dataset(bad)


def test_load_examiner_dataset_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_examiner_dataset(tmp_path / "nope.csv")


def test_recent_real_examples_slices_by_anchor(tmp_path: Path) -> None:
    rows = [
        "2026-03-25,1,Old Real,NYK,PHI,NYK,PHI,Player Points,fanduel,22.5,-110,-110,24,True,False,False,33.1,real",
        "2026-03-27,2,Mid Real,NYK,PHI,NYK,PHI,Player Points,fanduel,22.5,-110,-110,24,True,False,False,33.1,real",
        "2026-03-28,3,New Real,NYK,PHI,NYK,PHI,Player Points,fanduel,22.5,-110,-110,24,True,False,False,33.1,real",
    ]
    csv_path = _write_csv(tmp_path, rows)
    dataset = load_examiner_dataset(csv_path)

    recent = recent_real_examples(dataset, n_days=2)

    # Anchored on latest_date=2026-03-28 with a 2-day window → {03-27, 03-28}.
    names = {example.player_name for example in recent}
    assert names == {"Mid Real", "New Real"}


def test_recent_real_examples_with_explicit_reference(tmp_path: Path) -> None:
    rows = [
        "2026-03-25,1,Old Real,NYK,PHI,NYK,PHI,Player Points,fanduel,22.5,-110,-110,24,True,False,False,33.1,real",
        "2026-03-28,2,New Real,NYK,PHI,NYK,PHI,Player Points,fanduel,22.5,-110,-110,24,True,False,False,33.1,real",
    ]
    csv_path = _write_csv(tmp_path, rows)
    dataset = load_examiner_dataset(csv_path)

    # Explicit reference anchored on older date → only "Old Real" falls inside.
    recent = recent_real_examples(dataset, n_days=1, reference=date(2026, 3, 25))

    assert [example.player_name for example in recent] == ["Old Real"]


def test_recent_real_examples_empty_dataset_returns_empty() -> None:
    empty = load_examiner_dataset.__globals__["LabeledPropDataset"](
        examples=(),
        real_count=0,
        synthetic_count=0,
        earliest_date=None,
        latest_date=None,
        source_path="",
    )
    assert recent_real_examples(empty, n_days=5) == ()


def test_filter_by_markets_keeps_canonical(tmp_path: Path) -> None:
    rows = [
        "2026-03-28,1,A,NYK,PHI,NYK,PHI,Player Points,fanduel,22.5,-110,-110,24,True,False,False,33.1,real",
        "2026-03-28,2,B,NYK,PHI,NYK,PHI,Player Rebounds,fanduel,8.5,-110,-110,9,True,False,False,33.1,real",
    ]
    csv_path = _write_csv(tmp_path, rows)
    dataset = load_examiner_dataset(csv_path)

    filtered = filter_by_markets(dataset, ("points",))

    assert len(filtered.examples) == 1
    assert filtered.examples[0].market == "points"


def test_filter_by_markets_empty_tuple_passthrough(tmp_path: Path) -> None:
    rows = [
        "2026-03-28,1,A,NYK,PHI,NYK,PHI,Player Points,fanduel,22.5,-110,-110,24,True,False,False,33.1,real",
    ]
    csv_path = _write_csv(tmp_path, rows)
    dataset = load_examiner_dataset(csv_path)

    assert filter_by_markets(dataset, ()) is dataset


def test_load_examiner_dataset_header_only(tmp_path: Path) -> None:
    csv_path = tmp_path / "header_only.csv"
    csv_path.write_text(_CSV_HEADER + "\n", encoding="utf-8")

    dataset = load_examiner_dataset(csv_path)

    assert dataset.examples == ()
    assert dataset.real_count == 0
    assert dataset.synthetic_count == 0
    assert dataset.mix_ratio_real_vs_synthetic == 0.0
    assert dataset.earliest_date is None
    assert dataset.latest_date is None


# Keep the ``dedent`` import used so ruff does not flag it — this is a spot
# where future tests may want multi-line CSV fixtures.
_ = dedent
