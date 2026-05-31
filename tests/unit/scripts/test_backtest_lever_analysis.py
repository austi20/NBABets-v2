"""Tests for backtest_lever_analysis.attach_volatility SQL safety."""
from __future__ import annotations

import contextlib
import csv
import sqlite3
import sys
from pathlib import Path

import pytest

# Allow import of scripts module from repo root
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


@pytest.fixture()
def db_with_predictions(tmp_path: Path) -> Path:
    """Create a minimal SQLite DB with predictions table."""
    db_path = tmp_path / "nba_props.sqlite"
    with sqlite3.connect(db_path) as con:
        con.execute(
            "CREATE TABLE predictions (prediction_id INTEGER PRIMARY KEY, player_id INTEGER)"
        )
        con.execute("INSERT INTO predictions VALUES (1, 100)")
        con.execute("INSERT INTO predictions VALUES (2, 200)")
        con.commit()
    return db_path


@pytest.fixture()
def csv_with_injection(tmp_path: Path) -> Path:
    """CSV with a SQL injection string as prediction_id alongside a valid row."""
    path = tmp_path / "grading.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["prediction_id", "line", "proj", "p_over", "actual", "minutes", "hit", "market", "date", "side"])
        writer.writeheader()
        writer.writerow({
            "prediction_id": "1 OR 1=1 --",  # injection attempt
            "line": "25.5", "proj": "24.0", "p_over": "0.45",
            "actual": "26.0", "minutes": "35", "hit": "WIN",
            "market": "points", "date": "2026-05-10", "side": "over",
        })
        writer.writerow({
            "prediction_id": "1",  # legitimate row
            "line": "25.5", "proj": "24.0", "p_over": "0.45",
            "actual": "26.0", "minutes": "35", "hit": "WIN",
            "market": "points", "date": "2026-05-10", "side": "over",
        })
    return path


def test_attach_volatility_filters_non_numeric_ids_to_parameterized_query(
    db_with_predictions: Path, csv_with_injection: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The injection string must be filtered to integers BEFORE the DB IN-clause
    query, and the query must bind those integers as ``?`` placeholders.

    This pins the parameterization fix: capturing the SQL + bound params proves
    that (a) placeholders are used (no string concatenation of IDs into SQL), and
    (b) the non-numeric injection value never reaches the query. Reverting to
    string-concatenation or dropping the int-filter would surface the injection
    string in the captured call and fail the assertions below.
    """
    import scripts.backtest_lever_analysis as mod

    monkeypatch.chdir(csv_with_injection.parent)
    monkeypatch.setattr(mod, "ROWS_PATH", str(csv_with_injection))
    monkeypatch.setattr(mod, "_get_db_path", lambda: str(db_with_predictions))

    rows = mod.load_rows()
    assert len(rows) == 2

    captured: list[tuple[str, object]] = []

    class _SpyCursor(sqlite3.Cursor):
        def execute(self, sql: str, params: object = ()) -> sqlite3.Cursor:  # type: ignore[override]
            captured.append((sql, params))
            return super().execute(sql, params)  # type: ignore[arg-type]

    class _SpyConnection(sqlite3.Connection):
        def cursor(self, *args: object, **kwargs: object) -> sqlite3.Cursor:  # type: ignore[override]
            return super().cursor(_SpyCursor)

    real_connect = sqlite3.connect

    def _spy_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        kwargs["factory"] = _SpyConnection
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(mod.sqlite3, "connect", _spy_connect)

    # Stub the downstream so the test isolates the DB-query SQL safety, not the
    # volatility service. A no-op session scope plus identity feature builders
    # keep attach_volatility runnable to completion.
    monkeypatch.setattr(
        "app.db.session.session_scope",
        lambda: contextlib.nullcontext(object()),
    )
    monkeypatch.setattr("app.services.volatility.build_feature_snapshot", lambda **kw: {})
    monkeypatch.setattr(
        "app.services.volatility.compute_volatility",
        lambda **kw: type("Score", (), {"tier": "A", "coefficient": 1.0})(),
    )

    mod.attach_volatility(rows)

    # Exactly one SELECT against predictions with the IN-clause.
    select_calls = [c for c in captured if "FROM predictions" in c[0]]
    assert len(select_calls) == 1, f"expected one predictions query, got {select_calls}"
    sql, params = select_calls[0]

    # Parameterization: placeholders in the SQL, IDs bound separately.
    assert "?" in sql, "IDs must be bound via ? placeholders, not concatenated into SQL"
    assert "1 OR 1=1" not in sql, "injection string leaked into raw SQL"

    bound = list(params)  # type: ignore[arg-type]
    # Only the integer-coercible ID survives; injection string is filtered out.
    assert bound == [1], f"only the integer id should be bound, got {bound}"


def test_attach_volatility_leaves_predictions_table_intact(
    db_with_predictions: Path, csv_with_injection: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Defense in depth: the read-only query must not mutate the predictions table."""
    import scripts.backtest_lever_analysis as mod

    monkeypatch.chdir(csv_with_injection.parent)
    monkeypatch.setattr(mod, "ROWS_PATH", str(csv_with_injection))
    monkeypatch.setattr(mod, "_get_db_path", lambda: str(db_with_predictions))
    monkeypatch.setattr(
        "app.db.session.session_scope",
        lambda: contextlib.nullcontext(object()),
    )
    monkeypatch.setattr("app.services.volatility.build_feature_snapshot", lambda **kw: {})
    monkeypatch.setattr(
        "app.services.volatility.compute_volatility",
        lambda **kw: type("Score", (), {"tier": "A", "coefficient": 1.0})(),
    )

    rows = mod.load_rows()
    mod.attach_volatility(rows)

    with sqlite3.connect(db_with_predictions) as con:
        count = con.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
    assert count == 2
