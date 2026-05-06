"""Load the graded prop CSV into the examiner's labeled-example containers.

The CSV referenced here lives outside the repo (configurable via
``EXAMINER_CSV_PATH``). It's the ground truth the examiner agents should learn
from. Only ~10 days are ``source='real'`` — everything else is synthetic, so
``real_only=True`` is the default and the dataset always surfaces the mix
ratio downstream.
"""

from __future__ import annotations

import csv
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path

from .contracts import LabeledPropDataset, LabeledPropExample

# Map raw CSV market strings to the canonical vocabulary used in
# ``app.training.pipeline``. Keep this in sync with the training pipeline if
# the training vocabulary grows.
_MARKET_MAP: dict[str, str] = {
    "player points": "points",
    "player rebounds": "rebounds",
    "player assists": "assists",
    "player threes": "threes",
    "player turnovers": "turnovers",
    "player pra": "pra",
    # Already-canonical fall-throughs (in case upstream ever normalizes).
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "threes": "threes",
    "turnovers": "turnovers",
    "pra": "pra",
}

_EXPECTED_COLUMNS: frozenset[str] = frozenset(
    {
        "game_date",
        "player_name",
        "player_team",
        "opponent",
        "market",
        "sportsbook",
        "line_value",
        "over_odds",
        "under_odds",
        "actual",
        "hit_over",
        "hit_under",
        "push",
        "minutes",
        "source",
    }
)


def _normalize_market(raw: str) -> str:
    """Canonicalize a raw CSV market token. Unknown tokens pass through lowercased."""

    key = (raw or "").strip().lower()
    return _MARKET_MAP.get(key, key)


def _parse_float(value: str) -> float | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return float(stripped)
    except ValueError:
        return None


def _parse_bool(value: str) -> bool | None:
    if value is None:
        return None
    stripped = value.strip().lower()
    if stripped in {"true", "1", "yes", "y", "t"}:
        return True
    if stripped in {"false", "0", "no", "n", "f"}:
        return False
    return None


def _parse_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_source(value: str) -> str:
    stripped = (value or "").strip().lower()
    if stripped == "real":
        return "real"
    # Anything non-real — including blanks, typos, and junk — is treated as
    # synthetic so the real-only filter defaults to the safe side.
    return "synthetic"


def _row_to_example(row: dict[str, str]) -> LabeledPropExample | None:
    game_date = _parse_date(row.get("game_date", ""))
    if game_date is None:
        return None
    player = (row.get("player_name") or "").strip()
    if not player:
        return None
    market = _normalize_market(row.get("market", ""))
    line_value = _parse_float(row.get("line_value", ""))
    if line_value is None:
        return None
    source = _parse_source(row.get("source", ""))
    push_raw = _parse_bool(row.get("push", ""))
    return LabeledPropExample(
        game_date=game_date,
        player_name=player,
        team=(row.get("player_team") or "").strip(),
        opponent=(row.get("opponent") or "").strip(),
        market=market,
        sportsbook=(row.get("sportsbook") or "").strip(),
        line_value=line_value,
        over_odds=_parse_float(row.get("over_odds", "")),
        under_odds=_parse_float(row.get("under_odds", "")),
        actual=_parse_float(row.get("actual", "")),
        hit_over=_parse_bool(row.get("hit_over", "")),
        hit_under=_parse_bool(row.get("hit_under", "")),
        push=bool(push_raw) if push_raw is not None else False,
        minutes=_parse_float(row.get("minutes", "")),
        source=source,  # type: ignore[arg-type]  # Literal enforced at call site
    )


def load_examiner_dataset(
    path: str | Path,
    *,
    real_only: bool = True,
) -> LabeledPropDataset:
    """Read the graded prop CSV and return a ``LabeledPropDataset``.

    ``real_only`` defaults to ``True`` because the CSV is mostly synthetic and
    the examiner should learn from ground truth by default. Even when the
    filter is applied, ``mix_ratio_real_vs_synthetic`` reflects the *underlying
    file*, not the filtered slice — so the prompt can still warn the model.
    """

    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Examiner CSV not found at {csv_path}")

    total_real = 0
    total_synthetic = 0
    kept: list[LabeledPropExample] = []
    earliest: date | None = None
    latest: date | None = None

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        header = set(reader.fieldnames or [])
        missing = _EXPECTED_COLUMNS - header
        if missing:
            raise ValueError(
                f"Examiner CSV at {csv_path} is missing required columns: "
                f"{sorted(missing)}"
            )
        for row in reader:
            example = _row_to_example(row)
            if example is None:
                continue
            if example.source == "real":
                total_real += 1
            else:
                total_synthetic += 1
            if real_only and example.source != "real":
                continue
            kept.append(example)
            if earliest is None or example.game_date < earliest:
                earliest = example.game_date
            if latest is None or example.game_date > latest:
                latest = example.game_date

    return LabeledPropDataset(
        examples=tuple(kept),
        real_count=total_real,
        synthetic_count=total_synthetic,
        earliest_date=earliest,
        latest_date=latest,
        source_path=str(csv_path),
    )


def recent_real_examples(
    dataset: LabeledPropDataset,
    *,
    n_days: int,
    reference: date | None = None,
) -> tuple[LabeledPropExample, ...]:
    """Slice the most-recent ``n_days`` of real examples from ``dataset``.

    ``reference`` defaults to the dataset's latest date so ``recent_real_examples``
    is deterministic on a snapshot of the CSV. Anchoring on "today" would make
    behaviour change as wall-clock time moves, which is poison for tests.
    """

    if n_days <= 0 or not dataset.examples:
        return ()
    anchor = reference or dataset.latest_date
    if anchor is None:
        return ()
    cutoff = anchor - timedelta(days=n_days - 1)
    return tuple(
        example
        for example in dataset.examples
        if example.source == "real" and cutoff <= example.game_date <= anchor
    )


def filter_by_markets(
    dataset: LabeledPropDataset,
    markets: tuple[str, ...],
) -> LabeledPropDataset:
    """Return a new dataset keeping only rows whose canonical market matches."""

    if not markets:
        return dataset
    allow = frozenset(m.lower() for m in markets)
    filtered = tuple(ex for ex in dataset.examples if ex.market in allow)
    return replace(dataset, examples=filtered)
