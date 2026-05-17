"""Derive per-player over-probability bias offsets from historical grading.

Reads `historical_grading.csv` (output of grade_historical_props.py) and
applies Bayesian shrinkage toward the per-market prior to produce a
runtime-loadable table at `data/player_bias_offsets.json`.

Offset semantics (same as Settings.over_probability_bias_offset):
  positive => tilt model's over_probability DOWN toward UNDER
  negative => tilt model's over_probability UP toward OVER

A player's raw over% is shrunk to:
    shrunk_p_over = (n * raw_p_over + K * market_prior) / (n + K)
    offset        = 0.5 - shrunk_p_over

K=20 is the prior strength (effective sample size). Players with fewer
than MIN_N graded picks are omitted; the runtime falls back to the
per-market offset for them.
"""
from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
INPUT_CSV = _REPO_ROOT / "historical_grading.csv"
OUTPUT_JSON = _REPO_ROOT / "data" / "player_bias_offsets.json"

PRIOR_STRENGTH = 20.0  # K — Bayesian prior weight in "effective picks"
MIN_N = 5  # players below this are excluded; runtime falls back to per-market


def main() -> int:
    if not INPUT_CSV.exists():
        print(f"ERROR: {INPUT_CSV} not found. Run scripts/grade_historical_props.py first.")
        return 1

    rows = list(csv.DictReader(INPUT_CSV.open(encoding="utf-8")))
    for r in rows:
        r["over_hit"] = r["over_hit"] in ("True", "true", "1")

    # Per-market prior (over rate across all players in that market)
    market_over = defaultdict(list)
    for r in rows:
        market_over[r["prop_type"]].append(r["over_hit"])
    market_prior = {
        mkt: sum(hits) / len(hits) if hits else 0.5
        for mkt, hits in market_over.items()
    }

    # Per-player Bayesian shrinkage; one offset per player aggregated across markets,
    # with the prior being the weighted average per-market prior for that player's
    # markets (matches the analysis output).
    by_player: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        by_player[(r["player_id"], r["player_name"])].append(r)

    offsets = {}
    for (pid, name), items in by_player.items():
        if len(items) < MIN_N:
            continue
        raw_over = sum(1 for r in items if r["over_hit"]) / len(items)
        # Per-player prior = average market-prior weighted by sample size per market.
        per_market_count: dict[str, int] = defaultdict(int)
        for r in items:
            per_market_count[r["prop_type"]] += 1
        weighted_prior = sum(
            market_prior.get(mkt, 0.5) * n for mkt, n in per_market_count.items()
        ) / len(items)
        shrunk = (
            len(items) * raw_over + PRIOR_STRENGTH * weighted_prior
        ) / (len(items) + PRIOR_STRENGTH)
        offset = 0.5 - shrunk
        offsets[str(pid)] = {
            "name": name,
            "n": len(items),
            "raw_over_rate": round(raw_over, 4),
            "shrunk_over_rate": round(shrunk, 4),
            "offset": round(offset, 4),
        }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "source": "historical_grading.csv",
        "prior_strength": PRIOR_STRENGTH,
        "min_sample_size": MIN_N,
        "total_rows_graded": len(rows),
        "player_count": len(offsets),
        "offsets": offsets,
    }
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    abs_offsets = sorted(offsets.values(), key=lambda x: -abs(x["offset"]))
    print(f"Wrote {OUTPUT_JSON}")
    print(f"  {len(offsets)} players (>= {MIN_N} picks each)")
    print(f"  source rows: {len(rows)}")
    print()
    print("Top 10 by absolute offset:")
    print(f'  {"player":25} {"n":>4} {"raw%":>7} {"shrunk%":>9} {"offset":>9}')
    for entry in abs_offsets[:10]:
        print(
            f'  {entry["name"]:25} {entry["n"]:>4} '
            f'{100*entry["raw_over_rate"]:>6.1f}% {100*entry["shrunk_over_rate"]:>8.1f}% '
            f'{entry["offset"]:>+8.3f}'
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
