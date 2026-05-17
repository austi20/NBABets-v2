"""Bootstrap the 6-day grading dataset to size each proposed fix.

For each proposed lever, compute the hit rate uplift it produces on the
observed sample, then bootstrap-resample with replacement to size a
30-day-equivalent confidence interval. Outputs a ranked table the user
can act on.
"""
from __future__ import annotations

import csv
import random
import sqlite3
import statistics
import sys
from datetime import date
from pathlib import Path

# Allow running from any cwd: prepend the repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

random.seed(42)

ROWS_PATH = "grading_2026-05-10_to_15.csv"


def load_rows() -> list[dict]:
    rows = list(csv.DictReader(open(ROWS_PATH, encoding="utf-8")))
    for r in rows:
        r["line"] = float(r["line"])
        r["proj"] = float(r["proj"])
        r["p_over"] = float(r["p_over"])
        r["actual"] = float(r["actual"])
        r["minutes"] = float(r["minutes"] or 0)
        r["won"] = r["hit"] == "WIN"
    return rows


def attach_volatility(rows: list[dict]) -> None:
    con = sqlite3.connect("C:/Users/gunna/AppData/Local/NBAPropEngine/data/processed/nba_props.sqlite")
    cur = con.cursor()
    pred_ids = ",".join(r["prediction_id"] for r in rows)
    cur.execute(f"SELECT prediction_id, player_id FROM predictions WHERE prediction_id IN ({pred_ids})")
    pred_to_pid = {pid: ppid for pid, ppid in cur.fetchall()}
    con.close()

    from app.db.session import session_scope
    from app.services.volatility import build_feature_snapshot, compute_volatility

    tier_cache: dict = {}
    with session_scope() as session:
        for r in rows:
            ppid = pred_to_pid.get(int(r["prediction_id"]))
            if ppid is None:
                continue
            key = (ppid, r["market"], r["date"])
            if key not in tier_cache:
                snap = build_feature_snapshot(
                    session=session,
                    player_id=ppid,
                    market_key=r["market"],
                    as_of_date=date.fromisoformat(r["date"]),
                    predicted_minutes_std=None,
                )
                score = compute_volatility(raw_probability=r["p_over"], features=snap)
                tier_cache[key] = (score.tier, score.coefficient)
            r["vol_tier"] = tier_cache[key][0]
            r["vol_coef"] = tier_cache[key][1]


def simulate(rows, modify_fn):
    kept = []
    for r in rows:
        keep, won = modify_fn(r)
        if keep:
            kept.append(won)
    return (sum(kept) / len(kept) if kept else 0.0, len(kept))


def baseline(r):
    return True, r["won"]


def flip_grade(r, new_side: str) -> tuple[bool, bool]:
    if r["actual"] == r["line"]:
        return False, False
    won = (new_side == "over" and r["actual"] > r["line"]) or (
        new_side == "under" and r["actual"] < r["line"]
    )
    return True, won


def make_side_bias(offset):
    def fix(r):
        p = r["p_over"] - offset
        side = "over" if p > 0.5 else "under"
        if side == r["side"]:
            return True, r["won"]
        return flip_grade(r, side)
    return fix


def make_shrink(p_max):
    def fix(r):
        p = r["p_over"]
        p = min(p, p_max)
        p = max(p, 1 - p_max)
        side = "over" if p > 0.5 else "under"
        if side == r["side"]:
            return True, r["won"]
        return flip_grade(r, side)
    return fix


def threes_ban(r):
    if r["market"] == "threes":
        return False, False
    return True, r["won"]


def make_drop_above(p_max):
    def fix(r):
        if abs(r["p_over"] - 0.5) > p_max - 0.5:
            return False, False
        return True, r["won"]
    return fix


def stacked_v1(r):
    if r["market"] == "threes":
        return False, False
    p = r["p_over"] - 0.05
    p = min(p, 0.75)
    p = max(p, 0.25)
    side = "over" if p > 0.5 else "under"
    if side == r["side"]:
        return True, r["won"]
    return flip_grade(r, side)


def stacked_v2(r):
    if r["market"] == "threes":
        return False, False
    p = r["p_over"] - 0.05
    if abs(p - 0.5) > 0.30:
        return False, False
    side = "over" if p > 0.5 else "under"
    if side == r["side"]:
        return True, r["won"]
    return flip_grade(r, side)


def stacked_v3(r):
    if r["market"] == "threes":
        return False, False
    p = r["p_over"]
    if p > 0.5:
        return False, False
    if abs(p - 0.5) > 0.30:
        return False, False
    if r["actual"] == r["line"]:
        return False, False
    return True, r["actual"] < r["line"]


def stacked_v4(r):
    if r["market"] not in ("assists", "rebounds"):
        return False, False
    p = r["p_over"] - 0.07
    p = min(p, 0.80)
    p = max(p, 0.20)
    side = "over" if p > 0.5 else "under"
    if side == r["side"]:
        return True, r["won"]
    return flip_grade(r, side)


LEVERS = [
    ("baseline", baseline),
    ("side_bias -0.03", make_side_bias(0.03)),
    ("side_bias -0.05", make_side_bias(0.05)),
    ("side_bias -0.07", make_side_bias(0.07)),
    ("side_bias -0.10", make_side_bias(0.10)),
    ("shrink p_max=0.80", make_shrink(0.80)),
    ("shrink p_max=0.75", make_shrink(0.75)),
    ("shrink p_max=0.70", make_shrink(0.70)),
    ("threes_ban (all)", threes_ban),
    ("drop |p-.5|>0.30 (cap p at 0.80)", make_drop_above(0.80)),
    ("drop |p-.5|>0.25 (cap p at 0.75)", make_drop_above(0.75)),
    ("stacked v1: bias-.05 + cap.75 + no_threes", stacked_v1),
    ("stacked v2: bias-.05 + drop>.80 + no_threes", stacked_v2),
    ("stacked v3: under-only + drop>.80 + no_threes", stacked_v3),
    ("stacked v4: bias-.07 + clip.80 + assists/rebounds only", stacked_v4),
]


def main():
    rows = load_rows()
    print(f"Loaded {len(rows)} graded picks.")
    attach_volatility(rows)
    print("Volatility attached.")
    print()

    base_rate, _ = simulate(rows, baseline)
    print(f'{"lever":58} {"hit%":>7} {"kept":>6} {"vs base":>9}')
    print("-" * 84)
    for name, fn in LEVERS:
        rate, kept = simulate(rows, fn)
        delta = (rate - base_rate) * 100
        print(f"{name:58} {rate*100:>6.2f}% {kept:>6} {delta:>+7.2f}pt")

    print()
    print("=== Bootstrap CI: 2000 resamples, N=16090 (~30 day equivalent) ===")
    print(f'{"lever":58} {"mean":>7} {"5%":>7} {"95%":>7}')
    print("-" * 84)
    N = len(rows) * 5
    TRIALS = 2000
    for name, fn in LEVERS:
        rates = []
        for _ in range(TRIALS):
            sample = random.choices(rows, k=N)
            rate, _ = simulate(sample, fn)
            rates.append(rate * 100)
        rates.sort()
        p5 = rates[int(0.05 * TRIALS)]
        p95 = rates[int(0.95 * TRIALS)]
        print(f"{name:58} {statistics.mean(rates):>6.2f}% {p5:>6.2f}% {p95:>6.2f}%")


if __name__ == "__main__":
    main()
