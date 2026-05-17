"""Grade the 9,902 historical prop snapshots against actual box scores.

Two analyses:
1. Market-level: for each (game, player, prop_type) take the *closing* line
   (most recent updated_at) and grade over/under against the actual stat.
   Reports side-bias, per-market hit rates, sportsbook-favorite bias,
   etc. — all model-independent.
2. Implied-probability calibration: for each line, compute the no-vig
   implied probability from the over/under odds. Grade whether the
   implied favorite actually wins at the implied frequency.
"""
from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
CACHE_PATH = _REPO_ROOT / "historical_props.json"

# Maps from BDL prop_type -> box-score field extractor
PROP_TYPE_MAP = {
    "points": lambda s: s.get("pts", 0),
    "rebounds": lambda s: s.get("reb", 0),
    "assists": lambda s: s.get("ast", 0),
    "threes": lambda s: s.get("fg3m", 0),
    "threes_made": lambda s: s.get("fg3m", 0),
    "steals": lambda s: s.get("stl", 0),
    "blocks": lambda s: s.get("blk", 0),
    "turnovers": lambda s: s.get("turnover", 0),
    "points_rebounds_assists": lambda s: s.get("pts", 0) + s.get("reb", 0) + s.get("ast", 0),
    "points_rebounds": lambda s: s.get("pts", 0) + s.get("reb", 0),
    "points_assists": lambda s: s.get("pts", 0) + s.get("ast", 0),
    "rebounds_assists": lambda s: s.get("reb", 0) + s.get("ast", 0),
    "double_double": lambda s: int(sum(1 for v in (s.get("pts",0), s.get("reb",0), s.get("ast",0), s.get("stl",0), s.get("blk",0)) if v >= 10) >= 2),
    "triple_double": lambda s: int(sum(1 for v in (s.get("pts",0), s.get("reb",0), s.get("ast",0), s.get("stl",0), s.get("blk",0)) if v >= 10) >= 3),
}


def american_to_implied(odds: int | None) -> float | None:
    if odds is None:
        return None
    if odds < 0:
        return float(-odds) / (float(-odds) + 100.0)
    return 100.0 / (float(odds) + 100.0)


def hit_rate(items: list[bool]) -> tuple[int, int, float]:
    if not items:
        return (0, 0, 0.0)
    w = sum(items)
    return (w, len(items), 100.0 * w / len(items))


def main():
    cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    print(f"Loaded: {len(cache['games'])} games, "
          f"{sum(len(v) for v in cache['props_by_game'].values())} prop snapshots, "
          f"{sum(len(v) for v in cache['box_scores_by_date'].values())} box rows.")

    # Index actuals: (game_id, player_id) -> stat dict
    actuals = {}
    for _d, logs in cache["box_scores_by_date"].items():
        for log in logs:
            gid = log["game"]["id"]
            pid = log["player"]["id"]
            actuals[(gid, pid)] = log

    # For each (game_id, player_id, prop_type) keep the most recent line
    # (closing line). vendors can be many; pick the one with the latest
    # updated_at.
    closing = {}
    for gid_str, props in cache["props_by_game"].items():
        gid = int(gid_str)
        for p in props:
            pt = p.get("prop_type")
            if pt is None:
                continue
            key = (gid, p["player_id"], pt)
            ts = p.get("updated_at") or ""
            if key not in closing or ts > closing[key]["updated_at"]:
                closing[key] = p

    print(f"Closing-line records: {len(closing)}")
    print("Prop-type distribution:")
    pt_counts = defaultdict(int)
    for (_, _, pt), _ in closing.items():
        pt_counts[pt] += 1
    for pt, n in sorted(pt_counts.items(), key=lambda x: -x[1]):
        print(f"  {pt:30}: {n}")
    print()

    # Grade closing lines
    rows = []
    skipped = 0
    for (gid, pid, pt), prop in closing.items():
        extractor = PROP_TYPE_MAP.get(pt)
        if extractor is None:
            skipped += 1
            continue
        actual_log = actuals.get((gid, pid))
        if actual_log is None:
            skipped += 1
            continue
        try:
            line = float(prop["line_value"])
        except (TypeError, ValueError):
            skipped += 1
            continue
        market = prop.get("market") or {}
        over_odds = market.get("over_odds")
        under_odds = market.get("under_odds")
        actual = extractor(actual_log)
        if actual == line:
            # push
            continue
        over_hit = actual > line
        rows.append(
            dict(
                game_id=gid,
                player_id=pid,
                player_name=f"{actual_log['player']['first_name']} {actual_log['player']['last_name']}",
                team=actual_log["team"]["abbreviation"],
                prop_type=pt,
                line=line,
                over_odds=over_odds,
                under_odds=under_odds,
                actual=actual,
                over_hit=over_hit,
                minutes=actual_log.get("min", "0"),
                date=actual_log["game"]["date"][:10],
                vendor=prop.get("vendor"),
            )
        )

    print(f"Graded {len(rows)} closing lines; skipped {skipped} (unknown prop type or no box).")
    print()

    # Overall side bias
    over_wins = [r["over_hit"] for r in rows]
    under_wins = [not r["over_hit"] for r in rows]
    print("=== Overall side bias (closing lines) ===")
    w, n, pct = hit_rate(over_wins)
    print(f"  always-OVER : {w}/{n} = {pct:.2f}%")
    w, n, pct = hit_rate(under_wins)
    print(f"  always-UNDER: {w}/{n} = {pct:.2f}%")
    print()

    # Per market
    print("=== Per-market side bias ===")
    by_pt = defaultdict(list)
    for r in rows:
        by_pt[r["prop_type"]].append(r)
    print(f"{'market':32} {'n':>6} {'OVER%':>7} {'UNDER%':>7}")
    for pt, items in sorted(by_pt.items(), key=lambda x: -len(x[1])):
        ow = [r["over_hit"] for r in items]
        uw = [not r["over_hit"] for r in items]
        _, _, ovr = hit_rate(ow)
        _, _, und = hit_rate(uw)
        print(f"{pt:32} {len(items):>6} {ovr:>6.2f}% {und:>6.2f}%")
    print()

    # Per vendor
    print("=== Per-vendor side bias ===")
    by_v = defaultdict(list)
    for r in rows:
        by_v[r["vendor"]].append(r)
    print(f"{'vendor':20} {'n':>6} {'OVER%':>7} {'UNDER%':>7}")
    for v, items in sorted(by_v.items(), key=lambda x: -len(x[1])):
        ow = [r["over_hit"] for r in items]
        _, _, ovr = hit_rate(ow)
        _, _, und = hit_rate([not h for h in ow])
        print(f"{str(v):20} {len(items):>6} {ovr:>6.2f}% {und:>6.2f}%")
    print()

    # Calibration: implied no-vig probability vs actual hit rate
    print("=== Sportsbook implied probability calibration ===")
    print("For each prop, compute the OVER's no-vig implied probability and bucket.")
    cal = defaultdict(list)
    for r in rows:
        po = american_to_implied(r["over_odds"])
        pu = american_to_implied(r["under_odds"])
        if po is None or pu is None:
            continue
        # No-vig: po_nv = po / (po + pu)
        if po + pu == 0:
            continue
        po_nv = po / (po + pu)
        bucket = int(po_nv * 10) / 10
        cal[bucket].append(r["over_hit"])
    print(f"{'implied OVER':>14} {'actual %':>10} {'n':>6}  expected vs actual gap")
    for b in sorted(cal):
        ws = cal[b]
        _, n, pct = hit_rate(ws)
        expected = (b + 0.05) * 100
        gap = pct - expected
        print(f"  {b:.1f}-{b+0.1:.1f}{'':>5} {pct:>9.2f}% {n:>6} {gap:>+7.2f}pt")
    print()

    # Implied-favorite bet: always bet the side the market favors (lower vig).
    # Tests whether the market correctly identifies winners.
    fav_wins = []
    fav_under_wins = []
    fav_over_wins = []
    for r in rows:
        po = american_to_implied(r["over_odds"])
        pu = american_to_implied(r["under_odds"])
        if po is None or pu is None:
            continue
        if po > pu:
            won = r["over_hit"]
            fav_over_wins.append(won)
        else:
            won = not r["over_hit"]
            fav_under_wins.append(won)
        fav_wins.append(won)
    print("=== Implied-favorite bet (always pick the side the market favors) ===")
    w, n, pct = hit_rate(fav_wins)
    print(f"  all favorites: {w}/{n} = {pct:.2f}%")
    w, n, pct = hit_rate(fav_over_wins)
    print(f"  favorite=OVER: {w}/{n} = {pct:.2f}%")
    w, n, pct = hit_rate(fav_under_wins)
    print(f"  favorite=UNDER: {w}/{n} = {pct:.2f}%")
    print()

    # Underdog bet: always take the implied underdog
    underdog_wins = []
    for r in rows:
        po = american_to_implied(r["over_odds"])
        pu = american_to_implied(r["under_odds"])
        if po is None or pu is None:
            continue
        if po > pu:
            underdog_wins.append(not r["over_hit"])
        else:
            underdog_wins.append(r["over_hit"])
    w, n, pct = hit_rate(underdog_wins)
    print(f"  always-UNDERDOG (against market): {w}/{n} = {pct:.2f}%")
    print()

    # By line bucket per market
    print("=== Hit rate by line bucket per market ===")
    for pt in sorted(by_pt, key=lambda x: -len(by_pt[x]))[:6]:
        items = by_pt[pt]
        print(f"  -- {pt} (n={len(items)}) --")
        buckets = defaultdict(list)
        for r in items:
            line = r["line"]
            if line < 1:
                b = "0-1"
            elif line < 5:
                b = "1-5"
            elif line < 10:
                b = "5-10"
            elif line < 15:
                b = "10-15"
            elif line < 20:
                b = "15-20"
            elif line < 25:
                b = "20-25"
            elif line < 30:
                b = "25-30"
            else:
                b = "30+"
            buckets[b].append(r["over_hit"])
        for b in ["0-1","1-5","5-10","10-15","15-20","20-25","25-30","30+"]:
            if b in buckets:
                ws = buckets[b]
                _, n, ov = hit_rate(ws)
                _, _, un = hit_rate([not w for w in ws])
                print(f"      line {b:>6}: n={n:>4} over={ov:.1f}% under={un:.1f}%")

    # Save CSV for further analysis
    out_csv = _REPO_ROOT / "historical_grading.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print()
    print(f"Wrote {out_csv} ({len(rows)} rows).")


if __name__ == "__main__":
    main()
