"""Diagnostic run: reports what the model is doing at each pipeline stage.

Usage:
    python scripts/diagnostic_run.py
    python scripts/diagnostic_run.py --skip-predict --skip-brain
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _project_root() -> Path:
    """Return real project root, handling git worktree layouts.

    Git worktrees have .git as a FILE (containing 'gitdir: ...'), while the
    main repo has .git as a DIRECTORY.  When we're inside a worktree that
    lives at <project>/.claude/worktrees/<branch>/, go three levels up.
    """
    git_marker = ROOT / ".git"
    if git_marker.is_file():
        # Worktree: branch-name -> worktrees -> .claude -> project
        candidate = ROOT.parent.parent.parent
        if (candidate / "pyproject.toml").exists():
            return candidate
    return ROOT


def _section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="NBA prop engine diagnostic run")
    p.add_argument("--skip-predict", action="store_true", help="Skip Stage 3 predict_upcoming (slow)")
    p.add_argument("--skip-brain", action="store_true", help="Skip Stages 4-5 decision brain trace")
    return p.parse_args()


def stage1_training_data(settings) -> int:
    """Returns qualifying row count."""
    _section("STAGE 1: Training Data Health")
    from app.db.session import session_scope
    from app.training.data import DatasetLoader

    with session_scope() as session:
        loader = DatasetLoader(session)
        try:
            frame = loader.load_historical_player_games()
            if frame.empty:
                print("  [!] No historical data in DB")
                return 0
            print(f"  Total rows (minutes>=5):  {len(frame):,}")
            print(f"  Date range:               {frame['game_date'].min().date()} to {frame['game_date'].max().date()}")
            by_month = frame.groupby(frame["game_date"].dt.to_period("M")).size()
            print("  Rows per month:")
            for period, count in by_month.items():
                print(f"    {period}: {count:,}")
            est_folds = max(8, len(frame) // 120)
            print(f"  Estimated calibration folds: {est_folds}  (formula: max(8, {len(frame)}//120))")

            parquet_root = settings.historical_parquet_root
            if parquet_root and parquet_root.exists():
                pf = loader.load_historical_player_games_from_parquet(parquet_root)
                print(f"  Parquet rows:             {len(pf):,}")
                combined = len(frame) + len(pf)
                print(f"  Combined estimated folds: {max(8, combined // 120)}")
            else:
                print("  HISTORICAL_PARQUET_ROOT:  not set (set env var to add prior-season data)")

            return len(frame)
        except Exception as exc:
            print(f"  [ERROR] {exc}")
            return 0


def stage2_artifacts(settings) -> None:
    _section("STAGE 2: Model Artifact Status")
    from app.training.artifacts import artifact_paths, artifact_exists, resolve_artifact_namespace
    from app.db.session import session_scope
    import sqlite3

    import datetime as dt
    ns = resolve_artifact_namespace(settings.database_url, settings.app_env)
    # All markets share a single artifact directory keyed by model version "v1"
    paths = artifact_paths("v1", namespace=ns)
    key_files = {
        "minutes_model":   paths.minutes_model,
        "stat_models":     paths.stat_models,
        "calibrators":     paths.calibrators,
        "metadata":        paths.metadata,
        "pop_priors":      paths.population_priors,
        "consistency":     paths.consistency_scores,
    }
    for name, path in key_files.items():
        exists = artifact_exists(path)
        mtime = dt.datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M") if exists else None
        size_kb = (path.stat().st_size // 1024) if exists else 0
        status = "OK" if exists else "MISSING"
        print(f"  {name:<18} [{status}]  {mtime or 'n/a'}  ({size_kb} KB)")
    print(f"\n  Artifact root: {paths.root}")

    # Latest model run from DB
    try:
        db_path = settings.database_url[len("sqlite:///"):]
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute("SELECT model_run_id, training_window_start, training_window_end, started_at FROM model_runs ORDER BY started_at DESC LIMIT 1")
        row = cur.fetchone()
        con.close()
        if row:
            print(f"\n  Last model run: id={row[0]} window={row[1]} to {row[2]} at={row[3]}")
        else:
            print("\n  No model runs in DB")
    except Exception as exc:
        print(f"\n  [ERROR reading model_runs] {exc}")


def stage3_predict(settings) -> None:
    _section("STAGE 3: predict_upcoming (today's props)")
    from app.db.session import session_scope
    from app.training.pipeline import TrainingPipeline

    with session_scope() as session:
        pipeline = TrainingPipeline(session)
        try:
            result = pipeline.predict_upcoming(target_date=date.today())
            opps = result if isinstance(result, list) else result.get("opportunities", [])
            print(f"  Total props generated: {len(opps)}")
            from collections import Counter
            by_market = Counter(getattr(o, "market_key", "unknown") for o in opps)
            print("  By market_key:")
            for mk, cnt in sorted(by_market.items(), key=lambda x: -x[1]):
                print(f"    {mk:<15} {cnt}")

            top10 = sorted(opps, key=lambda o: getattr(o, "hit_probability", 0), reverse=True)[:10]
            print("\n  Top 10 by hit_probability:")
            print(f"  {'Player':<25} {'Market':<12} {'Line':>6} {'HitProb':>8} {'ProjMean':>9}")
            print("  " + "-" * 65)
            for o in top10:
                print(f"  {str(getattr(o,'player_name','?')):<25} {str(getattr(o,'market_key','?')):<12} "
                      f"{getattr(o,'line_value',0):>6.1f} {getattr(o,'hit_probability',0):>8.3f} {getattr(o,'projected_mean',0):>9.2f}")
        except Exception as exc:
            print(f"  [ERROR] {exc}")
            import traceback
            traceback.print_exc()


def stage4_brain_candidates(settings) -> list:
    _section("STAGE 4: Decision Brain Candidate Trace")
    from app.trading.decision_brain import (
        load_policy, candidates_from_board, load_manual_candidates, merge_candidates,
        _candidate_policy_blockers, _stale_context_blockers,
    )
    from pathlib import Path
    import json
    from collections import Counter, defaultdict

    try:
        policy = load_policy(settings)
        print(f"  Policy: {policy.policy_version}  allow_live_submit={policy.allow_live_submit}")
        print(f"  Allowed market keys: {sorted(policy.allowed_market_keys)}")
        print(f"  Blocked market keys: {sorted(policy.blocked_market_keys)}")
        print(f"  Min model_prob={policy.min_model_prob}  min_confidence={policy.min_confidence}  min_edge_bps={policy.min_edge_bps}")
    except Exception as exc:
        print(f"  [ERROR loading policy] {exc}")
        return []

    # Stale context check
    stale = _stale_context_blockers(settings, policy)
    if stale:
        print(f"\n  [WARN] Stale context detected: {stale}")
    else:
        print("\n  Context freshness: OK")

    # Read candidates from kalshi_resolution_targets.json (written by brain sync)
    # board_entry is a live object from predict_upcoming - for diagnostics we read
    # the last-written targets file instead.
    # Settings returns a path relative to the worktree; use the real project root.
    targets_path = Path(settings.kalshi_resolution_targets_path)
    if not targets_path.exists():
        # Fall back to main project config (artifact not checked into git)
        targets_path = _project_root() / "config" / "kalshi_resolution_targets.json"
        if not targets_path.exists():
            targets_path = ROOT / "config" / "kalshi_resolution_targets.json"
    if not targets_path.exists():
        print(f"\n  [!] No resolution targets file at {targets_path}")
        print("      Run `python scripts/build_live_decision_pack.py` first.")
        return []

    try:
        tgt_payload = json.loads(targets_path.read_text(encoding="utf-8"))
        targets = tgt_payload.get("targets", []) if isinstance(tgt_payload, dict) else []
        generated_at = tgt_payload.get("generated_at", "unknown") if isinstance(tgt_payload, dict) else "unknown"
        print(f"\n  Targets file: {targets_path}")
        print(f"  Generated:    {generated_at}")
        print(f"  Total targets exported: {len(targets)}")
    except Exception as exc:
        print(f"  [ERROR reading targets] {exc}")
        return []

    # Show market distribution from targets
    all_candidates = targets  # use raw dicts for reporting

    # Market distribution from targets
    market_dist: Counter = Counter()
    for t in all_candidates:
        mk = str(t.get("market_key", "?")).split(".")[-1] if isinstance(t, dict) else "?"
        market_dist[mk] += 1
    print("\n  Market key distribution:")
    for mk, cnt in sorted(market_dist.items(), key=lambda x: -x[1]):
        print(f"    {mk:<22} {cnt}")

    # Recommendation breakdown
    rec_dist: Counter = Counter(
        str(t.get("recommendation", "?")) for t in all_candidates if isinstance(t, dict)
    )
    print("\n  Recommendation distribution:")
    for rec, cnt in sorted(rec_dist.items(), key=lambda x: -x[1]):
        print(f"    {rec:<22} {cnt}")

    return all_candidates


def stage5_kalshi_attrition(settings, exportable_candidates: list) -> None:
    _section("STAGE 5: Kalshi Symbol Resolution Attrition")
    import json
    from collections import Counter
    from pathlib import Path

    symbols_path = Path(settings.kalshi_symbols_path)
    if not symbols_path.exists():
        # Fall back to main project config (artifact not checked into git)
        symbols_path = _project_root() / "config" / "kalshi_symbols.json"
        if not symbols_path.exists():
            symbols_path = ROOT / "config" / "kalshi_symbols.json"
    if not symbols_path.exists():
        print(f"  [!] Symbol map not found at {symbols_path}")
        print("      Run: python scripts/resolve_kalshi_targets.py")
        return

    payload = json.loads(symbols_path.read_text(encoding="utf-8"))
    symbols = payload.get("symbols", []) if isinstance(payload, dict) else []
    unresolved = payload.get("unresolved", []) if isinstance(payload, dict) else []

    generated_at = payload.get("generated_at", "unknown") if isinstance(payload, dict) else "unknown"
    print(f"  Symbol map generated: {generated_at}")
    print(f"  Resolved symbols:     {len(symbols)}")
    print(f"  Unresolved targets:   {len(unresolved)}")

    # Brain blocker breakdown on resolved symbols
    brain_blocker_dist: Counter = Counter()
    market_dist_sym: Counter = Counter()
    eligible_count = 0
    for sym in symbols:
        if not isinstance(sym, dict):
            continue
        mk = str(sym.get("market_key", "?")).split(".")[-1]
        market_dist_sym[mk] += 1
        bb = sym.get("brain_blockers", [])
        if bb:
            for b in bb:
                brain_blocker_dist[b] += 1
        else:
            eligible_count += 1

    print(f"\n  Resolved by market_key:")
    for mk, cnt in sorted(market_dist_sym.items(), key=lambda x: -x[1]):
        print(f"    {mk:<22} {cnt}")

    print(f"\n  Brain gate results on resolved symbols:")
    print(f"    Eligible (no blockers): {eligible_count}")
    print(f"    Blocked:                {len(symbols) - eligible_count}")
    if brain_blocker_dist:
        print("  Brain blockers:")
        for blocker, cnt in sorted(brain_blocker_dist.items(), key=lambda x: -x[1]):
            note = "  <-- games already closed" if blocker == "stale_event" else ""
            print(f"    {blocker:<35} {cnt}{note}")

    # All resolved symbols detail
    print(f"\n  All resolved symbols:")
    print(f"  {'Player':<26} {'Market':<12} {'Ticker':<30} {'Blockers'}")
    print("  " + "-" * 90)
    for sym in symbols:
        if not isinstance(sym, dict):
            continue
        player = str(sym.get("player_name", "?"))[:25]
        mk = str(sym.get("market_key", "?")).split(".")[-1][:11]
        ticker = str(sym.get("kalshi_ticker", sym.get("ticker", "?")))[:29]
        bb = sym.get("brain_blockers", [])
        print(f"  {player:<26} {mk:<12} {ticker:<30} {bb}")

    # Unresolved reasons
    if unresolved:
        unres_reasons: Counter = Counter(
            str(u.get("reason", "unknown")) for u in unresolved if isinstance(u, dict)
        )
        print(f"\n  Unresolved reasons ({len(unresolved)} total):")
        for reason, cnt in sorted(unres_reasons.items(), key=lambda x: -x[1]):
            print(f"    {reason:<38} {cnt}")
        print(f"\n  Unresolved sample (first 8):")
        print(f"  {'Player':<26} {'Market':<14} {'Line':>5}  {'Reason'}")
        print("  " + "-" * 70)
        for u in unresolved[:8]:
            if not isinstance(u, dict):
                continue
            player = str(u.get("player_name") or f"id={u.get('player_id', '?')}")[:25]
            mk = str(u.get("market_key", "?")).split(".")[-1][:13]
            line = u.get("line_value", "?")
            reason = u.get("reason", "?")
            print(f"  {player:<26} {mk:<14} {str(line):>5}  {reason}")


def main() -> None:
    args = _parse_args()
    from app.config.settings import get_settings
    settings = get_settings()

    print(f"NBA Prop Engine Diagnostic")
    print(f"DB: {settings.database_url}")
    print(f"Date: {date.today()}")

    sqlite_rows = stage1_training_data(settings)
    stage2_artifacts(settings)

    if not args.skip_predict:
        stage3_predict(settings)
    else:
        print("\n[STAGE 3 SKIPPED - use without --skip-predict to run predictions]")

    exportable = []
    if not args.skip_brain:
        exportable = stage4_brain_candidates(settings)
        stage5_kalshi_attrition(settings, exportable)
    else:
        print("\n[STAGES 4-5 SKIPPED - use without --skip-brain to trace decision brain]")

    _section("SUMMARY")
    print(f"  SQLite training rows:    {sqlite_rows:,}")
    print(f"  Estimated folds:         {max(8, sqlite_rows // 120)}")
    parquet_root = settings.historical_parquet_root
    if parquet_root and parquet_root.exists():
        print(f"  HISTORICAL_PARQUET_ROOT: {parquet_root}")
    else:
        print(f"  HISTORICAL_PARQUET_ROOT: not set")
    print(f"  Exportable candidates:   {len(exportable)}")
    print()


if __name__ == "__main__":
    main()
