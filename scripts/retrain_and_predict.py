"""Retrain the live model and regenerate predictions for today."""
from __future__ import annotations

import argparse
import os
import sys
import threading
from contextlib import contextmanager
from datetime import date
from typing import cast

import pandas as pd

from app.config.settings import get_settings
from app.db.session import session_scope
from app.training.data import DatasetLoader
from app.training.pipeline import TrainingPipeline
from app.training.upcoming import load_upcoming_scoped


def _safe_print(obj: object) -> None:
    try:
        print(obj)
    except UnicodeEncodeError:
        import json
        sys.stdout.buffer.write((json.dumps(str(obj), ensure_ascii=True) + "\n").encode("ascii", errors="replace"))


@contextmanager
def _heartbeat(seconds: int, phase_ref: dict[str, str]):
    if seconds <= 0:
        yield
        return
    stop_event = threading.Event()

    def run() -> None:
        tick = 0
        while not stop_event.wait(seconds):
            tick += 1
            _safe_print(f"  [heartbeat] {phase_ref['label']} still running ({tick * seconds}s elapsed)")

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop_event.set()
        worker.join(timeout=seconds)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retrain model and regenerate upcoming predictions")
    k_seasons_env = os.getenv("TRAINING_K_SEASONS", "").strip()
    l1_alpha_env = os.getenv("TRAINING_L1_ALPHA", "").strip()
    dist_family_env = os.getenv("TRAINING_DIST_FAMILY", "").strip()
    parser.add_argument(
        "--heartbeat-seconds",
        type=int,
        default=int(os.getenv("RETRAIN_HEARTBEAT_SEC", "0") or 0),
        help="Emit heartbeat output every N seconds (0 disables).",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        default=(os.getenv("RETRAIN_SMOKE", "").strip().lower() in {"1", "true", "yes"}),
        help="Run a bounded non-production smoke cycle.",
    )
    parser.add_argument(
        "--max-historical-rows",
        type=int,
        default=int(os.getenv("RETRAIN_SMOKE_MAX_HISTORICAL_ROWS", "12000") or 12000),
        help="Historical row cap for --smoke mode.",
    )
    parser.add_argument(
        "--max-game-count",
        type=int,
        default=int(os.getenv("RETRAIN_SMOKE_MAX_GAMES", "2") or 2),
        help="Game cap for --smoke mode.",
    )
    parser.add_argument(
        "--k-seasons",
        type=int,
        default=int(k_seasons_env) if k_seasons_env else None,
        help="Rolling-feature lookback scale for ablation runs.",
    )
    parser.add_argument(
        "--l1-alpha",
        type=float,
        default=float(l1_alpha_env) if l1_alpha_env else None,
        help="L1 regularization alpha for XGBoost regressor runs.",
    )
    parser.add_argument(
        "--dist-family",
        choices=["legacy", "count_aware", "decomposed"],
        default=dist_family_env or None,
        help="Override distribution family used during probability simulation.",
    )
    return parser.parse_args()


def _bounded_smoke_inputs(
    *,
    target_date: date,
    max_historical_rows: int,
    max_game_count: int,
) -> tuple[pd.DataFrame, set[int] | None]:
    with session_scope() as session:
        loader = DatasetLoader(session)
        historical = loader.load_historical_player_games(as_of_date=target_date)
        if max_historical_rows > 0 and len(historical) > max_historical_rows:
            historical = historical.tail(max_historical_rows).reset_index(drop=True)
        _, game_ids = load_upcoming_scoped(target_date, session, max_game_count)
    return historical, game_ids


def main() -> None:
    args = _parse_args()
    target_date = date.today()
    settings = get_settings()
    max_historical_rows = max(0, int(args.max_historical_rows))
    max_game_count = max(0, int(args.max_game_count))
    heartbeat_seconds = max(0, int(args.heartbeat_seconds))
    smoke_historical: pd.DataFrame | None = None
    smoke_game_ids: set[int] | None = None
    if args.smoke:
        print(
            f"SMOKE MODE active (non-production): historical<= {max_historical_rows:,} rows, "
            f"games<= {max_game_count}."
        )
        smoke_historical, smoke_game_ids = _bounded_smoke_inputs(
            target_date=target_date,
            max_historical_rows=max_historical_rows,
            max_game_count=max_game_count,
        )
    print(f"Retraining model for {target_date} ({settings.model_version}) ...")
    phase_ref = {"label": "idle"}
    with session_scope() as session:
        pipeline = TrainingPipeline(
            session,
            k_seasons=(max(1, int(cast(int, args.k_seasons))) if args.k_seasons is not None else None),
            l1_alpha=(max(0.0, float(cast(float, args.l1_alpha))) if args.l1_alpha is not None else None),
            dist_family=args.dist_family if args.dist_family is not None else None,
        )

        def progress(step: int | None, total: int | None, msg: str) -> None:
            if step is None or total is None or total <= 0:
                _safe_print(f"  [~] {msg}")
                return
            _safe_print(f"  [{step}/{total}] {msg}")

        print("\n--- Training ---")
        phase_ref["label"] = "training"
        with _heartbeat(heartbeat_seconds, phase_ref):
            pipeline.train(
                progress_callback=progress,
                historical=smoke_historical,
                skip_calibration=args.smoke,
            )

        print("\n--- Predicting upcoming games ---")
        phase_ref["label"] = "prediction"
        with _heartbeat(heartbeat_seconds, phase_ref):
            predictions = pipeline.predict_upcoming(
                target_date=target_date,
                progress_callback=progress,
                game_ids=smoke_game_ids,
                historical=smoke_historical,
            )

    print(f"\nDone. Generated {len(predictions)} predictions.")

    if predictions:
        from collections import defaultdict
        by_market: dict[str, list] = defaultdict(list)
        for p in predictions:
            by_market[p.market_key].append(p)

        print("\n--- Sample predictions (first 5 per market) ---")
        for market, preds in sorted(by_market.items()):
            print(f"\n  {market.upper()} ({len(preds)} total):")
            for p in sorted(preds, key=lambda x: abs(x.calibrated_over_probability - 0.5), reverse=True)[:5]:
                fair_over = p.calibrated_over_probability
                if fair_over > 0 and fair_over < 1:
                    fair_odds = (-fair_over / (1 - fair_over)) * 100 if fair_over > 0.5 else ((1 - fair_over) / fair_over) * 100
                    odds_str = f"{fair_odds:+.0f}" if fair_over <= 0.5 else f"{-abs(fair_odds):.0f}"
                else:
                    odds_str = "N/A"
                _safe_print(
                    f"    {p.player_name:<22} line={p.sportsbook_line:>5.1f}  "
                    f"over%={fair_over:.1%}  fair={odds_str}"
                )


if __name__ == "__main__":
    main()
