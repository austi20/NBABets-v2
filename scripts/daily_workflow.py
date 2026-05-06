"""Daily NBA Prop Engine workflow CLI."""

from __future__ import annotations

# ruff: noqa: E402
import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.resources import configure_process_runtime

configure_process_runtime()

from app.db.session import session_scope
from app.services.daily_workflow import DailyWorkflowService, WorkflowResult


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the real daily troubleshooting workflow.")
    parser.add_argument(
        "--agent-mode",
        choices=("off", "recommend", "auto"),
        default="recommend",
        help="Automation report agent mode for the fresh post-startup report.",
    )
    parser.add_argument("--top-parlays", type=int, default=5, help="Number of strict 4-leg parlays to print.")
    parser.add_argument("--max-attempts", type=int, default=2, help="Maximum workflow attempts (default: 2).")
    return parser.parse_args()


def _print_result(result: WorkflowResult) -> None:
    print(f"[Workflow] status={result.final_status} attempts={result.attempt_count} board_date={result.board_date}")
    if result.report_path:
        print(f"[Workflow] report={result.report_path}")
    if result.retry_reason:
        print(f"[Workflow] retry_reason={result.retry_reason}")

    gate = result.gate_summary
    print(
        "[Gate Summary] "
        f"scheduled={gate.scheduled_games} "
        f"predictions={gate.predictions_for_board}/{gate.expected_prediction_rows} "
        f"verified_games={gate.scheduled_games_with_verified_lines}/{gate.scheduled_games} "
        f"sentinel={gate.sentinel_status} "
        f"release={gate.release_status}"
    )
    print(
        "[Payloads] "
        f"stats={gate.raw_payload_counts.get('stats', 0)} "
        f"odds={gate.raw_payload_counts.get('odds', 0)} "
        f"injuries={gate.raw_payload_counts.get('injuries', 0)}"
    )

    if gate.recoverable_reasons:
        print("[Recoverable]")
        for reason in gate.recoverable_reasons:
            print(f"  - {reason}")
    if gate.terminal_reasons:
        print("[Terminal]")
        for reason in gate.terminal_reasons:
            print(f"  - {reason}")
    if gate.report_flags:
        print("[Report Flags]")
        for flag in gate.report_flags:
            print(f"  - {flag}")

    if not result.parlays:
        return

    print("\n[Top Parlays]")
    for index, parlay in enumerate(result.parlays, start=1):
        print(
            f"{index}. {parlay.sportsbook_name} | odds {parlay.combined_american_odds:+d} | "
            f"EV {parlay.expected_profit_per_unit:.2%} | edge {parlay.edge:.2%} | "
            f"games {' / '.join(parlay.game_labels)}"
        )
        for leg in parlay.legs:
            print(
                f"   {leg.matchup} | {leg.player_name} {leg.recommended_side} {leg.line_value} "
                f"{leg.market_key.upper()} at {leg.american_odds:+d} | P={leg.hit_probability:.1%}"
            )


def main() -> int:
    args = parse_args()
    with session_scope() as session:
        result = DailyWorkflowService(session).run(
            agent_mode=args.agent_mode,
            top_parlays=args.top_parlays,
            max_attempts=args.max_attempts,
        )
    _print_result(result)
    return 0 if result.final_status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
