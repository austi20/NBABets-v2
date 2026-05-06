from __future__ import annotations

# ruff: noqa: E402
import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db.bootstrap import create_all
from app.db.session import session_scope
from app.services.automation import generate_daily_automation_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate daily automation and optional multi-agent recommendations.")
    parser.add_argument(
        "--agent-mode",
        choices=("off", "recommend", "auto"),
        default="off",
        help="Enable multi-agent control plane in recommend or auto mode.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="When provided, never execute suggested actions, only report recommendations.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    create_all()
    with session_scope() as session:
        report_path = generate_daily_automation_report(
            session,
            agent_mode=args.agent_mode,
            dry_run=args.dry_run or args.agent_mode != "auto",
        )
    print(f"Generated automation report: {report_path}")


if __name__ == "__main__":
    main()
