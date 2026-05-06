from __future__ import annotations

import argparse
import json
from pathlib import Path

from llm_train.eval.metrics import summarize_jsonl


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Offline eval metrics for JSONL datasets (gold or model).")
    parser.add_argument("--val-jsonl", type=Path, required=True)
    args = parser.parse_args(argv)
    summary = summarize_jsonl(args.val_jsonl)
    out = {
        "total": summary.total,
        "csv_qa_rows": summary.csv_qa_rows,
        "csv_qa_parse_ok": summary.csv_qa_parse_ok,
        "csv_qa_schema_ok": summary.csv_qa_schema_ok,
        "autonomy_parse_ok": summary.autonomy_parse_ok,
        "autonomy_unknown_action_violations": summary.autonomy_action_violations,
        "automation_rows": summary.automation_rows,
        "automation_format_ok": summary.automation_bullets_ok,
    }
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
