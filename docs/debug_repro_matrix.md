# Debug Repro Matrix (Stabilization Baseline)

Date: 2026-04-06

Purpose: baseline reproducibility evidence for the four required commands.

## Results

| Command | Owner lane | Result | Evidence |
|---|---|---|---|
| `powershell -NoProfile -File scripts/check.ps1` | intern | failed (exit 1) | `temp/repro_matrix/check_ps1.txt` |
| `python scripts/retrain_and_predict.py` | engineer-heavy | timed out after prolonged no-output run | `temp/repro_matrix/retrain_and_predict.txt` |
| `python scripts/deep_model_eval.py` | engineer-heavy | timed out after prolonged no-output run | `temp/repro_matrix/deep_model_eval.txt` |
| `python scripts/run_daily_automation.py --agent-mode recommend --dry-run` | intern | passed (exit 0) | `temp/repro_matrix/run_daily_automation.txt` |

## Notable Failure Signatures

- `check.ps1` fails due to existing lint/type debt; initial failures include:
  - `UP035` in `app/desktop/insights.py`
  - `F401` unused imports in `app/desktop/insights.py`
  - `I001` import ordering in multiple modules
- Automation dry-run is operational and produced:
  - `reports/automation_daily_20260406T024555Z.md`

## Triage Notes

- Heavy scripts produced no incremental output in the runtime window used for baseline repro.
- Next pass should run heavy scripts as bounded engineer packets with explicit progress checkpoints or scoped test doubles.
