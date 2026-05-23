# Troubleshooting Runbooks and Milestone Cadence

Purpose: operational guide for week 3-4 hardening after stabilization baseline.

## Ownership split

- Engineer lane: heavy model/training/provider-rotation/startup orchestration fixes.
- Intern lane: reproducibility prep, narrow checks, artifact collection, log updates, and draft follow-up packets.

## Runbook 1 — Training and Model Artifacts

Surface:
- `app/training/pipeline.py`
- `app/training/data.py`
- `app/training/models.py`
- `app/training/distributions.py`
- `app/training/artifacts.py`

Symptoms:
- retrain/predict hangs or fails
- model artifacts missing on predict
- large quality drift from prior run

Checks (intern first):
1. `python -m ruff check app/training`
2. `python -m pytest tests/unit/test_features_ag_mod.py tests/unit/test_rolling_window_builder.py -q`
3. capture artifact namespace paths from configured DB and model version

Engineer escalation:
1. run `python scripts/retrain_and_predict.py`
2. inspect artifact read/write path parity and model namespace logic
3. ship 1-3 file packet with narrow regression checks

Primary failure taxonomy:
- `model_artifact`, `data`, `quality_gate`

## Runbook 2 — Provider Rotation and Ingestion

Surface:
- `app/providers/rotation.py`
- `app/providers/stats/rotating.py`
- `app/providers/odds/rotating.py`
- `app/providers/stats/nba_api.py`
- `app/tasks/ingestion.py`

Symptoms:
- data refresh incomplete
- provider fallback not triggering
- schema drift from upstream payloads

Checks (intern first):
1. `python -m ruff check app/providers app/tasks/ingestion.py`
2. `python -m pytest tests/unit/test_provider_schema_and_fallback.py -q`
3. collect provider event/report artifacts from latest automation run

Engineer escalation:
1. patch fallback/escalation behavior in a single packet
2. add targeted regression in provider schema/fallback tests
3. rerun the exact affected test scope only

Primary failure taxonomy:
- `network_provider`, `data`, `quality_gate`

## Runbook 3 — Startup Orchestration

Surface:
- `app/services/startup.py`
- `app/services/startup_cache.py`
- `app/services/startup_eta_history.py`

Symptoms:
- startup step stalls or skips unexpectedly
- ETA estimates diverge from observed durations
- partial startup success with inconsistent output

Checks (intern first):
1. run startup path and collect step-level log output
2. `python -m pytest tests/unit/test_startup_eta_history.py tests/unit/test_startup_cache.py tests/unit/test_startup_coordinator.py -q`

Engineer escalation:
1. isolate failing step transition
2. patch one transition or ETA-state behavior per packet
3. verify with targeted startup tests only

Primary failure taxonomy:
- `orchestration`, `config`

## Runbook 4 — Daily Automation and Agent Mode

Surface:
- `app/services/automation.py`
- `app/services/automation_preflight.py`
- `app/services/automation_trends.py`
- `scripts/run_daily_automation.py`

Symptoms:
- daily report generation fails
- agent mode preflight blocks unexpectedly
- trend section missing or inconsistent

Checks (intern first):
1. `python scripts/run_daily_automation.py --agent-mode recommend --dry-run`
2. `python -m pytest tests/unit/test_automation_preflight.py tests/unit/test_automation_trends.py -q`

Engineer escalation:
1. patch one report/preflight/trend failure class per packet
2. rerun affected unit and integration automation checks

Primary failure taxonomy:
- `orchestration`, `config`, `data`

## Runbook 5 — Quality Gate and Tooling Integrity

Surface:
- `scripts/check.ps1`
- lint/type/test command paths in `CLAUDE.md`

Symptoms:
- aggregate check result disagrees with split commands
- recurring lint/type regressions in same modules

Checks (intern first):
1. run `powershell -NoProfile -File scripts/check.ps1`
2. run split checks and compare exit codes

Engineer escalation:
1. fix gate logic mismatch or high-churn offenders in narrow packets
2. verify with split checks + full gate once per milestone

Primary failure taxonomy:
- `quality_gate`, `config`

## Milestone verification cadence (weeks 3-4)

### Cadence table

| Day | Owner | Command set | Pass condition | Artifact |
|---|---|---|---|---|
| Mon | intern | `python -m ruff check` on touched lanes + targeted `pytest` | no new lane regressions | `temp/repro_matrix/weekX_mon_checks.txt` |
| Tue | engineer | one heavy path (`retrain_and_predict.py` or `deep_model_eval.py`) | completes or yields actionable failure signature | `temp/repro_matrix/weekX_tue_heavy.txt` |
| Wed | intern | `run_daily_automation.py --agent-mode recommend --dry-run` | report generated | report path in `AGENT_COWORK.md` |
| Thu | engineer | targeted fix packet for highest-severity open incident | packet checks pass | packet evidence in `AGENT_COWORK.md` |
| Fri | intern + engineer | `scripts/check.ps1` plus one heavy script for top-risk lane | gate aligns with split checks; heavy script status captured | weekly summary in `ACTION_PLAN.md` |

### Weekly close checklist

- All incident rows in `docs/debug_incident_board.md` have updated status and evidence.
- Every packet in `AGENT_COWORK.md` includes failure taxonomy.
- Top-risk lane has one engineer-reviewed fix or explicit defer rationale.
- One full-gate run plus one heavy-path run is logged with artifacts.
