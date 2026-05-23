# Debug Stabilization Incident Board

Purpose: single source of truth for active reliability issues during stabilization.

## Board Rules

- One row per active incident.
- Keep scope tight: each follow-up packet should target 1 issue class and 1-3 files.
- Every row must include owner, reproducibility status, evidence artifact path, and next action.
- Mark incidents `resolved` only after command evidence is captured.

## Incident Board

| ID | Surface | Severity | Status | Owner | Repro Status | Evidence | Next Action |
|---|---|---|---|---|---|---|---|
| INC-TRAIN-001 | Training/modeling (`app/training/`) | critical | open | engineer | pending | pending | Run targeted training/predict reproduction and capture first failure signature. |
| INC-PROV-001 | Ingestion/providers (`app/providers/`, `app/tasks/ingestion.py`) | high | open | engineer | pending | pending | Reproduce provider rotation/escalation behavior and log fallback failures. |
| INC-START-001 | Startup orchestration (`app/services/startup.py`) | high | open | intern | pending | pending | Capture startup step-level failure points and ETA mismatch evidence. |
| INC-AUTO-001 | Automation report (`app/services/automation.py`) | high | open | intern | pending | pending | Run automation command in recommend and dry-run modes and collect report path. |
| INC-DESK-001 | Desktop path (`app/desktop/app.py`) | medium | open | intern | pending | pending | Confirm desktop launch path and capture any startup exception trace. |

## Severity Guide

- `critical`: blocks training/prediction correctness or hard-fails automation chain.
- `high`: major function unavailable or unreliable; no acceptable workaround.
- `medium`: degraded behavior with workaround available.
- `low`: cosmetic or minor quality issue.

## Status Guide

- `open`: issue accepted but not yet root-caused.
- `in_progress`: active packet is running against issue.
- `blocked`: cannot proceed without external dependency, credentials, or environment fix.
- `resolved`: fix merged and validated with command evidence.

## Evidence Requirements

- Command used (exact command string).
- Exit code.
- Key failure line or success indicator.
- Artifact path (log/report/output file) when available.
