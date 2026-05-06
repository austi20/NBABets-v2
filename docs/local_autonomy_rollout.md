# Local Autonomy Rollout and Evaluation

Purpose: phase heavy local Claude/Gemma integration with measurable safety and quality gates.

## Stage 0: Contract Stabilization

Exit criteria:

- schema-valid JSON responses handled (including malformed fallback),
- action taxonomy mapped and enforced,
- deterministic blocker downgrade verified,
- policy state file is readable/writable from automation and desktop.

Evidence:

- unit tests for policy state and local autonomy engine,
- automation report includes local autonomy section,
- audit events recorded in `agent_run_events`.

## Stage 1: Advisory Everywhere

Scope:

- overfit intel and debug copilot active in report generation,
- no auto execution (`auto_execute_safe=false`),
- local autonomy output affects narrative and can tighten release recommendation.

Acceptance metrics:

- malformed response rate < 10%,
- zero unauthorized writes,
- overfit signal precision review performed on at least 10 report runs.

## Stage 2: Guarded Writes

Scope:

- allow `safe_auto` actions when:
  - `AGENT_MODE=auto`,
  - `dry_run=false`,
  - policy `auto_execute_safe=true`,
  - deterministic blockers absent.

Acceptance metrics:

- safe action success rate >= 95%,
- rollback-needed rate <= 5%,
- no policy-violating action execution.

## Stage 3: Heavy Autonomy

Scope:

- sustained local autonomy across automation/troubleshooting loops,
- desktop operator controls used for live governance,
- `guarded_write` path defined with explicit approval workflow.

Acceptance metrics:

- measurable reduction in time-to-diagnose failures,
- measurable reduction in manual triage time per report,
- no increase in blocked-release false negatives.

## Downgrade and Kill-Switch Rules

Immediate downgrade to advisory-only when any is true:

- malformed output spike (>= 25% in 24h),
- 2+ failed auto actions in rolling 24h,
- any policy breach or unexpected write.

Immediate kill switch:

- set policy `enabled=false` via desktop control or policy file update.

Recovery requirements:

- root-cause packet logged in `AGENT_COWORK.md`,
- targeted validation suite green,
- one advisory-only cycle before re-enabling auto execution.

## Evaluation Dashboard Inputs

Track from existing artifacts:

- `agent_run_events` for status/action outcomes,
- `automation_daily_*.md` for decision/release posture drift,
- model diagnostics and backtest sufficiency metrics.

Minimum reporting cadence:

- daily summary in automation report,
- weekly drift and action quality snapshot.

