# Integration Contracts

Purpose: reduce cross-track drift by documenting interfaces shared across training, providers, startup orchestration, and automation.

## Startup step result contract

`StartupCoordinator` step handlers may only return:

- `refresh_mode`
- `reused_training`
- `board_date`
- `cached_historical`
- `early_complete`

Any other key is treated as contract drift and rejected.

## Provider routing contract

- Configured provider chains must come from the shared chain helpers in `app/providers/factory.py`.
- Runtime provider construction and diagnostic chain iteration must remain aligned.
- Fallback behavior must be validated via provider schema/fallback tests.

## Training prediction contract

- Prediction history gating is centralized in `TrainingPipeline._apply_prediction_history_requirements`.
- Any change to minimum history logic must preserve existing filter semantics unless explicitly approved.

## Verification contract

For cross-track changes:

1. Lint changed modules.
2. Run the narrowest relevant unit tests per touched lane.
3. Log evidence in `AGENT_COWORK.md` before marking packet complete.

## Release quality guardrail contract

- Daily automation release recommendation must include a quality guardrail decision derived from latest model/backtest artifacts.
- Guardrail evaluation checks:
  - calibration diagnostics presence and ECE values,
  - watch-market (`pra`, `turnovers`) ECE ceilings,
  - backtest sample sufficiency ratio from `BacktestResult.metrics.summary_rows`.
- Guardrail is allowed to escalate recommendation severity (`GO` -> `CAUTION`/`HOLD`/`BLOCKED`) but never downgrade a stricter base status.

## Local autonomy contract

- Local autonomy recommendations must be schema-validated JSON before action parsing.
- Deterministic policy remains authoritative:
  - autonomy may tighten release posture,
  - autonomy may not bypass `HOLD`/`BLOCKED` release states.
- Action classes are enforced:
  - `safe_auto` may execute only in `AGENT_MODE=auto`, non-dry-run, and policy state `auto_execute_safe=true`.
  - `guarded_write` and `manual_only` are blocked from autonomous execution.
- Every local autonomy run must emit `AgentRunEvent` telemetry with:
  - snapshot hash,
  - action summary,
  - executed/blocked action lists,
  - deterministic blockers.
