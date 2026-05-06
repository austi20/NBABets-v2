# Local Autonomy Contracts

Purpose: define strict contracts for heavy local Claude/Gemma autonomy so execution stays auditable, reversible, and policy-gated.

## Response Schema Contract

Every local autonomy completion must be parseable to one JSON object:

- `status`: `advisory` | `hold` | `execute`
- `confidence`: float in `[0.0, 1.0]`
- `summary`: concise decision rationale
- `actions`: list of action objects

Action object contract:

- `action_id`: optional string (generated if missing)
- `action_type`: action key
- `reason`: human-readable cause
- `confidence`: float in `[0.0, 1.0]`
- `payload`: object (optional)

If schema parse fails:

- execution status is forced to `hold`
- all actions are blocked
- raw response is truncated and logged

## Action Taxonomy Contract

Allowed action classes:

- `safe_auto`: can auto-execute when policy and mode permit.
- `guarded_write`: can only execute after explicit operator approval path.
- `manual_only`: never auto-executed.

Current action mapping:

- `run_refresh_all` -> `safe_auto`
- `run_backtest` -> `safe_auto`
- `retrain_and_predict` -> `guarded_write`
- `set_release_override` -> `guarded_write`
- `promote_model_candidate` -> `manual_only`
- `patch_feature_logic` -> `manual_only`

Unknown action types default to `manual_only`.

## Deterministic-Over-LLM Contract

The policy layer is authoritative:

- local autonomy may tighten release posture,
- local autonomy may not bypass deterministic hard blocks.

Current deterministic blockers:

- release status is `BLOCKED` or `HOLD`,
- overfit risk score is extreme (`>= 0.90`).

If blockers exist and autonomy requested `execute`, status is downgraded to `hold`.

## Policy State Contract

Policy state is persisted in `LOCAL_AGENT_POLICY_STATE_PATH` JSON with:

- `enabled`
- `auto_execute_safe`
- `updated_at`
- `updated_by`
- `note`

Desktop controls and automation must read/write this shared state.

## Audit Contract

Each local autonomy run writes one `AgentRunEvent`:

- `agent_role=local_autonomy`
- `event_type=local_autonomy_decision`
- `status`, `confidence`, `action_summary`, `detail`
- payload keys:
  - `snapshot_hash`
  - `overfit_risk_score`
  - `deterministic_blockers`
  - `executed_actions`
  - `blocked_actions`

This event is mandatory for traceability and rollback diagnostics.

