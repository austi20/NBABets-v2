# Controlled auto rollout (workflow agent)

This document defines **gates** for promoting workflow actions from **recommend** to **auto** execution, and how to **roll back**.

## Current safeguards (code)

- `AgentControlPlane` only executes actions when `mode == "auto"`, `dry_run` is **false**, and `WORKFLOW_AGENT_ALLOW_AUTO_ACTIONS=true`.
- Only actions marked `safe_to_auto_execute` are considered; today the low-risk paths are oriented around refresh/cache-style work — **retrain/backtest auto paths remain gated** until explicitly enabled and proven stable.
- Network and API behavior are governed by `app/config/settings.py` (circuit breaker, retries, timeouts).

## Rollout gates (check before enabling auto)

1. **Schema:** `agent_run_events` exists and preflight passes (see `docs/agent_mode_release_checklist.md`).
2. **Telemetry baseline:** At least several days of `recommend` runs with acceptable provider error rates and API coverage tier **A** or **B** (investigate **C** before auto).
3. **Trends:** Review **Automation Health Trends** and **Deterioration alerts** in recent daily reports — no sustained ECE regression or data-quality degradation.
4. **Quality guardrail:** Daily report must show `quality_guardrail_status: GO` (or explicitly reviewed `CAUTION`). Guardrail now blocks/holds on:
   - missing calibration diagnostics,
   - average ECE above release ceiling,
   - watch-market (`PRA`, `TURNOVERS`) ECE breaches,
   - low backtest sample sufficiency.
5. **Release policy:** If release status is **BLOCKED**, **HOLD**, or **CAUTION**, do not use auto execution to compensate; use explicit **release policy override** settings only with an audit reason and optional expiry (`RELEASE_POLICY_OVERRIDE_*`).

## Promotion order (suggested)

1. Keep `AGENT_MODE=recommend` while monitoring.
2. Enable `WORKFLOW_AGENT_ALLOW_AUTO_ACTIONS=true` only after gates 1–3 are met.
3. Start with `--agent-mode auto --dry-run` (or `AGENT_DEFAULT_DRY_RUN=true`) before allowing real side effects.
4. Expand allowed auto actions only after each stage is stable (see `ACTION_PLAN.md` v1.3.x notes).

## Rollback

- Set `AGENT_MODE=off` or `WORKFLOW_AGENT_ALLOW_AUTO_ACTIONS=false` immediately.
- Restore `AGENT_DEFAULT_DRY_RUN=true` for any environment that must not mutate data.
- If a bad deploy correlates with model or data issues, revert the release candidate per your normal model promotion process; the automation report’s **Champion-Challenger Snapshot** is the primary in-report signal.

Task reference: **AG-AUTO-005** in `AGENT_COWORK.md`.
