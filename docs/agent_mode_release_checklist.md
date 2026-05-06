# Agent mode release checklist

Complete **before** setting `AGENT_MODE=recommend` or `AGENT_MODE=auto` in any environment where the database predates multi-agent telemetry.

## 1. Database schema

- [ ] Confirm the `agent_run_events` table exists (ORM: `AgentRunEvent` / table name `agent_run_events`).
- [ ] Run a schema sync appropriate to your deployment:
  - **Dev / local SQLite:** `python -c "from app.db.bootstrap import create_all; create_all()"` or start the app once after upgrading code.
  - **Postgres / shared DB:** apply the same metadata migration process you use for other tables (Alembic or approved DDL).
- [ ] Optional: run a daily automation report with `AGENT_MODE=off` and confirm the file is written; then enable `recommend` and confirm the report shows `preflight_ok: True` under **Agent Control Plane**.

Daily automation runs **preflight**: if the table is missing, recommend/auto **skips** the control plane and prints a failure message in the report instead of crashing.

## 2. Configuration

- [ ] Start with `AGENT_MODE=recommend` and `AGENT_DEFAULT_DRY_RUN=true` (defaults in `app/config/settings.py`).
- [ ] Keep `WORKFLOW_AGENT_ALLOW_AUTO_ACTIONS=false` until controlled rollout gates are satisfied (see `docs/controlled_auto_rollout.md`).
- [ ] Ensure provider and AI settings are valid for your environment; agent modules respect per-agent toggles (`WORKFLOW_AGENT_ENABLED`, etc.).

## 3. Operational validation

- [ ] Run `scripts/run_daily_automation.py --agent-mode recommend` and review the **Agent Control Plane** section.
- [ ] If using `auto`, first run with `--dry-run` and confirm no unintended side effects in the narrative output.

## 4. Rollback

- [ ] Set `AGENT_MODE=off` immediately if telemetry writes fail, schema errors appear, or automation volume is unexpected.

Reference: `ACTION_PLAN.md` (v1.3.x multi-agent integration) and `AGENT_COWORK.md` task **AG-AUTO-001**.
