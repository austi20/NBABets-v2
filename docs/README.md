# Documentation Map

Purpose: make ownership and routing of operations documentation explicit.

## Program and Operations

- `full_review_governance.md` - full-project review governance contract.
- `full_review_audit_inventory.md` - complexity/routing/test-gap audit baseline.
- `troubleshooting_runbooks.md` - lane-specific troubleshooting and milestone cadence.

## Stabilization Artifacts

- `debug_incident_board.md` - active incident board with owner/severity/evidence.
- `debug_repro_matrix.md` - reproducibility baseline and command outcomes.

## Local Runtime

- Local Claude/Qwen runtime management is external to this repo.
- Default user-profile runtime home: `%LOCALAPPDATA%\\ClaudeHybridQwen35`
- Control script: `%LOCALAPPDATA%\\ClaudeHybridQwen35\\claude_qwen35_hybrid.ps1`

## Automation Release Controls

- `agent_mode_release_checklist.md` - rollout safety checklist for agent mode.
- `controlled_auto_rollout.md` - controlled auto-execution gates and rollback conditions.

## Kalshi Trading

- `kalshi_decision_brain.md` - vault-backed decision-brain adapter and authority split for Kalshi market decisions.
