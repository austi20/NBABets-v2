# Scripts Directory Guide

Purpose: reduce script-routing ambiguity during development and operations.

## Development and Startup

- `dev_startup.ps1` - launch desktop startup workflow for local development.
- `run_desktop_app.py` - legacy shim that points to the Tauri desktop workflow.

Local Claude/Qwen runtime management now lives outside this repo:

- Control script: `%LOCALAPPDATA%\\ClaudeHybridQwen35\\claude_qwen35_hybrid.ps1`
- App runtime: `powershell -ExecutionPolicy Bypass -File %LOCALAPPDATA%\\ClaudeHybridQwen35\\claude_qwen35_hybrid.ps1 -Action start-app-runtime`
- Hybrid Claude launcher: `powershell -ExecutionPolicy Bypass -File %LOCALAPPDATA%\\ClaudeHybridQwen35\\claude_qwen35_hybrid.ps1 -Action launch-claude -WorkingDir <repo>`

## Evaluation and Model Operations

- `retrain_and_predict.py` - retrain current model and generate upcoming predictions.
- `deep_model_eval.py` - deep evaluation and analytics on model behavior.
- `eval_daily.ps1` - bundled daily evaluation chain.

## Automation and Reporting

- `run_daily_automation.py` - generate daily automation report and optional agent workflow actions.

## Skillpack Sync

- `sync_claude_skillpack.ps1` / `sync_claude_skillpack.py` - mirror third-party git-backed skillpacks into `.claude/commands/` and `.claude/agents/` with explicit activation control.
  - `-DryRun` - analyze packs and report intended actions without writing repo-generated outputs; cache/worktree refresh is allowed
  - `-Prune` - remove stale generated assets (on by default)
  - `-Pack <id>` - sync only the named pack and prune only that pack's stale generated assets
  - `-ForceRefetch` - re-fetch all packs even if the cached ref is current
  - Config: copy `.claude/skillpacks.example.json` to `.claude/skillpacks.local.json` and edit
  - Local imported agents use `model: inherit`; skills accept only `mirror-only` or `disabled`
  - Generated manifest entries include the resolved commit SHA for each asset

## Hybrid Cloud Key

- For hybrid cloud access, place `CLAUDE_HYBRID_CLOUD_API_KEY=<your key>` in `%LOCALAPPDATA%\\ClaudeHybridQwen35\\.env` or export it in your shell.

## Build and Smoke

- `build_tauri.ps1` - Tauri packaging workflow.
- `smoke_tauri.ps1` - post-build bundle smoke checks.

## Quality Gate

- `check.ps1` - lint/type/test quality gate (`ruff`, `mypy app`, `pytest`).
