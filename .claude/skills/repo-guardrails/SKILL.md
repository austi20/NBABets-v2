---
name: repo-guardrails
description: Core repository guardrails, commands, and conventions for the NBA Prop Probability Engine.
user-invocable: false
---

Use this repository context whenever you work in this project.

## Goal

Make the smallest correct change with clear validation and minimal churn.

## Project layout

- `app/training/` for model training, features, artifacts, and related pipeline logic
- `app/providers/` for provider adapters, caching, rotating fallbacks, and factory registration
- `app/server/` for FastAPI sidecar endpoints and API wiring
- `desktop_tauri/` for the Tauri desktop shell and React UI
- `app/services/` for automation, startup, control-plane, parlay, and related application services
- `scripts/` for developer and operational entry points
- `tests/unit/` and `tests/integration/` for validation

## Working commands

```bash
pip install -e .[dev]
pytest
ruff check .
mypy app
powershell -File scripts/check.ps1
npm run tauri:dev --prefix desktop_tauri
python scripts/run_server.py
python scripts/retrain_and_predict.py
python scripts/deep_model_eval.py
python scripts/run_daily_automation.py
powershell -File scripts/build_tauri.ps1
```

## Rules

1. Use the existing `.venv` when present.
2. Keep secrets in `.env`, never commit them.
3. Update `pyproject.toml` when adding dependencies.
4. Preserve provider abstractions.
5. Register new providers through `app/providers/factory.py`.
6. Put tests in `tests/unit/` or `tests/integration/`.
7. Put generated artifacts in existing `data/processed/`, `reports/`, or temp directories.
8. Do not create ad hoc folders.
9. Prefer standard library first.
10. Keep code simple and readable.

## Default workflow

1. Inspect the local pattern in the target file.
2. Make the minimum correct change.
3. Update tests if behavior changed.
4. Run the narrowest useful validation.
5. Report changed files and evidence briefly.

## Additional resources

- For a command-only reference, see [commands.md](commands.md)
