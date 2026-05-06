---
name: quality-gate
description: Run the repository quality gate, or a narrower targeted validation path when full-gate execution is unnecessary.
disable-model-invocation: true
---

Use this skill when the task is about validation, regressions, or proving a patch is safe.

## Validation order

Choose the smallest check that gives real confidence.

### Level 1, file-local
Use for formatting, imports, basic lint, and tiny edits.

```bash
python -m ruff check <paths>
```

### Level 2, narrow behavior
Use for a touched unit or integration area.

```bash
python -m pytest tests/unit/<target>.py -q
python -m pytest tests/integration/<target>.py -q
```

### Level 3, narrow typing
Use when a touched module has meaningful typing and mypy signal is useful.

```bash
python -m mypy <paths>
```

### Level 4, repo gate
Use only when the task needs full-repo confidence.

```bash
powershell -File scripts/check.ps1
```

## Heavy runtime checks

Use only when the task affects these flows directly.

```bash
npm run tauri:dev --prefix desktop_tauri
python scripts/run_server.py
python scripts/retrain_and_predict.py
python scripts/deep_model_eval.py
python scripts/run_daily_automation.py
```

## Output format

- Checks run
- Pass/fail
- What remains unverified
- Any environment assumptions

## Additional resources

- For a faster decision checklist, see [checklist.md](checklist.md)
