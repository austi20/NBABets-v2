---
name: automation-change
description: Guardrails for automation, daily reports, release gates, local autonomy, and control-plane behavior.
disable-model-invocation: true
---

Use this skill for automation and governance work.

## Must keep true

1. Keep dry-run and advisory behavior explicit and safe.
2. Preserve release-gate clarity.
3. Prefer narrow policy or report fixes over broad orchestration rewrites.
4. Add focused tests for release rules, trend calculations, and automation decisions.
5. Surface environment or DB assumptions clearly.

## Typical files

- `app/services/automation.py`
- `app/services/automation_trends.py`
- `app/services/automation_preflight.py`
- `app/services/local_autonomy/`
- `app/services/agents/control_plane.py`
- `scripts/run_daily_automation.py`

## Validation ladder

### Fast
```bash
python -m ruff check app/services/<paths> scripts/run_daily_automation.py
python -m pytest tests/unit/<target>.py -q
```

### Integration
```bash
python -m pytest tests/integration/test_daily_automation.py -q
```

### Runtime
```bash
python scripts/run_daily_automation.py --agent-mode recommend
python scripts/run_daily_automation.py --agent-mode auto --dry-run
```

Use runtime checks only when behavior in reports, release gates, or action selection changed.
