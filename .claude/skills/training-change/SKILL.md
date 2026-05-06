---
name: training-change
description: Guardrails for training, feature, artifact, backtest, and evaluation changes.
disable-model-invocation: true
---

Use this skill for training or evaluation work.

## Must keep true

1. Keep feature changes explicit.
2. Preserve artifact and report paths unless the task requires a change.
3. Prefer behavior-preserving refactors.
4. Add focused tests for feature logic or evaluation rules when behavior changes.
5. Use heavy runtime commands intentionally.

## Typical files

- `app/training/`
- `app/evaluation/`
- `app/models/`
- `scripts/retrain_and_predict.py`
- `scripts/deep_model_eval.py`

## Validation ladder

### Fast
```bash
python -m ruff check app/training/<paths> app/evaluation/<paths>
python -m pytest tests/unit/<target>.py -q
```

### Medium
```bash
python -m mypy <touched-paths>
```

### Heavy
```bash
python scripts/retrain_and_predict.py
python scripts/deep_model_eval.py
```

## Change style

- Avoid broad pipeline rewrites
- Prefer one feature slice at a time
- Keep runtime cost in mind
- Add smoke or bounded paths instead of making every validation path heavy
