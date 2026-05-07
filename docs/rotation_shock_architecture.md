# Rotation Shock Architecture

## Status

Rotation shock is promoted as the authoritative v1 prediction path.

Default rollout flags:

- `ROTATION_SHOCK_ENABLED=true`
- `ROTATION_SHOCK_ABLATION_MODE=full`
- `ROTATION_SHOCK_SHADOW_MODE=false`
- `LEGACY_PIPELINE_ENABLED=true`

`LEGACY_PIPELINE_ENABLED` is kept during the promotion window as an operator
rollback affordance. It does not make legacy predictions authoritative by
itself; authoritative inference follows `ROTATION_SHOCK_ENABLED` and
`ROTATION_SHOCK_ABLATION_MODE`.

## Data Flow

Historical training and upcoming inference share the same rotation adjustment
path:

```text
availability context
-> baseline feature frame
-> baseline minutes model
-> rotation redistribution
-> adjusted minutes, usage, and rate channels
-> stat/rate models
-> availability-aware simulation
-> prediction payload and audit artifacts
```

The pure redistribution math lives in `app/training/rotation.py`. Pipeline
integration lives in `app/training/pipeline.py`, and learned redistribution
weights are loaded from `data/artifacts/rotation_weights.parquet` when present.

## Artifacts

- Learned weights: `data/artifacts/rotation_weights.parquet`
- Per-game audits: `data/artifacts/rotation_audit/<game_id>.parquet`
- Backtest reports: `reports/rotation_calibration/<timestamp>/`
- Shadow compare reports: `reports/rotation_shadow/<timestamp>/`

Prediction payloads remain backward compatible. Rotation-specific fields are
additive and are also summarized under
`Prediction.feature_attribution_summary["rotation_shock"]`.

## Rollback

Training rollback:

```powershell
$env:ROTATION_SHOCK_ENABLED = "false"
$env:ROTATION_SHOCK_ABLATION_MODE = "off"
python scripts/retrain_and_predict.py
```

Inference rollback:

```powershell
$env:ROTATION_SHOCK_ENABLED = "false"
$env:ROTATION_SHOCK_ABLATION_MODE = "off"
$env:LEGACY_PIPELINE_ENABLED = "true"
python scripts/retrain_and_predict.py
```

Shadow comparison after promotion is dry-run by default:

```powershell
python scripts/rotation_shadow_compare.py
```

Persisting legacy predictions is reserved for rollback operations:

```powershell
python scripts/rotation_shadow_compare.py --persist-legacy
```

## Monitoring

During the first promotion week, check nightly automation reports for:

- prediction count and model run count
- data quality sentinel status
- high-injury projection deltas
- clean-roster regression signals
- rotation audit sanity for high-profile absences

Useful commands:

```powershell
python scripts/run_daily_automation.py --shadow-compare
python scripts/join_rotation_shadow_with_actuals.py reports/rotation_shadow/<timestamp>/rotation_shadow_overlap.csv
python scripts/backtest_rotation_shock.py --eval-days 30 --train-days 150
```

## Deferred

The Qwen explanation layer is not part of rotation-shock v1. Future narrative
work should consume only the factual audit payloads and must not calculate or
invent projection deltas.
