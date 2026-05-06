# NBA Prop Probability Engine

Desktop NBA prop analysis app for ingesting live data, training models locally, generating predictions, and presenting ranked prop and parlay views in a native window.

## Runtime Scope

- Tauri desktop shell + React frontend + FastAPI sidecar
- Local SQLite database discovery and bootstrap
- Provider-backed ingestion with local SQLite provider cache
- Historical training, prediction generation, and rolling-origin backtests
- Ranked prop and parlay views for the local board

## Project layout

```text
app/
  config/
  core/
  db/
  server/
  evaluation/
  models/
  providers/
  schemas/
  services/
  tasks/
  training/
scripts/
```

## Setup

1. Create a virtual environment and install the project.
2. Copy `.env.example` to `.env` and set provider API keys.
3. Launch the sidecar API + Tauri desktop shell.

```bash
pip install -e .[dev]
```

## Desktop App

Use the Tauri desktop client for the local UI:

```bash
npm run tauri:dev --prefix desktop_tauri
```

Or build and install bundles:

```bash
scripts/build_tauri.ps1
scripts/smoke_tauri.ps1
```

If you need a standalone sidecar API process:

```bash
nba-prop-server --host 127.0.0.1 --port 8765
```

On startup the app finds the freshest usable local database, initializes missing schema, refreshes provider data when needed, trains models, generates predictions, runs backtests, and then loads the desktop board views.

Optional local AI summaries use `AI_LOCAL_ENDPOINT`, `AI_LOCAL_MODEL`, and `AI_LOCAL_API_KEY`. The default `.env.example` values point at the externalized Claude/Qwen runtime on `http://127.0.0.1:8080`, which now lives outside this repo.

## Developer Workflow

```bash
pytest
ruff check .
mypy app
```

## Automation Scripts

```bash
python scripts/retrain_and_predict.py
python scripts/deep_model_eval.py
python scripts/run_daily_automation.py
```

## Multi-Agent Automation

Run the daily report with integrated agent recommendations:

```bash
python scripts/run_daily_automation.py --agent-mode recommend --dry-run
```

Staged rollout guidance:
- Stage A (observe-only): `--agent-mode recommend --dry-run`
- Stage B (low-risk automation): `--agent-mode auto` with `WORKFLOW_AGENT_ALLOW_AUTO_ACTIONS=true`
- Stage C (expanded automation): keep auto mode and tune thresholds/budgets via `.env`

Track these success signals in generated reports:
- ingestion failure recovery time
- stale or orphan DB record counts
- provider/API drift alerts per week
- manual intervention frequency

## Packaging

```powershell
scripts/build_tauri.ps1
scripts/smoke_tauri.ps1
```
