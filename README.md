# NBA Prop Probability Engine

Can a model that projects a player's stat line beat the sportsbook on player props? This project trains one locally, turns each projection into an over and under probability for any line, and shows the ranked results in a desktop app. It covers six markets: points, rebounds, assists, threes, turnovers, and points plus rebounds plus assists (PRA). Props only.

## What I found

Not much, and I would rather say that than hide it.

Scored on held out games, the model's average miss is 1.0 to 1.8% smaller than the better of a trailing 5 game average and the project's own recency baseline on five of six markets, and 1.4% larger on threes. That comes from 10 rolling folds of 14 days (2026 01 28 to 2026 06 08, 14,390 player games per market).

| market | model MAE | best naive baseline | model vs baseline | 80% interval coverage |
|---|---:|---:|---:|---:|
| points | 4.844 | 4.908 | +1.3% | 83.1% |
| rebounds | 1.980 | 2.017 | +1.8% | 85.4% |
| assists | 1.436 | 1.455 | +1.3% | 87.7% |
| threes | 0.972 | 0.959 | -1.4% | 89.4% |
| turnovers | 0.940 | 0.957 | +1.8% | 89.1% |
| pra | 6.334 | 6.399 | +1.0% | 80.4% |

A gap that small is not proof of skill. I have not put a confidence interval on it, and a cross check from raw box scores flips the sign on rebounds and PRA. The full write up, including how this compares with my NFL prop project, is in `docs/plans/2026-10-20-checkup-and-improvement-plan.md`.

The money test is weaker still. The only backtest against real sportsbook prices covers two days (622 quotes, 452 bets). It lost 8.4% while the model's own expected value averaged +19.9%. That tells me the probabilities were too confident in that sample, and two days is not enough to say more. Calibration against the model's own outcomes is better: average expected calibration error is 0.051, and threes is the best market at 0.026.

## How it works

1. A minutes model predicts playing time.
2. An XGBoost model per market predicts the stat total and the per minute rate, and a variance model sets the spread.
3. A Monte Carlo draw turns the mean and variance into a full distribution for every player market in a game. The model does not see sportsbook lines, so the projection is independent of the market.
4. A pricing layer reads any line or Kalshi threshold off that distribution and compares it with the offered price. Isotonic calibration with a market prior maps raw probabilities to final ones, clamped to 0.04 to 0.96.
5. Injury and rotation changes redistribute minutes and usage to teammates before the model runs (`docs/rotation_shock_architecture.md`).

## The data

- **BallDontLie** supplies the current season: games, player game stats, active rosters and injury reports. All of that works on the ALL-STAR tier (60 requests a minute; set `BALLDONTLIE_MAX_RPM=540` on GOAT). Sportsbook odds and player props are GOAT only.
- **Kalshi** player prop markets (points, rebounds, assists, threes, PRA) are the line source when the BallDontLie key has no odds access, which `ODDS_PROVIDER=balldontlie` handles automatically. `ODDS_PROVIDER=kalshi` skips the BallDontLie attempt. Kalshi has no turnovers market, so turnovers have no lines on ALL-STAR.
- **Four seasons of box scores** (2022 to 2025, 111,949 player games) come from `nba_api` through `scripts/backfill_historical_box_scores.py` and sit in `data/parquet/box_scores`. The two sources use different player IDs, so rows never join across them. Training adds the parquet history when `HISTORICAL_PARQUET_ROOT` points at that folder. In an A/B run in June, adding it cut calibration error by about 46%.
- **Kalshi** supplies exchange prices for the live trading side. Live order placement is off by default (`KALSHI_LIVE_TRADING=false`) and needs explicit opt in.

Sportsbook quote history is thin: about 11,000 snapshots from May and June 2026. That is the main limit on what I can claim.

## Run it

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"
copy .env.example .env      # then add your provider keys
```

Desktop app in development:

```powershell
npm run tauri:dev --prefix desktop_tauri
```

Desktop app as a double click shortcut. The shell finds `.env` by walking up from its own exe, so it has to run from inside the repo:

```powershell
powershell -File scripts/build_sidecar.ps1
npm run tauri:build --prefix desktop_tauri -- --no-bundle
powershell -File scripts/create_desktop_shortcut.ps1
```

Checks, retraining and evaluation:

```powershell
powershell -File scripts/check.ps1          # ruff, mypy floor, pytest
python scripts/retrain_and_predict.py
python scripts/deep_model_eval.py
python scripts/baseline_comparison.py --folds <backtest_folds_*.csv>
python scripts/run_daily_automation.py --agent-mode recommend --dry-run
```

Backtest and report files land in `%LOCALAPPDATA%\NBAPropEngine\reports`. pandas is pinned below 3, and every number above comes from pandas 2.3.3.

## What would break this

- **Thin market evidence.** Two days of real quotes, no matched closing lines (CLV reads 0), and 16.7% of backtest folds meeting the sample minimum.
- **Kalshi as the line source.** Kalshi prices are asks that include the spread, thresholds come as a ladder of X.5 lines per player, and coverage is partial: on 2026 10 20 it priced 35 players across 3 games. Edges against Kalshi are not comparable to edges against sportsbook lines, and every backtest number above except the accuracy table predates the switch.
- **One draw.** Ten folds over about four and a half months of one season. The 1 to 2% accuracy gaps could move with a different window.
- **Short training windows in the backtest.** Each fold trains on 120 days, so it understates what the full four season set can do.
- **Season rollover.** The newest game in my database is from 2026 06 10. Retraining on the new season is the first thing to do before trusting any output.
- **Surfacing filters.** The edge ceiling and the disabled market list are currently off by default. Under the old 0.30 ceiling, model probabilities above 0.80 hit only 41% of the time.

## What I would do next

Capture sportsbook and Kalshi quotes and closing lines daily until there are 30 days to test against. Bootstrap the accuracy gaps by player. Try count based distributions for threes and turnovers, which is what my NFL model uses for low count stats. Tighten the interval width on the four markets where coverage runs well above 80%.

## Layout

```text
app/         config, db, server (FastAPI), evaluation, providers, services, training, trading
desktop_tauri/   Tauri shell and React frontend
scripts/     retrain, backtest, baseline comparison, build and shortcut helpers
tests/       unit and integration tests
docs/        architecture notes, rollout checklists, plans
```
