# Full Performance Optimization

## Context

Target machine: Ryzen 7 9800X3D (8C/16T, 96MB L3), 64GB DDR5-6000, AMD Radeon RX 9070 XT, Windows 11. Stack: Python 3.14 + FastAPI + XGBoost + Monte Carlo, PyInstaller --onefile sidecar (213MB), Tauri 2 + React 19 desktop shell, SQLite for state + caches.

Audit located in conversation; ranked tier list distilled below. Calibration fold parallelism (4-wide ThreadPoolExecutor + threadpool_limits) and fold cap 200→120 already shipped in `4f68b51`. This branch (`perf/full-optimization`) addresses the rest.

## Selection criteria

For each candidate, weighed:
1. **Impact** — measurable speedup or footprint reduction.
2. **Risk** — likelihood of behavior regression.
3. **Code complexity added** — does this introduce new abstractions or dependencies?
4. **Debuggability** — does the change make future debugging harder?

Rejected anything that scored badly on (3) or (4) regardless of (1).

## Decision matrix

### Implement (low risk, clear win)

| # | Change | Files | Net code complexity |
|---|---|---|---|
| 1 | Drop unused `torch` dep | `pyproject.toml` | Removal only |
| 2 | FastAPI default = ORJSONResponse | `app/server/main.py` | +1 line |
| 3 | `winloop` event loop in uvicorn | `app/server/main.py`, `pyproject.toml` | +5 lines |
| 4 | Bump `simulation_batch_size` default | `app/config/settings.py` | Config tweak |
| 5 | `memory_map=True` on parquet reads | `app/training/data.py` | +1 arg |
| 6 | SQLite WAL + pragmas | `app/db/session.py` | +10 lines |
| 7 | `joblib.Memory` cache for FeatureEngineer | `app/training/features.py`, settings | +1 decorator, +1 setting |
| 8 | Vectorize `_fill_with_population_priors` | `app/training/features.py` | Pure refactor (kill `.iterrows()`) |
| ~~9~~ | ~~Parallelize `StatModelSuite.fit` market loop~~ | ~~`app/training/models.py`~~ | **REJECTED** (oversubscription on top of fold parallelism — see "Rejected" section) |

### Implement after Opus spec (high risk, high reward)

| # | Change | Files | Why Opus spec |
|---|---|---|---|
| ~~10~~ | ~~PyInstaller `--onefile` → `--onedir`~~ | ~~build script + Tauri config~~ | **REJECTED** (Tauri externalBin doesn't support directories — see "Rejected" section) |
| ~~11~~ | ~~Vectorize Monte Carlo per-market batched sampling~~ | ~~`app/training/distributions.py`, `app/training/pipeline.py`~~ | **DEFERRED** — spec at `docs/plans/specs/mc-vectorization-spec.md` is solid, but in-PR implementation hit too many numerical edge cases (NaN propagation through rng params, mask-then-compute design tension) without a convergent fix loop. Picking up under separate PR with proper TDD iteration. |

### Rejected

| Change | Reason rejected |
|---|---|
| Polars migration of features | Massive rewrite, breaks debuggability, conditional gain |
| Zen-tuned BLAS (AOCL/MKL) swap | 5-15% only, Windows install pain |
| Process priority HIGH (was ABOVE_NORMAL) | Risks UI starvation, marginal gain |
| GPU offload (XGBoost / numpy / torch) | AMD on Windows → no CUDA, no ROCm, no practical DirectML for XGB |
| Bulk attach context async in `data.py` | Lower ROI than the others, would touch async surface |
| Parallelize `StatModelSuite.fit` 6-market loop | Composes badly with existing fold parallelism (4 folds × 6 markets × 2 OMP = 48 threads on 16 HW). Clean fix needs a context-aware flag (parallel only on full-data fit); maintenance cost > gain since full-data fit is not the bottleneck. |
| PyInstaller `--onefile` → `--onedir` | Tauri `externalBin` only supports single-file executables. `--onedir` produces a directory (bootstrap exe + `_internal/` library tree). Migration requires splitting into externalBin + resources, updating `tauri.conf.json`, `build_sidecar.ps1`, and verifying runtime path layout. Cold-start saving (~5 sec) is one-time-per-launch on a tool kept open; not worth the install-bundle risk. |

## Execution

Sequential task list — each task gets a fresh implementer subagent, then spec compliance review, then code quality review. Order chosen so each task is independent of those after it (no merge conflicts), with the riskiest (Tauri sidecar repack + MC vectorize) last to maintain branch stability through the easy wins.

1. Drop unused `torch` dependency
2. ORJSONResponse default
3. winloop event loop
4. simulation_batch_size config bump
5. parquet `memory_map=True`
6. SQLite WAL + pragmas
7. joblib.Memory feature cache
8. Vectorize `_fill_with_population_priors`
9. Parallelize StatModelSuite market loop
10. PyInstaller `--onedir` (after Opus build/packaging plan)
11. Monte Carlo vectorization (after Opus numerical spec)

## Verification

- After every task: targeted pytest run covering touched module + `ruff check` + `mypy app`.
- After all tasks: full `pytest tests/`, rebuild sidecar, launch Tauri dev, smoke-test predictions tab and trading tab.
- Benchmark calibration wall-time on a representative training dataset before/after as the final confidence check.

## Expected outcome

| Metric | Before | After (target) |
|---|---|---|
| Calibration wall-time | ~50 min | 8-12 min |
| Sidecar binary size | 213 MB | ~110 MB |
| Sidecar cold-start | ~5-8 sec | ~1 sec |
| API endpoint JSON latency | baseline | ~2x faster for large payloads |
| Feature-build re-run | full each time | cached when input unchanged |
