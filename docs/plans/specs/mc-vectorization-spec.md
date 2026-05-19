# Monte Carlo Vectorization Spec

## 1. Objective

Replace the per-row ThreadPoolExecutor fan-out in `_parallel_market_probabilities`
(`app/training/pipeline.py:1531-1573`) with one vectorized numpy call that draws
samples for all N rows of a single market simultaneously and returns one
`over_probability` per row. Public signatures of `simulate_line_probability` and
`summarize_line_probability` do not change; a new batched code path is added
inside `app/training/distributions.py` and consumed only by the wrapper.

## 2. Signatures

Add to `app/training/distributions.py` (no edits to existing public functions):

```python
def simulate_line_probabilities_batch(
    *,
    means: np.ndarray,            # shape (N,), float
    variances: np.ndarray,        # shape (N,), float
    lines: np.ndarray,            # shape (N,), float
    minutes_means: np.ndarray,    # shape (N,), float, NaN allowed
    minutes_stds: np.ndarray,     # shape (N,), float, NaN allowed
    market_key: str,
    contexts: list[dict[str, float] | None],   # length N
    dist_family: DistFamily = "legacy",
    sample_size: int | None = None,            # default: settings.simulation_min_samples
    seed: int = 42,
) -> np.ndarray:                  # shape (N,), float, over_probability per row
    ...


def _sample_market_distribution_batch(
    *,
    means: np.ndarray,            # (N,)
    variances: np.ndarray,        # (N,)
    sample_size: int,             # S
    rng: np.random.Generator,
    minutes_means: np.ndarray,    # (N,)
    minutes_stds: np.ndarray,     # (N,)
    market_key: str | None,
    contexts: list[dict[str, float] | None],
    dist_family: DistFamily = "legacy",
) -> np.ndarray:                  # shape (N, S), float


def _sample_minutes_batch(
    minutes_means: np.ndarray, minutes_stds: np.ndarray, sample_size: int, rng: np.random.Generator,
) -> np.ndarray:                  # (N, S)


def _stack_scalar_contexts(
    contexts: list[dict[str, float] | None], key: str, fallback: float,
) -> np.ndarray:                  # (N,)
```

Per-market batched samplers (private, mirror the existing single-row functions):

```python
def _sample_generic_count_batch(means, variances, minutes, rng, contexts) -> np.ndarray  # (N, S)
def _sample_points_batch(means, variances, minutes, rng, contexts) -> np.ndarray
def _sample_rebounds_batch(means, variances, minutes, rng, contexts) -> np.ndarray
def _sample_assists_batch(means, variances, minutes, rng, contexts) -> np.ndarray
def _sample_turnovers_batch(means, variances, minutes, rng, contexts) -> np.ndarray
def _sample_threes_batch(means, minutes, rng, contexts) -> np.ndarray
```

All take `means`, `variances` as shape (N,), `minutes` as shape (N, S), and return shape (N, S).

## 3. Algorithm

### 3.1 Top-level

Single fixed-size draw at `sample_size = settings.simulation_min_samples` (10000 default).
The wrapper today calls `summarize_line_probability` (`distributions.py:49-73`) which is a
single-shot fixed-size draw — there is no early-stop in this call chain. Parity therefore
only needs to match a single fixed-size draw. Per-row early-stop is **out of scope**.

```
rng = np.random.default_rng(seed)
samples = _sample_market_distribution_batch(..., sample_size=S, rng=rng)
# samples shape (N, S)
over_counts = np.sum(samples > lines[:, None], axis=1)
return (over_counts + 0.5) / (S + 1.0)   # matches _posterior_probability at distributions.py:612
```

### 3.2 Minutes (`_sample_minutes_batch`)

Mirror `_sample_minutes` (`distributions.py:322-334`):

- NaN minutes_mean → row gets `np.full(S, 30.0)`.
- minutes_mean <= 0 → row gets zeros.
- Otherwise: `safe_mean = max(m, 1.0)`, `safe_std = max(s or sqrt(m)*0.5, 1.0)`, draw
  `rng.normal(safe_mean, safe_std, size=S)` clipped to `[6, 48]`.

Vectorized: one `rng.standard_normal(size=(N, S))` then `loc[:,None] + scale[:,None]*z`,
with NaN/non-positive rows overwritten afterward.

### 3.3 Dispatch (`_sample_market_distribution_batch`)

Mirror `_sample_market_distribution` (`distributions.py:273-319`):

1. `bounded_means = np.maximum(means, 1e-6)`, `bounded_vars = np.maximum(variances, bounded_means + 1e-6)`.
2. `minutes = _sample_minutes_batch(...)`. Track row-mask `zero_minutes_rows` where
   `minutes[i].max() <= 0` and overwrite the final result rows to zeros for those.
3. Branch:
   - `dist_family == "count_aware"` → count-aware batched path.
   - else dispatch on market_key: threes / rebounds / assists / turnovers / points / generic.

### 3.4 Generic count (`_sample_generic_count_batch`)

Mirror `_sample_generic_count` (`distributions.py:337-356`):

- `base_minutes_i = max(mean(minutes[i, :]), 1.0)` per row, broadcast (N, 1).
- `shared_latent = _stack_scalar_contexts(contexts, "shared_latent", 1.0)` shape (N,).
  In wrapper calls `shared_latent` is always scalar/absent. State this as precondition.
- `adjusted_mean = clip(means[:, None] * (minutes / base_minutes) * shared_latent[:, None], 1e-6, None)`.
- Per-row Poisson vs NB decision via `fit_count_distribution(mean_i, var_i)`:
  - Build `is_poisson` boolean mask shape (N,).
  - Draw `poisson_samples = rng.poisson(lam=adjusted_mean)` for all rows (since lam can be array).
  - For NB rows: `dispersion_i = max((var_i - mean_i) / max(mean_i**2, 1e-6), 0)`,
    `size_i = max(1/dispersion_i, 1e-3)`, `p_i = clip(size_i / (size_i + adjusted_mean[i,:]), 1e-6, 1-1e-6)`.
    Draw `rng.negative_binomial(n=size_arr[:, None], p=p_arr)` (numpy broadcasts both params).
  - Where `dispersion_i <= 1e-6` for an NB-flagged row, fall back to poisson_samples for that row.
- Return `np.where(is_poisson[:, None], poisson_samples, nb_samples).astype(float)`.

Note: legacy code uses `scipy.stats.nbinom.rvs` with scalar `size`. The batched version uses
`rng.negative_binomial`. These diverge bitwise but match statistically — see parity tolerance.

### 3.5 Threes (`_sample_threes_batch`)

Mirror `_sample_threes` (`distributions.py:378-398`):

Context scalars stacked per row: `estimated_three_point_attempts_per_minute`,
`three_point_make_rate`, `percentage_field_goals_attempted_3pt`, `shared_latent`.

Row-mask fallback to generic when attempt_rate or make_rate is non-positive or non-finite.
Compute both paths for all rows; combine via `np.where(mask[:, None], generic, three_path)`.

- attempt_rate floored at 0.02, three_share clipped [0.05, 0.85], make_rate clipped [0.12, 0.65],
  shared_latent floor 0.6.
- `base_attempt_mean = clip(minutes * attempt_rate[:,None] * shared_latent[:,None] * (0.75 + 0.5*three_share[:,None]), 0, None)`.
- `expected_makes_mean_per_row = (base_attempt_mean * make_rate[:,None]).mean(axis=1)`.
- `scaling_i = clip(means / max(expected_makes_mean_per_row, 1e-6), 0.25, 4.0)`. (Vectorized
  form of `_mean_anchor_multiplier` — confirm the scalar version uses the same clamp range; if
  it differs, follow the scalar implementation exactly.)
- `attempts_mean = clip(base_attempt_mean * scaling[:, None], 0, None)`.
- `attempts = rng.poisson(lam=attempts_mean)`.
- Return `rng.binomial(attempts, make_rate[:, None]).astype(float)`.

### 3.6 Rebounds (`_sample_rebounds_batch`)

Mirror `_sample_rebounds` (`distributions.py:401-423`):

Context scalars: `rebound_chances_total_per_minute`, `rebound_conversion_rate`, `shared_latent`,
`rebounds_std_10` (for the lognormal sigma).

Row-mask fallback to generic when chances_rate or conversion is non-positive.

- `sigma_arr[i] = min(0.30, 0.08 + 0.02 * volatility_i)`.
- `rebound_noise = rng.lognormal(mean=0.0, sigma=sigma_arr[:, None] * np.ones((1, S)))`
  (lognormal supports broadcast on sigma).
- `chance_mean = clip(base_chance_mean * scaling[:, None] * rebound_noise, 0, None)`.
- `chances = _draw_count_batch(chance_mean, variances, rng)`.
- Return `rng.binomial(chances.astype(int), conversion[:, None]).astype(float)`.

### 3.7 Assists (`_sample_assists_batch`)

Mirror `_sample_assists` (`distributions.py:426-447`). Same structure as rebounds, no
lognormal noise. Context: `assist_creation_proxy_per_minute`, `assist_conversion_rate`,
`passes_per_minute`.

### 3.8 Turnovers (`_sample_turnovers_batch`)

Mirror `_sample_turnovers` (`distributions.py:450-486`):

Context: `touches_per_minute`, `turnover_ratio`, `usage_rate`, `shared_latent`.

Row-mask fallback to generic when touches_rate or turnover_ratio is non-positive.

- `events = _draw_count_batch(event_mean, variances * 1.25, rng)`.
- `base_samples = rng.binomial(events.astype(int), turnover_ratio[:, None]).astype(float)`.
- Zero-inflation: `p_zero_arr[i] = exp(-clip(mean_i, 0.1, 5.0))`.
- `zero_mask = rng.random(size=(N, S)) < p_zero_arr[:, None]`.
- Return `np.where(zero_mask, 0.0, base_samples)`.

### 3.9 Points (`_sample_points_batch`)

Mirror `_sample_points` (`distributions.py:489-538`). Context scalars:
`field_goal_attempts_per_minute`, `estimated_three_point_attempts_per_minute`,
`free_throw_attempts_per_minute`, `usage_rate`, `true_shooting_percentage`,
`three_point_make_rate`, `free_throw_make_rate`, `points_pace_exposure`,
`points_3pt_variance`, `shared_latent`.

Row-mask fallback to generic when any of shot_rate / three_rate / free_throw_rate is
non-positive or non-finite.

- Apply all clip/floor rules from `distributions.py:506-519` per-row, broadcast `[:, None]`.
- `pace_noise = rng.lognormal(mean=0.0, sigma=volatility_sigma[:, None] * np.ones((1, S)))`.
- `multiplier = shared_latent[:, None] * (0.70 + usage_rate[:, None]) * pace_factor[:, None] * pace_noise`.
- `two_attempts = rng.poisson(lam=two_attempts_mean)`, similarly `three_attempts`, `free_throws`.
- `made_twos = rng.binomial(two_attempts, two_make_arr[:, None])`, similarly threes / FTs.
- `points = 2*made_twos + 3*made_threes + made_free_throws`.
- Per-row scale: `sim_mean_per_row = points.mean(axis=1)`. Where `sim_mean_per_row <= 0`,
  fall back to generic for that row. Else `scale = clip(means / max(sim_mean_per_row, 1e-6), 0.95, 1.05)`.
  Return `np.round(points * scale[:, None], 0)`.

### 3.10 `_draw_count_batch`

Mirror `_draw_count` (`distributions.py:541-552`). Same Poisson/NB split as 3.4, except the
scalar mean used for the fit decision is `adjusted_mean[i].mean()`.

### 3.11 Count-aware family

Mirror `_sample_count_aware_distribution` (`distributions.py:359-375`). Same shape as 3.4
but uses `params["n"]` from `fit_count_distribution` directly.

## 4. Integration point

Edit `app/training/pipeline.py:1531-1573`. Replace the body of `_parallel_market_probabilities`
with a fast path that falls back to the existing thread pool for PRA (PRA uses
`dist_family="legacy"` and is in MARKET_TARGETS, but its sampling logic is the legacy
component-combination path — keep it on the old code path for safety).

Pseudo-diff:

```python
def _parallel_market_probabilities(
    self, *, records, means, variances, lines, market_key,
    progress_callback, progress_state, detail_prefix,
) -> np.ndarray:
    n = len(records)
    if n == 0:
        return np.zeros(0, dtype=float)

    # PRA keeps the legacy per-row path (legacy combo handling, not yet batched).
    if market_key == "pra":
        return self._parallel_market_probabilities_legacy(
            records=records, means=means, variances=variances, lines=lines,
            market_key=market_key, progress_callback=progress_callback,
            progress_state=progress_state, detail_prefix=detail_prefix,
        )

    # Fast path: batched vectorized sampling.
    minutes_means = np.array(
        [_row_optional_float(r, ("adjusted_projected_minutes", "predicted_minutes", "minutes_avg_10")) or np.nan
         for r in records],
        dtype=float,
    )
    minutes_stds = np.array(
        [_row_optional_float(r, ("predicted_minutes_std", "minutes_volatility")) or np.nan for r in records],
        dtype=float,
    )
    contexts = [self._simulation_context(r, market_key) for r in records]
    dist_family = self._dist_family_for_market(market_key)

    probabilities = simulate_line_probabilities_batch(
        means=np.asarray(means, dtype=float),
        variances=np.asarray(variances, dtype=float),
        lines=np.asarray(lines, dtype=float),
        minutes_means=minutes_means,
        minutes_stds=minutes_stds,
        market_key=market_key,
        contexts=contexts,
        dist_family=dist_family,
    )

    if progress_callback is not None and progress_state[1] > 0:
        progress_callback(progress_state[0] + n, progress_state[1], f"{detail_prefix}: batch x{n}")
    return probabilities
```

The previous body (containing `summarize_task` and `_parallel_task_results`) is moved verbatim
into a new private method `_parallel_market_probabilities_legacy` with the same signature.
This preserves a known-good fallback path and keeps PRA behavior unchanged.

`_parallel_task_results` itself is NOT touched (other callers — e.g. PRA combo at line 1387 —
keep using it).

## 5. Numerical parity test plan

New file: `tests/unit/test_distributions_batch_parity.py`.

**Tolerance: 0.01 absolute** per row. Looser than the 0.005 MC noise floor because:
(a) batched NB uses `rng.negative_binomial`, legacy uses `scipy.stats.nbinom.rvs` — RNG
streams diverge bitwise; (b) legacy consumes RNG state one row at a time, batched consumes it
all-at-once, so even matched seeds produce different draws.

Cases:

1. **test_generic_count_parity** — 50 synthetic rows with means ∈ [0.5, 25],
   variances ∈ [mean+0.5, mean*3], market_key="rebounds", empty context (forces generic).
   Compare old per-row `summarize_line_probability` (called with same seed=42) vs new
   `simulate_line_probabilities_batch`. Assert `np.max(np.abs(p_old - p_new)) <= 0.01`.

2. **test_points_parity** — 50 rows with full points context drawn from fixtures.
3. **test_rebounds_parity** — 50 rows, populated rebound context.
4. **test_assists_parity** — 50 rows.
5. **test_turnovers_parity** — 50 rows including low-mean rows (zero-inflation territory).
6. **test_threes_parity** — 50 rows.
7. **test_minutes_edge_cases** — rows with `minutes_mean=None`, `minutes_mean<=0`,
   `minutes_std=None`. Assert batched output matches scalar within 0.01 each.
8. **test_partial_context_fallback** — half the rows missing required keys, forcing per-row
   mask fallback to generic. Tolerance 0.01.
9. **test_empty_input** — `simulate_line_probabilities_batch` with N=0 returns `np.zeros(0)`.
10. **test_posterior_form** — single row with deterministic mean/variance/line. Confirm
    returned value equals `(hits + 0.5) / (S + 1.0)` for the observed hits exactly.

Fixtures local to the new file: `representative_rows(market_key, n)` returning lists of
records / arrays / contexts drawn from typical training-set ranges, seeded with
`np.random.default_rng(123)`.

Run: `pytest tests/unit/test_distributions_batch_parity.py -q`.

## 6. Wall-time benchmark plan

New script: `scripts/bench_mc_batch.py` (not a pytest test).

Outline:

```python
import time
import numpy as np
from app.training.distributions import (
    summarize_line_probability,
    simulate_line_probabilities_batch,
)

N = 1000
S = 10000
rng = np.random.default_rng(7)

for market_key in ["points", "rebounds", "assists", "threes", "turnovers"]:
    means = rng.uniform(3, 25, size=N)
    variances = means + rng.uniform(0.5, 5, size=N)
    lines = means + rng.normal(0, 1.5, size=N)
    minutes_means = rng.uniform(15, 36, size=N)
    minutes_stds = rng.uniform(2, 6, size=N)
    contexts = [_realistic_context(market_key, rng) for _ in range(N)]

    t0 = time.perf_counter()
    old = np.array([
        summarize_line_probability(
            means[i], variances[i], lines[i],
            simulations=S,
            minutes_mean=minutes_means[i],
            minutes_std=minutes_stds[i],
            market_key=market_key,
            context=contexts[i],
        ).over_probability
        for i in range(N)
    ])
    t_old = time.perf_counter() - t0

    t0 = time.perf_counter()
    new = simulate_line_probabilities_batch(
        means=means, variances=variances, lines=lines,
        minutes_means=minutes_means, minutes_stds=minutes_stds,
        market_key=market_key, contexts=contexts,
        sample_size=S,
    )
    t_new = time.perf_counter() - t0

    print(f"{market_key}: old={t_old:.2f}s new={t_new:.2f}s "
          f"speedup={t_old/t_new:.1f}x max_abs_diff={np.max(np.abs(old - new)):.4f}")
```

Acceptance: every market shows `speedup >= 3.0` and `max_abs_diff <= 0.02`. Record results in
the commit message / PR description.

## 7. Out of scope

- `summarize_line_probability` (`distributions.py:49-73`) — signature, behavior, seed
  handling unchanged.
- `simulate_line_probability` (`distributions.py:118-173`) with its dynamic early-stop loop
  is unchanged. Per-row early-stop is NOT ported.
- `simulate_joint_combo_probability` / PRA combo (`distributions.py:176-270`) unchanged. PRA
  flows through the wrapper via market_key="pra" but stays on the legacy per-row code path
  via the early-exit branch in section 4.
- `_parallel_task_results` (`pipeline.py:1575-1614`) and all other callers untouched.
- No new dependencies (no numba, cython, joblib).
- No changes to calibration-fold structure or the 6-market loop.
- Progress callback fidelity drops from per-row to per-market batch — accepted tradeoff.
