# Principled Per-Market Calibration: ECE Thresholds + Folds

## Context

Calibration quality is gated by Expected Calibration Error (ECE) thresholds that are
**unvalidated magic numbers** and **scattered** across the codebase:

- `app/training/pipeline.py:1101-1105` — `_DEFAULT_ECE_THRESHOLD = 0.05`, with per-market
  overrides `{"points": 0.07, "rebounds": 0.06}`. Points/rebounds were almost certainly
  raised *because they kept alerting* (circular), not from analysis.
- `app/services/brain/actions/selective_retrain.py:53` — `ece_threshold = 0.10`.
- `app/trading/live_pack_builder.py:443` — threshold from an env var.

ECE is computed with 10 equal-width bins on `[0,1]`, count-weighted `|confidence - accuracy|`
(`pipeline.py:1106-1129`).

**The statistical problem:** a *perfectly* calibrated model still produces ECE > 0 from
finite-sample binning noise. That noise floor is **higher for markets with fewer OOF samples
or clumpier probability distributions** (turnovers/threes are low-count and discrete; points
spread smoothly across `[0.04, 0.96]`). A flat 0.05 is therefore not "respectful to the
market." Worse, the floor depends on OOF **sample size**, which depends on calibration **fold
count** — so a fixed number cannot be correct across configurations.

This spec covers **Phase 1**: derive principled, self-calibrating ECE gates. **Phase 2**
(per-market fold-count optimization) is documented as a follow-on that consumes Phase 1's
output; it gets its own plan.

## Goals

- Replace every hardcoded ECE threshold with a **dynamic, self-calibrating gate**: each
  calibration run computes its own per-market null floor from that run's OOF and gates the
  observed ECE against it. No committed magic numbers.
- Make the gate **sample-size aware** and **market-shape aware** by construction.
- Produce a one-time **research report** documenting, per market, observed ECE vs the null
  floor — i.e. which markets are *genuinely* miscalibrated vs merely noisy at their sample size.
- Emit a reusable `floor(market, fold_count)` table that Phase 2's compliance gate consumes.
- Capture the OOF **once** (one instrumented calibration run) and drive both the threshold
  analysis and the Phase 2 fold sweep from that single artifact — no grid of expensive runs.

## Non-Goals

- Changing the ECE *metric* (keep 10-bin count-weighted ECE).
- Phase 2 implementation (per-market fold participation refactor + sweep). Designed below for
  continuity, specced/planned separately.
- Tuning the distribution models themselves.

## Design — Phase 1

### A1. Shared OOF capture (one run, reused by both phases)

Add a flag-gated capture path to the calibration pass in `app/training/pipeline.py`. When
enabled (env `CALIBRATION_DUMP_OOF=1` / settings flag), a single max-fold run with **full
participation** persists, per market and per fold:

- the OOF predicted probabilities and binary labels (`oof_raw`, `oof_labels` already exist
  in-memory in `_fit_calibrators`),
- per-fold wall-time and per-market fit+MC time (for Phase 2's runtime axis).

Artifact: `data/artifacts/calibration_oof_dump.json` (probs/labels as arrays; timing map).
Normal (non-capture) runs are unaffected. The capture run should use a generous ceiling
(default `max_folds = 20`) so Phase 2 can evaluate fold subsets up to 20.

Because a market's OOF in a fold depends only on that fold's train split (independent of which
other markets ran), this one dump is sufficient for **both** the threshold null-sim and the
Phase 2 sweetspot analysis.

### A2. Null-sim threshold derivation

New module `app/training/calibration_ece.py` with pure, testable functions:

```python
def compute_ece(probs: np.ndarray, labels: np.ndarray, n_bins: int = 10) -> float:
    """10-bin count-weighted |confidence - accuracy|. Extracted verbatim from the
    existing pipeline computation so production and analysis share one implementation."""

def null_ece_distribution(
    probs: np.ndarray, *, n_bins: int = 10, n_boot: int = 2000,
    rng: np.random.Generator,
) -> np.ndarray:
    """Null distribution of ECE achievable by a PERFECTLY calibrated model at this
    probability vector. Each draw: y_i ~ Bernoulli(probs_i); recompute compute_ece(probs, y).
    Returns array of length n_boot."""

def null_floor(
    probs: np.ndarray, *, percentile: float = 99.0, n_bins: int = 10,
    n_boot: int = 2000, rng: np.random.Generator,
) -> float:
    """percentile-th percentile of null_ece_distribution — the max ECE consistent with
    perfect calibration at this sample size and probability shape."""
```

The default gate percentile is **99** (minimizes false alarms). The analysis also reports the
95th for inspection. Bootstrap RNG is seeded for determinism.

`floor(market, fold_count)` is produced by the offline analysis (A3 report) by subsampling the
captured OOF down to each candidate fold count's equivalent sample size and calling `null_floor`.

Optional refinement (deferred unless floors look too tight): perturb `probs` by MC sampling
noise (~`1/sqrt(samples)`) inside the null draw to fold in MC-induced ECE.

### A3. Dynamic self-calibrating gate + centralization

In the calibration diagnostics loop (`pipeline.py:1110-1153`):

- Compute observed `ece` per market (via `compute_ece`).
- Compute `floor = null_floor(probs, percentile=99, rng=seeded)` from **that run's** OOF.
- `passed = ece <= floor`. Log observed ECE, the floor, and the verdict.
- Store `{"ece", "null_floor_p99", "null_floor_p95", "passed", "sample_count", "fold_count"}`
  per market in `calibration_health` / `calibration_diagnostics`.

Delete `_DEFAULT_ECE_THRESHOLD` and `_MARKET_ECE_THRESHOLDS` (the magic numbers). Downstream
consumers read the stored per-market floor instead of their own constants:

- `selective_retrain.py` and `live_pack_builder.py` consume the persisted
  `calibration_health` floor (or a thin accessor) rather than `0.10` / an env var. Where a
  live value isn't available, they fall back to the most recent persisted floor.

A standalone, re-runnable script `scripts/analyze_calibration_ece.py` loads the A1 dump, runs
the null-sim per market at full size and at fold-subsample sizes, and writes a committed report
`docs/plans/reports/calibration-ece-findings.md`: per market — observed ECE, null mean / p95 /
p99, verdict (genuine miscalibration vs noise), and the `floor(market, fold_count)` table.

### A4. Validation

Unit tests in `tests/unit/test_calibration_ece.py`:

1. **Perfect calibration passes** — synthetic `probs`, `labels ~ Bernoulli(probs)`; observed
   ECE ≤ `null_floor` (seeded).
2. **Miscalibration breaches** — shift probs by +0.10 vs the label-generating probs; observed
   ECE > `null_floor`.
3. **Monotonic floor** — `null_floor` decreases as `n` grows (e.g. n=500 vs n=20000).
4. **Determinism** — same seed → identical floor.
5. **`compute_ece` parity** — matches the previous inline pipeline computation on a fixed input.

## Design — Phase 2 (follow-on, separate plan)

Recorded here so the agreed design isn't lost; **not** implemented in this spec.

- **Per-market fold config:** `calibration_folds_by_market: dict[str,int]` + a `calibration_max_folds`
  ceiling. Fold structure built once at `max(counts)`.
- **Participation-subset calibration:** one shared fold structure; each market fits its model +
  runs MC only in its assigned fold subset (selection strategy chosen per market by the sweep —
  evenly-spaced vs last-k, whichever gives lower ECE). `MinutesModel` fits every fold (shared);
  `StatModelSuite.fit` gains a `markets` subset param. Savings: a market with `k < max` skips
  its XGBoost fit + MC in `(max - k)` folds.
- **Sweetspot rule:** per market, among fold counts whose ECE stays under the **Phase 1 null
  floor at that sample size**, pick the best `0.7·accuracy + 0.3·runtime` blend.
- **One-run sweep:** all analysis runs offline on the A1 dump (OOF + per-fold timing). No grid
  of runs.

## Affected files (Phase 1)

- `app/training/calibration_ece.py` — new (compute_ece, null_ece_distribution, null_floor).
- `app/training/pipeline.py` — flag-gated OOF dump; dynamic gate replacing magic thresholds;
  store floors in diagnostics.
- `app/config/settings.py` — capture flag + `calibration_oof_max_folds` (default 20),
  `calibration_ece_percentile` (default 99), `calibration_ece_n_boot` (default 2000).
- `app/services/brain/actions/selective_retrain.py`, `app/trading/live_pack_builder.py` —
  consume persisted floors instead of own constants.
- `scripts/analyze_calibration_ece.py` — new analysis/report generator.
- `tests/unit/test_calibration_ece.py` — new.
- `docs/plans/reports/calibration-ece-findings.md` — generated, committed.

## Risks / Notes

- **Interim perf changes already staged (uncommitted):** thread-oversubscription fix
  (independently sound — keep) and provisional `calibration_max_folds=10` / calibration MC
  `simulations=2500`. The latter two are themselves *unvalidated* numbers; they are interim
  defaults only and are superseded by Phase 2's data-driven per-market fold counts. They must
  not be treated as final.
- **Dynamic gate stability:** a pathological run could compute an unusually loose floor. If
  observed in practice, fall back to the Hybrid option (`max(dynamic_floor, small_absolute_floor)`).
- **Capture run cost:** one ~20-fold calibration run (local, once) with the thread fix applied.
  Acceptable; all subsequent analysis is offline.
- **Bootstrap cost in production gate:** `n_boot=2000` ECE computations on existing OOF arrays
  per market — sub-second; negligible at end of calibration.
