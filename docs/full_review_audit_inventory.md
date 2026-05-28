# Full Review Audit Inventory

Date: 2026-04-06

Purpose: consolidated intern-lane audit output for complexity, routing, tests, and repository organization.

## A) Complexity hotspots (priority targets)

Top high-complexity modules for aggressive simplification:

1. `app/desktop/app.py`
2. `app/services/ingestion.py`
3. `app/training/pipeline.py`
4. `app/training/data.py`
5. `app/training/features.py`
6. `app/services/parlays.py`
7. `app/training/models.py`
8. `app/providers/stats/nba_api.py`
9. `app/evaluation/backtest.py`
10. `app/services/provider_cache.py`
11. `app/providers/cached.py`
12. `app/services/startup.py`
13. `app/desktop/insights.py`
14. `app/training/distributions.py`
15. `app/tasks/ingestion.py`

Primary simplification pattern:
- extract pure transformations first
- isolate orchestration from transformations
- keep public interfaces stable until characterization checks pass

## B) Routing and file-layout cleanup findings

High-value cleanup candidates:

- Consolidate configured provider-chain source of truth in `app/providers/factory.py` (avoid drift between constructor and iterator surfaces).
- Clarify naming boundaries between persisted rotation state (`app/providers/rotation.py`) and rotating composites (`app/providers/stats/rotating.py`, `app/providers/odds/rotating.py`).
- Reduce startup orchestration branching in `app/services/startup.py` by step-handler decomposition.
- Decouple feature-builder constants from `app/training/features.py` to avoid cycle-prone boundaries.
- Reduce docs/process sprawl by codifying ownership and update cadence in shared runbooks.

## C) Tests and quality profile

Current test/quality shape:

- Tests are concentrated in unit modules with narrower integration coverage.
- Stronger coverage exists in provider schema/fallback and automation slices.
- Sparse coverage remains in desktop flow and full training pipeline behavior.
- Quality gates rely on:
  - `ruff check`
  - `mypy app`
  - `pytest`

Validation strategy for aggressive simplification:

1. Packet gate: scoped `ruff` + targeted `pytest` for touched surfaces.
2. Milestone gate: full `scripts/check.ps1` + one heavy-path script + one automation path.
3. Behavior lock: prioritize public behavior assertions over private helper assertions.

## D) Repository hygiene observations

- `docs/` has grown into active operational control surface; preserve but keep ownership explicit.
- `scripts/` has mixed purposes; assign categories (dev, ci, eval) in follow-up cleanup.
- Ensure temporary/generated artifacts do not pollute core review lanes.

## E) First-wave execution order

1. Track A training decomposition packets.
2. Track B provider routing/fallback consolidation.
3. Track C startup orchestration step-handler extraction.
4. Track D desktop monolith splits.
5. Track E docs/scripts/file-routing hygiene.
