# Kalshi Decision Brain

## Current State

Durable brain section:

`E:/AI Brain/ClaudeBrain/05 Knowledge and Skills/Data Analysis/Kalshi Market Decision Brain/`

Repo doc:

`docs/kalshi_decision_brain.md`

## Authority Split

- Deterministic scripts choose the executable market, line, side, price gates, ranking, and row-zero winner.
- The vault stores policy, mappings, candidate frontmatter, overrides, templates, boards, and snapshots.
- The local LLM is advisory only and may downgrade to hold or observe-only.
- The preflight and guarded runner remain the final live execution gate.

## Next Implementation Targets

- `scripts/export_brain_targets.py`
- `scripts/rank_and_enrich_symbols.py`

These should read vault frontmatter, write `config/kalshi_resolution_targets.json`, preserve deterministic row-zero ordering, and snapshot outputs back into the vault.
