# Kalshi Decision Brain

The durable Kalshi decision-brain section now lives in the Obsidian vault:

`E:/AI Brain/ClaudeBrain/05 Knowledge and Skills/Data Analysis/Kalshi Market Decision Brain/`

This repo should treat that vault section as the source for policy, mappings, candidate frontmatter, human overrides, templates, and audit snapshots. The repo remains the source for live market truth, quote reads, preflight checks, order submission, ledger writes, and reconciliation.

## Authority Split

- Vault: policy, mappings, overrides, candidates, templates, snapshots.
- Deterministic scripts: eligibility, ranking, row-zero selection, target export, symbol enrichment, decision-pack writing.
- Local LLM: commentary, anomaly explanation, postmortems, and optional downgrade to hold or observe-only.
- Runner and preflight: final gate for live execution.

The local LLM is not the final authority for real-money contract selection.

## Current Pipeline

```text
Vault candidate frontmatter
  -> scripts/export_brain_targets.py
  -> config/kalshi_resolution_targets.json
  -> scripts/resolve_kalshi_targets.py
  -> config/kalshi_symbols.json
  -> scripts/rank_and_enrich_symbols.py
  -> data/decisions/decisions.json
  -> scripts/kalshi_live_preflight.py
  -> scripts/run_trading_loop.py --live --decisions data/decisions/decisions.json
```

`scripts/export_brain_targets.py` and `scripts/rank_and_enrich_symbols.py` are the next missing implementation pieces. The existing resolver, pack builder, preflight, and runner should stay in the execution path.

## Row-Zero Rule

The live pack builder and live runner privilege the first executable row. The exporter/ranker must therefore produce one deterministic winner as row zero and write every runner-up as observe-only/watchlist context behind it.

## Starting Points

- Vault hub: `Kalshi Market Decision Brain.md`
- Project adapter: `20 Mappings/NBABets v2 Kalshi Wiring.md`
- Policy: `00 System/Policy Core.md`
- Schema: `00 System/Vault Schema and Export Contract.md`
- Manifest: `99 System/Exports/kalshi-decision-brain-manifest.json`
