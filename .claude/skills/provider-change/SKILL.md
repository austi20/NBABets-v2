---
name: provider-change
description: Guardrails for provider-layer work in app/providers, factory registration, caching, rotating fallbacks, and schema safety.
disable-model-invocation: true
---

Use this skill for provider-layer changes.

## Must keep true

1. Preserve provider abstractions.
2. Register new providers through `app/providers/factory.py`.
3. Keep rotating fallback chains explicit and easy to inspect.
4. Prefer minimal schema-safe fixes over broad refactors.
5. Avoid hidden behavior in caching layers.
6. Call out API-key or tier assumptions clearly.

## Typical files

- `app/providers/factory.py`
- `app/providers/cached.py`
- `app/providers/canonical_schema.py`
- `app/providers/odds/`
- `app/providers/stats/`
- `app/providers/injuries/`

## Typical validation

```bash
python -m ruff check app/providers/<paths>
python -m pytest tests/unit/test_provider_schema_and_fallback.py -q
```

Add other provider tests only if the touched behavior requires them.

## Change style

- Small patch first
- Keep provider names and chains explicit
- Prefer additive registry updates over framework changes
- Preserve existing env-based configuration patterns

## Additional resources

- For a compact provider checklist, see [checklist.md](checklist.md)
