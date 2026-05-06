---
name: desktop-change
description: Guardrails for Tauri desktop UI changes, sidecar startup flow, and local board views.
disable-model-invocation: true
---

Use this skill for desktop app work.

## Must keep true

1. Keep UI patches local and easy to review.
2. Preserve startup behavior unless startup is the task.
3. Prefer extracting one cohesive helper over broad UI rewrites.
4. Keep labels and user-facing text short.
5. Avoid introducing unnecessary dependencies.

## Typical files

- `desktop_tauri/src/`
- `desktop_tauri/src-tauri/`
- `app/server/` and startup helpers in `app/services/` when sidecar startup depends on them

## Validation ladder

### Fast
```bash
python -m ruff check app/server/<paths>
npm run lint --prefix desktop_tauri
npm run typecheck --prefix desktop_tauri
```

### Targeted typing
```bash
python -m mypy <new-or-isolated-desktop-module>
```

### Runtime
```bash
npm run tauri:dev --prefix desktop_tauri
```

Run the runtime check only when the change affects startup or visible UI behavior.
