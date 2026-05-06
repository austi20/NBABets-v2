from __future__ import annotations

import json
from pathlib import Path

from app.config.settings import get_settings


class ProviderRotationState:
    """Round-robin provider ordering with JSON persistence.

    State is read/written at ``settings.provider_rotation_state_path`` (or a
    test ``path``). Each ``order()`` call advances the cursor for that namespace.
    """

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or get_settings().provider_rotation_state_path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def order(self, namespace: str, provider_names: list[str]) -> list[str]:
        unique_names = list(dict.fromkeys(provider_names))
        if len(unique_names) <= 1:
            return unique_names
        state = self._load()
        cursor = int(state.get(namespace, 0)) % len(unique_names)
        ordered = unique_names[cursor:] + unique_names[:cursor]
        state[namespace] = (cursor + 1) % len(unique_names)
        self._store(state)
        return ordered

    def _load(self) -> dict[str, int]:
        if not self._path.exists():
            return {}
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(payload, dict):
            return {}
        return {str(key): int(value) for key, value in payload.items()}

    def _store(self, state: dict[str, int]) -> None:
        self._path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
