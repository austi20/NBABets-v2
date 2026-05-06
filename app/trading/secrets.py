from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

keyring_module: Any | None
try:
    import keyring as keyring_module
except ImportError:  # pragma: no cover - optional import path on constrained hosts
    keyring_module = None


@dataclass(frozen=True)
class SecretRef:
    service: str
    account: str


def can_use_keyring() -> bool:
    return keyring_module is not None


def store_secret(secret_ref: SecretRef, value: str) -> None:
    if keyring_module is None:
        raise RuntimeError("keyring is not available in this environment")
    keyring_module.set_password(secret_ref.service, secret_ref.account, value)


def load_secret(secret_ref: SecretRef) -> str | None:
    if keyring_module is None:
        return None
    try:
        return cast(str | None, keyring_module.get_password(secret_ref.service, secret_ref.account))
    except Exception:
        return None
