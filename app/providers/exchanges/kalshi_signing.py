from __future__ import annotations

import base64
from functools import lru_cache
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

_PSS_SALT_LENGTH = 32  # SHA-256 digest size; matches Kalshi API PSS spec


@lru_cache(maxsize=4)
def _load_private_key(path_str: str) -> RSAPrivateKey:
    pem = Path(path_str).read_bytes()
    key = serialization.load_pem_private_key(pem, password=None)
    if not isinstance(key, RSAPrivateKey):
        raise ValueError(f"Kalshi private key at {path_str} is not an RSA key")
    return key


def sign_request(
    private_key_path: Path | str,
    timestamp_ms: str,
    method: str,
    path: str,
) -> str:
    key = _load_private_key(str(Path(private_key_path).resolve()))
    message = (timestamp_ms + method.upper() + path).encode("utf-8")
    signature = key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=_PSS_SALT_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("ascii")
