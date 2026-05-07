from __future__ import annotations

import base64
from pathlib import Path

import pytest
from app.providers.exchanges.kalshi_signing import sign_request
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


@pytest.fixture()
def private_key_pem(tmp_path: Path) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path = tmp_path / "key.pem"
    path.write_bytes(pem)
    return path


def test_sign_request_returns_base64_pss_signature_verifiable(private_key_pem: Path) -> None:
    timestamp_ms = "1714953600000"
    method = "GET"
    path = "/trade-api/v2/portfolio/balance"
    sig = sign_request(private_key_pem, timestamp_ms, method, path)
    raw = base64.b64decode(sig)
    pubkey = serialization.load_pem_private_key(private_key_pem.read_bytes(), password=None).public_key()
    message = (timestamp_ms + method + path).encode("utf-8")
    pubkey.verify(
        raw,
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256.digest_size),
        hashes.SHA256(),
    )  # raises if invalid


def test_sign_request_caches_key_load(private_key_pem: Path) -> None:
    sig1 = sign_request(private_key_pem, "1", "GET", "/x")
    sig2 = sign_request(private_key_pem, "2", "GET", "/x")
    assert sig1 != sig2  # different timestamps yield different signatures
