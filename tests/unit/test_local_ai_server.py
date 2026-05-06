from __future__ import annotations

import uuid
from pathlib import Path

from app.services.local_ai_server import local_ai_server


def test_local_ai_server_skips_when_not_configured(monkeypatch) -> None:
    from app.config.settings import get_settings

    monkeypatch.setenv("AI_LOCAL_SERVER_BINARY", "")
    monkeypatch.setenv("AI_LOCAL_MODEL_PATH", "")
    monkeypatch.setenv("AI_LOCAL_ENDPOINT", "http://127.0.0.1:8080/v1/chat/completions")
    get_settings.cache_clear()

    try:
        outcome = local_ai_server.ensure_running()
    finally:
        local_ai_server.shutdown()
        get_settings.cache_clear()

    assert outcome["status"] == "skipped"
    assert "not configured" in outcome["message"].lower()


def test_local_ai_server_build_args_uses_endpoint_and_alias(monkeypatch) -> None:
    from app.config.settings import get_settings

    root = Path("temp") / f"pytest_local_ai_server_{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    binary = root / "llama-server.exe"
    model = root / "qwen.gguf"
    binary.write_bytes(b"")
    model.write_bytes(b"")

    monkeypatch.setenv("AI_LOCAL_SERVER_BINARY", str(binary))
    monkeypatch.setenv("AI_LOCAL_MODEL_PATH", str(model))
    monkeypatch.setenv("AI_LOCAL_ENDPOINT", "http://127.0.0.1:8099/v1/chat/completions")
    monkeypatch.setenv("AI_LOCAL_MODEL", "qwen3-1.7b-q8")
    get_settings.cache_clear()

    try:
        settings = get_settings()
        args = local_ai_server._build_args(settings, binary, model)
    finally:
        local_ai_server.shutdown()
        get_settings.cache_clear()

    assert "--model" in args
    assert str(model) in args
    assert "--port" in args
    assert args[args.index("--port") + 1] == "8099"
    assert "--alias" in args
    assert args[args.index("--alias") + 1] == "qwen3-1.7b-q8"
