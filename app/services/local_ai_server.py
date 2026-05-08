from __future__ import annotations

import contextlib
import logging
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import urlsplit

import httpx

from app.config.settings import Settings, get_settings

_log = logging.getLogger(__name__)


class LocalAIServer:
    """Own the app-facing llama.cpp server when it is configured locally."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._process: subprocess.Popen[bytes] | None = None
        self._stdout_handle: BinaryIO | None = None
        self._stderr_handle: BinaryIO | None = None

    def ensure_running(self) -> dict[str, Any]:
        settings = get_settings()
        binary_path = settings.ai_local_server_binary
        model_path = settings.ai_local_model_path
        if binary_path is None or model_path is None:
            return {
                "status": "skipped",
                "message": "Local AI server auto-start skipped: AI_LOCAL_SERVER_BINARY and AI_LOCAL_MODEL_PATH are not configured.",
                "owned": False,
                "endpoint": settings.ai_local_endpoint,
            }
        if self._is_healthy(settings):
            return {
                "status": "already_running",
                "message": "Local AI server already running.",
                "owned": False,
                "endpoint": settings.ai_local_endpoint,
            }

        binary_path = binary_path.expanduser()
        model_path = model_path.expanduser()
        if not binary_path.is_file():
            raise FileNotFoundError(f"AI local server binary was not found at '{binary_path}'.")
        if not model_path.is_file():
            raise FileNotFoundError(f"AI local model was not found at '{model_path}'.")

        with self._lock:
            if self._process is not None and self._process.poll() is None:
                return self._wait_for_ready_locked(settings)

            stdout_log = settings.logs_dir / "local_ai_server_stdout.log"
            stderr_log = settings.logs_dir / "local_ai_server_stderr.log"
            stdout_log.parent.mkdir(parents=True, exist_ok=True)
            self._close_handles_locked()
            self._stdout_handle = stdout_log.open("ab")
            self._stderr_handle = stderr_log.open("ab")

            env = os.environ.copy()
            rocm_bin = Path(os.environ.get("ROCM_BIN_PATH", r"C:\Program Files\AMD\ROCm\7.1\bin"))
            if rocm_bin.exists():
                env["PATH"] = f"{rocm_bin};{env.get('PATH', '')}"

            process = subprocess.Popen(
                self._build_args(settings, binary_path, model_path),
                cwd=str(binary_path.parent),
                env=env,
                stdout=self._stdout_handle,
                stderr=self._stderr_handle,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            self._process = process

        try:
            return self._wait_for_ready(settings)
        except Exception:
            self.shutdown()
            raise

    def shutdown(self) -> None:
        with self._lock:
            process = self._process
            self._process = None
            stdout_handle = self._stdout_handle
            stderr_handle = self._stderr_handle
            self._stdout_handle = None
            self._stderr_handle = None

        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

        for handle in (stdout_handle, stderr_handle):
            if handle is not None:
                with contextlib.suppress(OSError):
                    handle.close()

    def _wait_for_ready(self, settings: Settings) -> dict[str, Any]:
        with self._lock:
            return self._wait_for_ready_locked(settings)

    def _wait_for_ready_locked(self, settings: Settings) -> dict[str, Any]:
        deadline = time.monotonic() + settings.ai_local_server_wait_seconds
        while time.monotonic() < deadline:
            if self._is_healthy(settings):
                return {
                    "status": "started",
                    "message": "Local AI server ready.",
                    "owned": True,
                    "endpoint": settings.ai_local_endpoint,
                    "pid": self._process.pid if self._process is not None else None,
                }
            if self._process is not None and self._process.poll() is not None:
                raise RuntimeError(f"Local AI server exited before becoming ready (exit code {self._process.returncode}).")
            time.sleep(1.0)
        raise TimeoutError(
            f"Local AI server did not become ready within {settings.ai_local_server_wait_seconds} seconds."
        )

    def _is_healthy(self, settings: Settings) -> bool:
        health_url = f"{self._base_url(settings)}/health"
        headers = {"Authorization": f"Bearer {settings.ai_local_api_key}"}
        try:
            with httpx.Client(timeout=2.0) as client:
                response = client.get(health_url, headers=headers)
                response.raise_for_status()
                body = response.json()
            return body.get("status") == "ok"
        except Exception:
            return False

    def _base_url(self, settings: Settings) -> str:
        parsed = urlsplit(settings.ai_local_endpoint)
        scheme = parsed.scheme or "http"
        host = parsed.hostname or "127.0.0.1"
        if parsed.port is not None:
            port = parsed.port
        elif scheme == "https":
            port = 443
        else:
            port = 80
        return f"{scheme}://{host}:{port}"

    def _build_args(self, settings: Settings, binary_path: Path, model_path: Path) -> list[str]:
        parsed = urlsplit(settings.ai_local_endpoint)
        host = parsed.hostname or "127.0.0.1"
        if parsed.port is not None:
            port = parsed.port
        elif parsed.scheme == "https":
            port = 443
        else:
            port = 80
        stderr_log = settings.logs_dir / "local_ai_server_stderr.log"
        return [
            str(binary_path),
            "--model",
            str(model_path),
            "--n-gpu-layers",
            "99",
            "--flash-attn",
            "on",
            "--ctx-size",
            "32768",
            "--cache-type-k",
            "q8_0",
            "--cache-type-v",
            "q8_0",
            "--parallel",
            "1",
            "--batch-size",
            "2048",
            "--ubatch-size",
            "512",
            "--threads",
            "8",
            "--threads-batch",
            "8",
            "--numa",
            "distribute",
            "--temp",
            "0.2",
            "--top-k",
            "20",
            "--top-p",
            "0.95",
            "--min-p",
            "0.05",
            "--repeat-penalty",
            "1.1",
            "--n-predict",
            "32768",
            "--host",
            host,
            "--port",
            str(port),
            "--alias",
            settings.ai_local_model,
            "--api-key",
            settings.ai_local_api_key,
            "--metrics",
            "--log-file",
            str(stderr_log),
        ]

    def _close_handles_locked(self) -> None:
        for attr in ("_stdout_handle", "_stderr_handle"):
            handle = getattr(self, attr)
            if handle is not None:
                with contextlib.suppress(OSError):
                    handle.close()
                setattr(self, attr, None)


local_ai_server = LocalAIServer()
