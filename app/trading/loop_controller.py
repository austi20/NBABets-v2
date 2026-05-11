from __future__ import annotations

import os
import subprocess
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.config.settings import Settings
from app.trading.decision_brain import TradingBrainSyncResult, sync_decision_brain
from app.trading.loop import set_kill_switch


@dataclass(frozen=True)
class TradingLoopProcessStatus:
    state: str
    message: str
    pid: int | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    return_code: int | None = None
    command: list[str] | None = None
    log_path: str | None = None
    preflight_output: str | None = None
    brain_state: str | None = None
    selected_candidate_id: str | None = None
    selected_ticker: str | None = None


class TradingLoopController:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._process: subprocess.Popen[bytes] | None = None
        self._status = TradingLoopProcessStatus(state="idle", message="Loop has not been started.")

    def status(self) -> TradingLoopProcessStatus:
        with self._lock:
            self._refresh_locked()
            return self._status

    def start(
        self,
        *,
        settings: Settings,
        board_entry: Any,
        session_factory: Callable[[], Session],
        board_date: date | None = None,
    ) -> TradingLoopProcessStatus:
        with self._lock:
            self._refresh_locked()
            if self._process is not None and self._process.poll() is None:
                return self._status
            self._status = TradingLoopProcessStatus(
                state="starting",
                message="Running supervised brain sync before live loop start.",
                started_at=datetime.now(UTC),
            )

        brain_result: TradingBrainSyncResult | None = None
        try:
            brain_result = sync_decision_brain(
                settings=settings,
                board_entry=board_entry,
                board_date=board_date or board_entry.board_date,
                mode="supervised-live",
                resolve_markets=True,
                build_pack=True,
            )
            if brain_result.state != "synced":
                return self._set_blocked(
                    message=f"Decision brain did not produce a live-ready pack: {brain_result.state}",
                    brain_result=brain_result,
                )

            preflight = self._run_preflight(settings)
            if preflight.returncode != 0:
                return self._set_blocked(
                    message="Read-only preflight failed; live runner was not started.",
                    brain_result=brain_result,
                    preflight_output=self._tail(preflight.stdout + preflight.stderr),
                )

            set_kill_switch(session_factory, killed=False, set_by="loop_start")
            command = [
                sys.executable,
                "scripts/run_trading_loop.py",
                "--live",
                "--decisions",
                str(Path(settings.kalshi_decisions_path)),
                "--yes",
            ]
            log_path = Path(settings.logs_dir) / "kalshi_live_loop_latest.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("wb") as log_file:
                process = subprocess.Popen(
                    command,
                    cwd=str(_repo_root()),
                    env=os.environ.copy(),
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                )
            watcher = threading.Thread(target=self._watch_process, args=(process,), daemon=True)
            with self._lock:
                self._process = process
                self._status = TradingLoopProcessStatus(
                    state="running",
                    message="Live runner process started.",
                    pid=process.pid,
                    started_at=datetime.now(UTC),
                    command=command,
                    log_path=str(log_path),
                    preflight_output=self._tail(preflight.stdout + preflight.stderr),
                    brain_state=brain_result.state,
                    selected_candidate_id=brain_result.selected_candidate_id,
                    selected_ticker=brain_result.selected_ticker,
                )
            watcher.start()
            return self.status()
        except Exception as exc:  # noqa: BLE001 - surfaced as loop control status
            return self._set_blocked(
                message=f"Loop start failed: {exc}",
                brain_result=brain_result,
            )

    def kill(self, *, session_factory: Callable[[], Session]) -> TradingLoopProcessStatus:
        set_kill_switch(session_factory, killed=True, set_by="loop_kill")
        with self._lock:
            process = self._process
            if process is None or process.poll() is not None:
                self._refresh_locked()
                self._status = replace(
                    self._status,
                    state="killed",
                    message="Kill switch engaged; no active runner process.",
                    ended_at=datetime.now(UTC),
                )
                return self._status
        try:
            process.terminate()
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5.0)
        with self._lock:
            self._status = replace(
                self._status,
                state="killed",
                message="Kill switch engaged and runner process terminated.",
                ended_at=datetime.now(UTC),
                return_code=process.returncode,
            )
            self._process = None
            return self._status

    def _run_preflight(self, settings: Settings) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "scripts/kalshi_live_preflight.py"],
            cwd=str(_repo_root()),
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            timeout=float(settings.request_timeout_seconds) + 30.0,
            check=False,
        )

    def _watch_process(self, process: subprocess.Popen[bytes]) -> None:
        return_code = process.wait()
        with self._lock:
            if self._process is not process:
                return
            state = "exited" if return_code == 0 else "failed"
            message = "Live runner exited cleanly." if return_code == 0 else f"Live runner exited with code {return_code}."
            self._status = replace(
                self._status,
                state=state,
                message=message,
                ended_at=datetime.now(UTC),
                return_code=return_code,
            )
            self._process = None

    def _refresh_locked(self) -> None:
        if self._process is None:
            return
        return_code = self._process.poll()
        if return_code is None:
            return
        state = "exited" if return_code == 0 else "failed"
        message = "Live runner exited cleanly." if return_code == 0 else f"Live runner exited with code {return_code}."
        self._status = replace(
            self._status,
            state=state,
            message=message,
            ended_at=datetime.now(UTC),
            return_code=return_code,
        )
        self._process = None

    def _set_blocked(
        self,
        *,
        message: str,
        brain_result: TradingBrainSyncResult | None,
        preflight_output: str | None = None,
    ) -> TradingLoopProcessStatus:
        with self._lock:
            self._status = TradingLoopProcessStatus(
                state="blocked",
                message=message,
                started_at=self._status.started_at,
                ended_at=datetime.now(UTC),
                preflight_output=preflight_output,
                brain_state=brain_result.state if brain_result else None,
                selected_candidate_id=brain_result.selected_candidate_id if brain_result else None,
                selected_ticker=brain_result.selected_ticker if brain_result else None,
            )
            return self._status

    @staticmethod
    def _tail(text: str, limit: int = 4000) -> str:
        return text[-limit:] if len(text) > limit else text


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]
