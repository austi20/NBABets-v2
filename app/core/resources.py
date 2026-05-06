from __future__ import annotations

import ctypes
import math
import os
import sys
from dataclasses import dataclass
from functools import lru_cache

from app.config.settings import get_settings

_THREAD_ENV_VARS = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "BLIS_NUM_THREADS",
)

_ABOVE_NORMAL_PRIORITY_CLASS = 0x00008000


@dataclass(frozen=True)
class RuntimeBudget:
    total_memory_bytes: int
    memory_budget_bytes: int
    cpu_count: int
    worker_count: int
    simulation_batch_size: int
    simulation_max_samples: int


def configure_process_runtime() -> RuntimeBudget:
    budget = get_runtime_budget()
    for key in _THREAD_ENV_VARS:
        os.environ.setdefault(key, str(budget.worker_count))
    _set_process_priority()
    return budget


@lru_cache(maxsize=1)
def get_runtime_budget() -> RuntimeBudget:
    settings = get_settings()
    total_memory_bytes = max(_detect_total_memory_bytes(), 2 * 1024**3)
    memory_budget_bytes = max(
        int(total_memory_bytes * settings.runtime_memory_fraction_limit),
        512 * 1024**2,
    )
    cpu_count = max(os.cpu_count() or 1, 1)
    worker_count = min(
        cpu_count,
        max(1, math.floor(cpu_count * settings.runtime_cpu_fraction_limit)),
    )
    max_samples_by_memory = max(10_000, memory_budget_bytes // 512)
    simulation_max_samples = min(settings.simulation_max_samples, int(max_samples_by_memory))
    simulation_batch_size = min(
        max(5_000, simulation_max_samples // 10),
        settings.simulation_batch_size,
    )
    return RuntimeBudget(
        total_memory_bytes=total_memory_bytes,
        memory_budget_bytes=memory_budget_bytes,
        cpu_count=cpu_count,
        worker_count=worker_count,
        simulation_batch_size=simulation_batch_size,
        simulation_max_samples=simulation_max_samples,
    )


def _detect_total_memory_bytes() -> int:
    if sys.platform.startswith("win"):
        return _detect_windows_memory_bytes()
    if hasattr(os, "sysconf"):
        page_size = os.sysconf("SC_PAGE_SIZE")
        page_count = os.sysconf("SC_PHYS_PAGES")
        if isinstance(page_size, int) and isinstance(page_count, int):
            return int(page_size * page_count)
    return 8 * 1024**3


def _detect_windows_memory_bytes() -> int:
    class MemoryStatusEx(ctypes.Structure):
        _fields_ = [
            ("dwLength", ctypes.c_ulong),
            ("dwMemoryLoad", ctypes.c_ulong),
            ("ullTotalPhys", ctypes.c_ulonglong),
            ("ullAvailPhys", ctypes.c_ulonglong),
            ("ullTotalPageFile", ctypes.c_ulonglong),
            ("ullAvailPageFile", ctypes.c_ulonglong),
            ("ullTotalVirtual", ctypes.c_ulonglong),
            ("ullAvailVirtual", ctypes.c_ulonglong),
            ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
        ]

    status = MemoryStatusEx()
    status.dwLength = ctypes.sizeof(MemoryStatusEx)
    if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)) == 0:
        return 8 * 1024**3
    return int(status.ullTotalPhys)


def _set_process_priority() -> None:
    try:
        if sys.platform.startswith("win"):
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            ctypes.windll.kernel32.SetPriorityClass(handle, _ABOVE_NORMAL_PRIORITY_CLASS)
        elif hasattr(os, "nice"):
            os.nice(0)
    except Exception:
        return
