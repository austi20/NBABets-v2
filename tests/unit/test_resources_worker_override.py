from __future__ import annotations

import contextvars
import threading

from app.core import resources


def test_get_worker_override_defaults_to_none() -> None:
    assert resources.get_worker_override() is None


def test_worker_override_isolated_per_context() -> None:
    """A child thread's override must not leak into the main thread's context."""
    resources.set_worker_override(None)
    seen_in_thread: list[int | None] = []

    def worker() -> None:
        resources.set_worker_override(8)
        seen_in_thread.append(resources.get_worker_override())

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()

    assert seen_in_thread == [8]
    # Main thread context is untouched by the child thread's .set().
    assert resources.get_worker_override() is None


def test_worker_override_isolated_across_copied_contexts() -> None:
    """Two copied contexts each carry their own override value."""
    resources.set_worker_override(None)

    def run_with(value: int) -> int | None:
        resources.set_worker_override(value)
        return resources.get_worker_override()

    ctx_a = contextvars.copy_context()
    ctx_b = contextvars.copy_context()

    assert ctx_a.run(run_with, 2) == 2
    assert ctx_b.run(run_with, 5) == 5
    # Neither copied context mutated the caller's context.
    assert resources.get_worker_override() is None


def test_set_worker_override_clamps_and_reflects_in_effective_count() -> None:
    resources.set_worker_override(3)
    try:
        assert resources.get_worker_override() == 3
        assert resources.get_effective_worker_count() == 3
        resources.set_worker_override(0)
        assert resources.get_worker_override() == 1
    finally:
        resources.set_worker_override(None)
