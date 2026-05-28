"""Root conftest: add repo root to sys.path for scripts package imports."""
from __future__ import annotations

import sys
from pathlib import Path

# Add repo root to path early so `import scripts.xxx` works in all test modules.
_ROOT = str(Path(__file__).resolve().parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
