"""Removed: ``pycubrid.compat`` raises ``ImportError`` with migration guidance."""

from __future__ import annotations

raise ImportError(
    "pycubrid.compat was removed. Use pycubrid or pycubrid.aio instead; "
    "see https://cubrid-lab.github.io/pycubrid/COMPAT_MIGRATION/"
)
