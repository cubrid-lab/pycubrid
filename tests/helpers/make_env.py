"""Environment for tests that run the real Makefile (#1052).

An outer ``make`` (``make test PYTHON=...`` running this suite) hands its command-line
variables to every child make in ``MAKEFLAGS`` and also exports them into its recipes,
so the pytest process inherits them. A tested make must inherit neither, or the outer
values replace the Makefile defaults the tests assert.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
# Every variable the Makefile assigns, so a new one is dropped without editing this list.
MAKEFILE_VARIABLES = frozenset(
    re.findall(r"^([A-Z][A-Z0-9_]*) *[?:+]?=", (ROOT / "Makefile").read_text(), re.MULTILINE)
)
OUTER_MAKE = frozenset({"MAKEFLAGS", "MFLAGS", "MAKEOVERRIDES", "MAKELEVEL"}) | MAKEFILE_VARIABLES


def make_env(base: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return ``base`` (default ``os.environ``) without the outer make's state."""
    source = os.environ if base is None else base
    return {key: value for key, value in source.items() if key not in OUTER_MAKE}
