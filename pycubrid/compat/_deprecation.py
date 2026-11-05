"""The retirement warning for the opt-in compatibility namespaces (#1120)."""

from __future__ import annotations

import sys
import warnings

MIGRATION_URL = "https://cubrid-lab.github.io/pycubrid/COMPAT_MIGRATION/"
MESSAGE = (
    "pycubrid.compat is deprecated and will be removed in a later 1.x MINOR release. "
    f"Use pycubrid or pycubrid.aio instead; see {MIGRATION_URL}"
)


def warn_compat_deprecated() -> None:
    """Warn at the first caller outside the compatibility namespaces.

    FutureWarning is the category because Python hides DeprecationWarning for
    imported modules by default (PEP 565), and that is where these connections
    are usually created. Every Compat use starts from a connection, so calling
    this once from the native constructor covers all the factories.
    """
    stacklevel = 2
    frame = sys._getframe(1)
    while frame.f_back is not None and frame.f_globals.get("__name__", "").startswith(
        "pycubrid.compat."
    ):
        frame = frame.f_back
        stacklevel += 1
    warnings.warn(MESSAGE, FutureWarning, stacklevel=stacklevel)
