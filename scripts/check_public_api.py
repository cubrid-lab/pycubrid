#!/usr/bin/env python3
"""Verify that the public API surface has not changed unexpectedly.

This script extracts the public API surface of ``pycubrid`` and ``pycubrid.aio``
and compares it against a committed baseline (``api-baseline.json``). If the
surface differs, the script prints a structured diff and exits non-zero.

The intent is to catch *accidental* breaking changes (renamed parameter,
removed method, added required argument) before they ship. Intentional changes
are accommodated by regenerating the baseline:

    python scripts/check_public_api.py --update

When a developer regenerates the baseline, the diff is visible in the pull
request and reviewers can decide whether the change is acceptable under the
project's release policy (see ``RELEASE_POLICY.md``). For the 1.x line, any
non-additive surface change requires a major version bump (2.0+).

Usage:
    python scripts/check_public_api.py            # verify (CI mode)
    python scripts/check_public_api.py --update   # regenerate baseline

Exit codes:
    0 — surface matches baseline (or update succeeded)
    1 — surface differs from baseline
    2 — internal/import error

Design notes:
    * Runtime introspection (``inspect`` module) is used rather than AST
      parsing so that lazy attributes (``__getattr__``) and re-exports are
      resolved exactly the way end users see them.
    * Only structural information is compared: parameter names, kinds, and
      whether they have a default. Type annotations are intentionally NOT
      compared because they are routinely refined without changing behavior.
    * Private symbols (leading underscore) are skipped at every level.
"""

from __future__ import annotations

import argparse
import functools
import importlib
import inspect
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
BASELINE_PATH = ROOT / "api-baseline.json"

# Modules whose public surface is tracked. Both modules are checked together;
# the JSON file is written with sort_keys=True so on-disk order is alphabetical
# regardless of the order declared here.
TRACKED_MODULES = ("pycubrid", "pycubrid.aio")

# Classes that are not listed in any ``__all__`` but are part of the de facto
# public API because users receive them as return values (``pycubrid.connect()``
# returns ``Connection``; ``connection.cursor()`` returns ``Cursor``; etc.).
# Each entry becomes a top-level key in the baseline JSON, separately from
# ``TRACKED_MODULES``.
TRACKED_CLASSES: tuple[tuple[str, str], ...] = (
    ("pycubrid.connection", "Connection"),
    ("pycubrid.cursor", "Cursor"),
    ("pycubrid.aio.connection", "AsyncConnection"),
    ("pycubrid.aio.cursor", "AsyncCursor"),
    ("pycubrid.lob", "Lob"),
)

# Dunder methods we *do* protect on tracked classes. Anything not on this list
# is treated as "Python language detail" and ignored — this is what keeps the
# baseline stable across CPython versions (e.g. ``BaseException.add_note`` added
# in 3.11 does not appear here).
TRACKED_DUNDERS = frozenset(
    {
        "__init__",
        "__enter__",
        "__exit__",
        "__aenter__",
        "__aexit__",
        "__iter__",
        "__aiter__",
        "__next__",
        "__anext__",
        "__repr__",
        "__str__",
    }
)

# Module prefix that defines "our" code. Members inherited from outside this
# prefix (built-ins, stdlib bases, third-party mixins) are skipped so the
# baseline is not affected by upstream CPython surface drift.
OWN_MODULE_PREFIX = "pycubrid"

# JSON-serializable scalar types whose *value* is captured for module-level
# public attributes — but only when the attribute name is in
# ``VALUE_TRACKED_ATTRIBUTES`` below. Other scalar attributes are recorded by
# type only (matching how non-scalar attributes are recorded).
SCALAR_VALUE_TYPES: tuple[type, ...] = (str, int, float, bool, type(None))

# Module-level scalar attributes whose *value* is part of the 1.x contract and
# therefore captured into the baseline. Limited to PEP 249 contract constants
# whose semantic meaning depends on the literal value; bumping any of these
# requires the same explicit review as any other surface change. Crucially,
# ``__version__`` is NOT in this set: bumping the version is the routine
# release-engineering action gated separately by ``scripts/check_version.py``,
# so capturing it here would force a redundant baseline regeneration on every
# version bump.
VALUE_TRACKED_ATTRIBUTES: frozenset[str] = frozenset({"paramstyle", "apilevel", "threadsafety"})


def _signature_to_dict(sig: inspect.Signature) -> dict[str, Any]:
    """Serialize an ``inspect.Signature`` into a stable, JSON-friendly dict.

    Only structural information is captured. Type annotations (both parameter
    and return) are excluded by design — see module docstring and
    ``RELEASE_POLICY.md`` §1 "Out of Scope".
    """
    params: list[dict[str, Any]] = []
    for param in sig.parameters.values():
        params.append(
            {
                "name": param.name,
                "kind": param.kind.name,
                "has_default": param.default is not inspect.Parameter.empty,
            }
        )
    return {"params": params}


def _describe_callable(obj: Any) -> dict[str, Any]:
    try:
        sig = inspect.signature(obj)
    except (TypeError, ValueError):
        return {"kind": "callable", "signature": None}
    return {"kind": "callable", "signature": _signature_to_dict(sig)}


def _defining_class(cls: type, name: str) -> type | None:
    for base in cls.__mro__:
        if name in base.__dict__:
            return base
    return None


def _is_own_member(cls: type, name: str) -> bool:
    defining = _defining_class(cls, name)
    if defining is None:
        return False
    module = getattr(defining, "__module__", "") or ""
    return module == OWN_MODULE_PREFIX or module.startswith(OWN_MODULE_PREFIX + ".")


def _describe_class(cls: type) -> dict[str, Any]:
    """Describe a class: its public methods (with signatures) and properties.

    Only members whose *defining* class lives under the ``pycubrid`` package
    are included, plus a small allow-list of dunders that pycubrid itself
    overrides. This keeps the baseline stable across CPython versions:
    ``BaseException.add_note`` (added in 3.11) and similar built-in dunders
    are deliberately not part of the surface contract.

    Descriptor kinds are normalized: ``classmethod`` and ``staticmethod`` are
    surfaced as methods with their underlying signatures; ``property`` and
    ``functools.cached_property`` are surfaced as properties.
    """
    methods: dict[str, dict[str, Any]] = {}
    properties: list[str] = []
    class_vars: list[str] = []

    for name in sorted(dir(cls)):
        if name.startswith("_") and name not in TRACKED_DUNDERS:
            continue
        if not _is_own_member(cls, name):
            continue
        try:
            attr = inspect.getattr_static(cls, name)
        except AttributeError:
            continue

        if isinstance(attr, (property, functools.cached_property)):
            properties.append(name)
        elif isinstance(attr, staticmethod):
            methods[name] = _describe_callable(attr.__func__)
        elif isinstance(attr, classmethod):
            methods[name] = _describe_callable(attr.__func__)
        elif inspect.isfunction(attr) or inspect.ismethod(attr):
            try:
                bound = getattr(cls, name)
            except AttributeError:
                continue
            methods[name] = _describe_callable(bound)
        elif callable(attr):
            try:
                bound = getattr(cls, name)
            except AttributeError:
                continue
            methods[name] = _describe_callable(bound)
        else:
            class_vars.append(name)

    own_bases = [
        f"{base.__module__}.{base.__qualname__}"
        for base in cls.__bases__
        if (
            getattr(base, "__module__", "") == OWN_MODULE_PREFIX
            or getattr(base, "__module__", "").startswith(OWN_MODULE_PREFIX + ".")
        )
    ]

    return {
        "kind": "class",
        "bases": own_bases,
        "methods": methods,
        "properties": sorted(properties),
        "class_vars": sorted(class_vars),
    }


def _describe_attribute(name: str, value: Any) -> dict[str, Any]:
    type_name = type(value).__name__
    entry: dict[str, Any] = {"kind": "attribute", "type": type_name}
    if name in VALUE_TRACKED_ATTRIBUTES and isinstance(value, SCALAR_VALUE_TYPES):
        entry["value"] = value
    return entry


def extract_surface(module_name: str) -> dict[str, Any]:
    """Extract the public API surface of a single module.

    Public surface is defined as the union of:
        * Items listed in the module's ``__all__`` (authoritative).
        * If ``__all__`` is missing, all non-underscore module attributes.

    For each entry we record either a callable signature, a class structure,
    or an attribute type.
    """
    module = importlib.import_module(module_name)

    if hasattr(module, "__all__"):
        public_names = list(module.__all__)
    else:
        public_names = [name for name in dir(module) if not name.startswith("_")]

    entries: dict[str, dict[str, Any]] = {}
    for name in sorted(public_names):
        try:
            value = getattr(module, name)
        except AttributeError as exc:
            # Listed in __all__ but missing — surface itself is broken.
            raise RuntimeError(
                f"{module_name}.{name} is listed in __all__ but cannot be resolved: {exc}"
            ) from exc

        if inspect.isclass(value):
            entries[name] = _describe_class(value)
        elif callable(value):
            entries[name] = _describe_callable(value)
        else:
            entries[name] = _describe_attribute(name, value)

    return {
        "__all__": sorted(public_names),
        "entries": entries,
    }


def extract_full_surface() -> dict[str, Any]:
    surface: dict[str, Any] = {name: extract_surface(name) for name in TRACKED_MODULES}
    for module_name, class_name in TRACKED_CLASSES:
        module = importlib.import_module(module_name)
        try:
            cls = getattr(module, class_name)
        except AttributeError as exc:
            raise RuntimeError(
                f"{module_name}.{class_name} is tracked but not importable: {exc}"
            ) from exc
        surface[f"{module_name}.{class_name}"] = _describe_class(cls)
    return surface


def _diff_dicts(expected: dict[str, Any], actual: dict[str, Any], path: str = "") -> list[str]:
    diffs: list[str] = []
    expected_keys = set(expected.keys())
    actual_keys = set(actual.keys())

    for key in sorted(expected_keys - actual_keys):
        diffs.append(f"REMOVED:   {path}{key}")
    for key in sorted(actual_keys - expected_keys):
        diffs.append(f"ADDED:     {path}{key}")
    for key in sorted(expected_keys & actual_keys):
        e_val = expected[key]
        a_val = actual[key]
        sub_path = f"{path}{key}."
        if isinstance(e_val, dict) and isinstance(a_val, dict):
            diffs.extend(_diff_dicts(e_val, a_val, sub_path))
        elif e_val != a_val:
            diffs.append(f"CHANGED:   {path}{key}")
            diffs.append(f"           expected: {json.dumps(e_val, sort_keys=True)}")
            diffs.append(f"           actual:   {json.dumps(a_val, sort_keys=True)}")
    return diffs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--update",
        action="store_true",
        help="Regenerate api-baseline.json from the current surface.",
    )
    args = parser.parse_args(argv)

    try:
        current = extract_full_surface()
    except Exception as exc:  # pragma: no cover — defensive: surface itself is broken
        print(f"ERROR: failed to extract public API surface: {exc}", file=sys.stderr)
        return 2

    if args.update:
        BASELINE_PATH.write_text(
            json.dumps(current, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(f"Baseline updated: {BASELINE_PATH}")
        return 0

    if not BASELINE_PATH.exists():
        print(
            f"ERROR: baseline missing at {BASELINE_PATH}. Run with --update to create it.",
            file=sys.stderr,
        )
        return 2

    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    diffs = _diff_dicts(baseline, current)

    if not diffs:
        print(f"✓ Public API surface matches baseline ({BASELINE_PATH.name})")
        return 0

    print("✗ Public API surface differs from baseline:", file=sys.stderr)
    print("", file=sys.stderr)
    for line in diffs:
        print(f"  {line}", file=sys.stderr)
    print("", file=sys.stderr)
    print(
        "If this change is intentional and policy-compliant, regenerate the\n"
        "baseline and commit the diff so reviewers can audit the surface change:\n"
        "\n"
        "    python scripts/check_public_api.py --update\n"
        "    git add api-baseline.json && git commit\n"
        "\n"
        "For the 1.x line, any non-additive surface change requires a 2.0 major\n"
        "version bump per RELEASE_POLICY.md.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
