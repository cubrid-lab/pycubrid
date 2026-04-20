#!/usr/bin/env python3
"""Verify version consistency between pyproject.toml and pycubrid/__init__.py.

Usage:
    python scripts/check_version.py

Exit codes:
    0 — versions match
    1 — mismatch or parse error
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path


def get_pyproject_version(root: Path) -> str:
    """Extract version from pyproject.toml."""
    content = (root / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
    if not match:
        print("ERROR: Could not find 'version' in pyproject.toml", file=sys.stderr)
        sys.exit(1)
    return match.group(1)


def get_init_version(root: Path) -> str:
    """Extract __version__ from pycubrid/__init__.py using AST."""
    init_path = root / "pycubrid" / "__init__.py"
    tree = ast.parse(init_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "__version__" for t in node.targets)
            and isinstance(node.value, ast.Constant)
        ):
            return str(node.value.value)
    print("ERROR: Could not find '__version__' in pycubrid/__init__.py", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    pyproject_ver = get_pyproject_version(root)
    init_ver = get_init_version(root)

    print(f"pyproject.toml: {pyproject_ver}")
    print(f"__init__.py:    {init_ver}")

    if pyproject_ver != init_ver:
        print(
            f"\nERROR: Version mismatch! pyproject.toml={pyproject_ver}, __init__.py={init_ver}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"\n✓ Versions match: {pyproject_ver}")


if __name__ == "__main__":
    main()
