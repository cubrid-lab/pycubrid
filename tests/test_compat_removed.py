"""``pycubrid.compat`` is an error-only tombstone with migration guidance (#1130)."""

from __future__ import annotations

import importlib
import inspect
import subprocess
import sys
from pathlib import Path

import pytest

import pycubrid

GUIDE = "https://cubrid-lab.github.io/pycubrid/COMPAT_MIGRATION/"


@pytest.mark.parametrize(
    "statement",
    [
        "import pycubrid.compat",
        "from pycubrid import compat",
        "from pycubrid.compat import native",
        "from pycubrid.compat import cubriddb",
        "import pycubrid.compat.native",
        "import pycubrid.compat.cubriddb",
        "import pycubrid.compat.cursors",
    ],
)
def test_every_import_form_fails_with_the_migration_guide(statement: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", statement], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 1
    assert "ImportError" in result.stderr
    assert GUIDE in result.stderr


def test_the_url_points_at_a_published_guide() -> None:
    root = Path(__file__).resolve().parent.parent
    assert (root / "docs" / "COMPAT_MIGRATION.md").is_file()
    assert "COMPAT_MIGRATION.md" in (root / "mkdocs.yml").read_text(encoding="utf-8")


def test_package_import_raises_in_process() -> None:
    sys.modules.pop("pycubrid.compat", None)
    with pytest.raises(ImportError, match="pycubrid.compat was removed"):
        importlib.import_module("pycubrid.compat")
    assert "pycubrid.compat" not in sys.modules


def test_ordinary_packages_import_without_touching_the_tombstone() -> None:
    code = (
        "import sys, pycubrid, pycubrid.aio;"
        "assert not any(name.startswith('pycubrid.compat') for name in sys.modules)"
    )
    result = subprocess.run(
        [sys.executable, "-W", "error", "-c", code], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr


def test_ordinary_contract_is_unchanged() -> None:
    assert pycubrid.threadsafety == 1
    assert inspect.signature(pycubrid.connect).parameters["user"].default == "dba"


@pytest.mark.repo_tooling
def test_public_api_gate_no_longer_tracks_compat() -> None:
    from scripts import check_public_api

    assert not any(name.startswith("pycubrid.compat") for name in check_public_api.TRACKED_MODULES)
    assert not any(
        module.startswith("pycubrid.compat") for module, _ in check_public_api.TRACKED_CLASSES
    )
