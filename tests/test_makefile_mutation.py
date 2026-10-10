"""``make mutation`` stops on Python 3.11 before running mutmut (#812).

mutmut 3.8 fails at stats collection on 3.11, so the target checks the
interpreter first. The tests run the real Makefile with a ``PYTHON`` stub that
reports a chosen version and a ``mutmut`` stub that records each call.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pytestmark = [
    pytest.mark.repo_tooling,
    pytest.mark.skipif(os.name != "posix", reason="command stubs need a POSIX shell"),
]


def _executable(path: Path, text: str) -> Path:
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _run(tmp_path: Path, version: tuple[int, int]) -> tuple[subprocess.CompletedProcess[str], str]:
    python = _executable(
        tmp_path / "python",
        "#!/bin/sh\n"
        f'exec "{sys.executable}" -c '
        f'"import sys; sys.version_info = {version!r}; exec(sys.argv[1])" "$2"\n',
    )
    log = tmp_path / "mutmut.log"
    _executable(tmp_path / "mutmut", f'#!/bin/sh\nprintf "%s\\n" "$*" >> "{log}"\n')
    env = {**os.environ, "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}"}
    result = subprocess.run(
        ["make", "-s", "-C", str(ROOT), "mutation", f"PYTHON={python}"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    return result, log.read_text() if log.exists() else ""


def test_mutation_stops_on_python_3_11_without_running_mutmut(tmp_path: Path) -> None:
    result, calls = _run(tmp_path, (3, 11))
    assert result.returncode != 0
    assert "make mutation needs Python 3.12+" in result.stderr
    assert calls == ""


def test_mutation_runs_mutmut_on_python_3_12(tmp_path: Path) -> None:
    result, calls = _run(tmp_path, (3, 12))
    assert result.returncode == 0, result.stderr
    assert calls.splitlines() == ["run", "results"]
