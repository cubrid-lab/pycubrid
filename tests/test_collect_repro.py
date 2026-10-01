"""Offline checks for the bug-hunt repro bundle (issues #359, #351)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import collect_repro

pytestmark = pytest.mark.repo_tooling

MATRIX = "10.2=localhost:33102,11.4=localhost:33114"


@pytest.fixture
def bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(collect_repro, "REPRO_DIR", tmp_path / "bug-hunt-repro")
    monkeypatch.setattr(collect_repro, "HYPOTHESIS_DB", tmp_path / ".hypothesis")
    return tmp_path / "bug-hunt-repro"


def test_version_matrix_is_recorded_in_metadata(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CUBRID_VERSION_MATRIX", MATRIX)
    assert collect_repro.main() == 0
    meta = json.loads((bundle / "metadata.json").read_text())
    assert meta["cubrid_version_matrix"] == MATRIX


def test_version_matrix_is_part_of_the_reproduce_command(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CUBRID_VERSION_MATRIX", MATRIX)
    collect_repro.main()
    assert f'CUBRID_VERSION_MATRIX="{MATRIX}"' in (bundle / "reproduce.md").read_text()


def test_reproduce_command_omits_an_unset_version_matrix(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CUBRID_VERSION_MATRIX", raising=False)
    collect_repro.main()
    assert "CUBRID_VERSION_MATRIX" not in (bundle / "reproduce.md").read_text()
