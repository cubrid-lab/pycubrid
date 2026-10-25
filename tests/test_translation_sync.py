from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "_pycubrid_check_translation_sync", ROOT / "scripts" / "check_translation_sync.py"
)
assert _SPEC is not None and _SPEC.loader is not None
translation_sync = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(translation_sync)


def _translation_tree(tmp_path: Path, *languages: str) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    for language in languages:
        (docs / f"README.{language}.md").write_text(f"# {language}\n", encoding="utf-8")


def test_changed_files_collects_both_rename_endpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BASE_REF", "origin/release")

    def fake_run(
        command: list[str], *, capture_output: bool, text: bool, check: bool
    ) -> subprocess.CompletedProcess[str]:
        assert command == [
            "git",
            "diff",
            "--name-only",
            "--find-renames",
            "origin/release...HEAD",
        ]
        assert capture_output and text and check
        return subprocess.CompletedProcess(
            command,
            0,
            "README.md\ndocs/README.de.md\tdocs/README.ru.md\n",
        )

    monkeypatch.setattr(translation_sync.subprocess, "run", fake_run)

    assert translation_sync.changed_files() == {
        "README.md",
        "docs/README.de.md",
        "docs/README.ru.md",
    }


def test_unchanged_canonical_readme_is_in_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _translation_tree(tmp_path, "ko")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(translation_sync, "changed_files", lambda: {"pycubrid/cursor.py"})

    assert translation_sync.main() == 0
    assert "README.md unchanged in this PR" in capsys.readouterr().out


def test_changed_canonical_and_required_translation_are_in_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _translation_tree(tmp_path, "ko")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        translation_sync,
        "changed_files",
        lambda: {"README.md", "docs/README.ko.md"},
    )

    assert translation_sync.main() == 0
    assert capsys.readouterr().out == "translation sync OK\n"


def test_changed_canonical_without_required_translation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _translation_tree(tmp_path, "ko")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(translation_sync, "changed_files", lambda: {"README.md"})

    assert translation_sync.main() == 1
    output = capsys.readouterr().out
    assert "::error::README.md changed" in output
    assert "docs/README.ko.md (한국어)" in output


def test_community_translation_drift_warns_without_failing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _translation_tree(tmp_path, "ko", "de")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        translation_sync,
        "changed_files",
        lambda: {"README.md", "docs/README.ko.md"},
    )

    assert translation_sync.main() == 0
    output = capsys.readouterr().out
    assert (
        "::warning file=README.md::community translation drifted: docs/README.de.md (Deutsch)"
    ) in output
    assert output.endswith("translation sync OK\n")
