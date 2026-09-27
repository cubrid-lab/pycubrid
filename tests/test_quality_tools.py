"""Deliberate pin/environment/scope drift must fail the shared quality gate."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess

import pytest

from scripts.check_quality_tools import ROOT, check_configuration, check_environment, declared_pins


@pytest.fixture
def project(tmp_path: Path) -> Path:
    for name in (
        "pyproject.toml",
        ".pre-commit-config.yaml",
        "Makefile",
        ".github/workflows/ci.yml",
        ".github/workflows/maintenance.yml",
    ):
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text((ROOT / name).read_text())
    return tmp_path


def test_declared_hooks_and_active_environment_agree() -> None:
    check_environment(check_configuration())


def test_wrong_hook_revision_fails_and_restored_fixture_passes(project: Path) -> None:
    path = project / ".pre-commit-config.yaml"
    original = path.read_text()
    pin = declared_pins(project)["ruff"]
    path.write_text(original.replace(f"rev: v{pin}", "rev: v0.0.0"))
    with pytest.raises(ValueError, match="hook revision"):
        check_configuration(project)
    path.write_text(original)
    check_configuration(project)


def test_wrong_installed_version_fails(project: Path) -> None:
    pins = check_configuration(project)
    with pytest.raises(ValueError, match="installed 0.0.0"):
        check_environment(pins, lambda tool: "0.0.0" if tool == "ruff" else pins[tool])


@pytest.mark.parametrize("directory", ["scripts", "demos", "examples"])
def test_removed_maintained_scope_fails(project: Path, directory: str) -> None:
    path = project / "Makefile"
    path.write_text(path.read_text().replace(f" {directory}", "", 1))
    with pytest.raises(ValueError, match="lint scope omits"):
        check_configuration(project)


@pytest.mark.parametrize("duplicate", [False, True])
def test_missing_or_ambiguous_dev_pin_fails(project: Path, duplicate: bool) -> None:
    path = project / "pyproject.toml"
    pin = declared_pins(project)["ruff"]
    item = f'    "ruff=={pin}",'
    replacement = item + "\n" + item if duplicate else item.replace("==", ">=")
    path.write_text(path.read_text().replace(item, replacement))
    with pytest.raises(ValueError, match="exactly one exact dev pin"):
        check_configuration(project)


def test_precommit_scope_cannot_omit_demos(project: Path) -> None:
    path = project / ".pre-commit-config.yaml"
    path.write_text(path.read_text().replace("|demos", ""))
    with pytest.raises(ValueError, match="files scope"):
        check_configuration(project)


def test_ci_cannot_replace_shared_scope_with_package_only_lint(project: Path) -> None:
    path = project / ".github" / "workflows" / "ci.yml"
    path.write_text(path.read_text().replace("run: make lint", "run: ruff check pycubrid/ tests/"))
    with pytest.raises(ValueError, match="shared make lint"):
        check_configuration(project)


def test_ruff_hook_cannot_narrow_the_shared_global_scope(project: Path) -> None:
    path = project / ".pre-commit-config.yaml"
    path.write_text(
        path.read_text().replace(
            "- id: ruff-format", '- id: ruff-format\n        files: "^pycubrid/"'
        )
    )
    with pytest.raises(ValueError, match="override the shared lint scope"):
        check_configuration(project)


def test_mypy_hook_cannot_omit_the_package_target(project: Path) -> None:
    path = project / ".pre-commit-config.yaml"
    path.write_text(path.read_text().replace('"pycubrid/"', '"another_package/"'))
    with pytest.raises(ValueError, match="explicitly check"):
        check_configuration(project)


@pytest.mark.parametrize("types", ["types_or: [python, pyi, markdown]", "types_or: [python]"])
def test_ruff_hook_type_drift_is_rejected(project: Path, types: str) -> None:
    path = project / ".pre-commit-config.yaml"
    path.write_text(path.read_text().replace("types_or: [python, pyi]", types, 1))
    with pytest.raises(ValueError, match="types must be exactly"):
        check_configuration(project)


def test_system_guard_requires_python3(project: Path) -> None:
    path = project / ".pre-commit-config.yaml"
    path.write_text(path.read_text().replace("entry: python3", "entry: python"))
    with pytest.raises(ValueError, match="must use python3"):
        check_configuration(project)


@pytest.mark.parametrize("include", ['include = ["*.py", "*.pyi", "*.md"]', 'include = ["*.py"]'])
def test_cli_file_discovery_cannot_expand_to_markdown_or_drop_pyi(
    project: Path, include: str
) -> None:
    path = project / "pyproject.toml"
    path.write_text(path.read_text().replace('include = ["*.py", "*.pyi"]', include))
    with pytest.raises(ValueError, match="explicitly Python/pyi-only"):
        check_configuration(project)


def test_shared_scope_covers_every_tracked_python_file() -> None:
    match = re.search(r"^LINT_PATHS = (.+)$", (ROOT / "Makefile").read_text(), re.MULTILINE)
    assert match is not None
    scope = set(match.group(1).split())
    tracked = subprocess.check_output(
        ["git", "ls-files", "--", "*.py", "*.pyi"],
        cwd=ROOT,
        text=True,
    ).splitlines()
    assert tracked
    uncovered = [name for name in tracked if Path(name).parts[0] not in scope]
    assert not uncovered, f"tracked Python files lost from the shared hook/CLI scope: {uncovered}"
