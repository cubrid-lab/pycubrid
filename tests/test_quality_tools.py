"""Deliberate pin/environment/scope drift must fail the shared quality gate."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

from scripts.check_quality_tools import ROOT, check_configuration, check_environment, declared_pins

pytestmark = pytest.mark.repo_tooling


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


def test_hook_entry_drift_fails_and_restored_fixture_passes(project: Path) -> None:
    path = project / ".pre-commit-config.yaml"
    original = path.read_text()
    path.write_text(original.replace("entry: python3 -m ruff format", "entry: ruff format"))
    with pytest.raises(ValueError, match="entry must be exactly"):
        check_configuration(project)
    path.write_text(original)
    check_configuration(project)


def test_hook_swapped_subcommand_fails(project: Path) -> None:
    """A hook entry that still invokes `python3 -m ruff` but with the wrong
    subcommand (e.g. the format hook running `check` instead) must fail even
    though the module prefix looks right."""
    path = project / ".pre-commit-config.yaml"
    original = path.read_text()
    path.write_text(
        original.replace("entry: python3 -m ruff format", "entry: python3 -m ruff check")
    )
    with pytest.raises(ValueError, match="entry must be exactly"):
        check_configuration(project)
    path.write_text(original)
    check_configuration(project)


def test_duplicate_local_hook_id_fails(project: Path) -> None:
    path = project / ".pre-commit-config.yaml"
    original = path.read_text()
    anchor = "      - id: ruff\n        name: ruff\n"
    duplicate_ruff_hook = (
        "      - id: ruff\n"
        "        name: ruff (unpinned duplicate)\n"
        "        entry: ruff check\n"
        "        language: system\n"
        "        types_or: [python, pyi]\n"
    )
    assert original.count(anchor) == 1
    path.write_text(original.replace(anchor, duplicate_ruff_hook + anchor, 1))
    with pytest.raises(ValueError, match="duplicate local hook id"):
        check_configuration(project)
    path.write_text(original)
    check_configuration(project)


def test_ruff_hook_args_fails(project: Path) -> None:
    """pre-commit appends `args:` to `entry:`, so an added `args: ["--check"]`
    on ruff-format would silently turn formatting into a no-op check while
    the exact-entry check alone still sees the pinned entry line."""
    path = project / ".pre-commit-config.yaml"
    original = path.read_text()
    path.write_text(
        original.replace(
            "        entry: python3 -m ruff format\n        language: system\n",
            '        entry: python3 -m ruff format\n        language: system\n        args: ["--check"]\n',
        )
    )
    with pytest.raises(ValueError, match="must not add args"):
        check_configuration(project)
    path.write_text(original)
    check_configuration(project)


def test_hook_missing_language_system_fails(project: Path) -> None:
    path = project / ".pre-commit-config.yaml"
    original = path.read_text()
    path.write_text(
        original.replace(
            "        entry: python3 -m mypy\n        language: system\n",
            "        entry: python3 -m mypy\n",
        )
    )
    with pytest.raises(ValueError, match="language: system"):
        check_configuration(project)
    path.write_text(original)
    check_configuration(project)


def test_dependabot_style_pin_bump_alone_does_not_require_hook_edit(project: Path) -> None:
    """The whole point of the local/system hooks: bumping only the pyproject pin
    (what Dependabot's pip ecosystem does) must not require also touching
    .pre-commit-config.yaml, since there is no separate hook revision to sync."""
    path = project / "pyproject.toml"
    original = path.read_text()
    pin = declared_pins(project)["ruff"]
    path.write_text(original.replace(f'"ruff=={pin}"', '"ruff==99.0.0"'))
    check_configuration(project)
    path.write_text(original)


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
def test_missing_or_ambiguous_lint_group_pin_fails(project: Path, duplicate: bool) -> None:
    path = project / "pyproject.toml"
    pin = declared_pins(project)["ruff"]
    item = f'    "ruff=={pin}",'
    replacement = item + "\n" + item if duplicate else item.replace("==", ">=")
    path.write_text(path.read_text().replace(item, replacement))
    with pytest.raises(ValueError, match="exactly one exact lint group pin"):
        check_configuration(project)


def test_precommit_scope_cannot_omit_demos(project: Path) -> None:
    path = project / ".pre-commit-config.yaml"
    path.write_text(path.read_text().replace("|demos", ""))
    with pytest.raises(ValueError, match="files scope"):
        check_configuration(project)


def test_ci_cannot_replace_shared_scope_with_package_only_lint(project: Path) -> None:
    path = project / ".github" / "workflows" / "ci.yml"
    path.write_text(path.read_text().replace("run: make check", "run: ruff check pycubrid/ tests/"))
    with pytest.raises(ValueError, match="shared make check"):
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
    path.write_text(
        path.read_text().replace(
            "entry: python3 scripts/check_quality_tools.py",
            "entry: python scripts/check_quality_tools.py",
        )
    )
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


def _replace_lint_group(project: Path, replacement: str) -> None:
    path = project / "pyproject.toml"
    original = path.read_text()
    start = original.index("lint = [")
    end = original.index("]\n", start) + 2
    path.write_text(original[:start] + replacement + original[end:])


def test_equivalent_toml_formatting_is_accepted(project: Path) -> None:
    """tomllib reads the configuration structurally, so other valid TOML layouts
    of the same policy (one-line arrays, single quotes, spaces around `==`, a
    reordered include list) are accepted with the same pins."""
    pins = declared_pins(project)
    _replace_lint_group(
        project,
        f"lint = ['ruff == {pins['ruff']}', \"mypy=={pins['mypy']}\", 'pre-commit']\n",
    )
    path = project / "pyproject.toml"
    path.write_text(
        path.read_text().replace(
            'include = ["*.py", "*.pyi"]', "include = [\n    '*.pyi',\n    '*.py',\n]"
        )
    )
    assert check_configuration(project) == pins


@pytest.mark.parametrize(
    "lint",
    [
        'lint = ["ruff[all]==0.16.10", "mypy==2.4.0"]\n',
        'lint = ["ruff==0.16.*", "mypy==2.4.0"]\n',
        'lint = ["ruff===0.16.10", "mypy==2.4.0"]\n',
        'lint = ["ruff==0.16.10; python_version >= \'3.11\'", "mypy==2.4.0"]\n',
        'lint = ["ruff==0.16.10", "Ruff==0.16.11", "mypy==2.4.0"]\n',
        'lint = ["mypy==2.4.0"]\n',
    ],
    ids=[
        "extras",
        "wildcard",
        "arbitrary-equality",
        "marker",
        "duplicate-normalized-name",
        "missing",
    ],
)
def test_non_exact_or_ambiguous_toml_pin_fails(project: Path, lint: str) -> None:
    _replace_lint_group(project, lint)
    with pytest.raises(ValueError, match="ruff: exactly one exact lint group pin"):
        declared_pins(project)


def test_malformed_toml_reports_a_validation_error(project: Path) -> None:
    path = project / "pyproject.toml"
    path.write_text(path.read_text() + "\n[tool.ruff\n")
    with pytest.raises(ValueError, match="pyproject.toml is not valid TOML"):
        check_configuration(project)


@pytest.mark.parametrize(
    ("edit", "message"),
    [
        (("[dependency-groups]", "[unrelated-groups]"), "`dependency-groups` is required"),
        (("lint = [", "lint-tools = ["), "`dependency-groups.lint` is required"),
        (('include = ["*.py", "*.pyi"]', 'extend-include = ["*.py"]'), "`tool.ruff.include`"),
    ],
    ids=["no-dependency-groups", "no-lint-group", "no-ruff-include"],
)
def test_missing_toml_keys_report_the_key(
    project: Path, edit: tuple[str, str], message: str
) -> None:
    path = project / "pyproject.toml"
    original = path.read_text()
    assert original.count(edit[0]) == 1
    path.write_text(original.replace(*edit))
    with pytest.raises(ValueError, match=re.escape(message)):
        check_configuration(project)


@pytest.mark.parametrize(
    ("lint", "message"),
    [
        ('lint = "ruff==0.16.10"\n', "`dependency-groups.lint` must be an array"),
        (
            'lint = ["ruff==0.16.10", "mypy==2.4.0", 42]\n',
            "entries must be requirement strings or include-group tables, got 42",
        ),
    ],
    ids=["not-an-array", "entry-not-a-string"],
)
def test_wrongly_typed_lint_group_fails(project: Path, lint: str, message: str) -> None:
    _replace_lint_group(project, lint)
    with pytest.raises(ValueError, match=re.escape(message)):
        check_configuration(project)


def test_wrongly_typed_ruff_include_fails(project: Path) -> None:
    path = project / "pyproject.toml"
    path.write_text(path.read_text().replace('include = ["*.py", "*.pyi"]', 'include = "*.py"'))
    with pytest.raises(ValueError, match="explicitly Python/pyi-only"):
        check_configuration(project)


def test_include_group_entries_in_the_lint_group_are_allowed(project: Path) -> None:
    pins = declared_pins(project)
    _replace_lint_group(
        project,
        f'lint = [{{include-group = "test"}}, "ruff=={pins["ruff"]}", "mypy=={pins["mypy"]}"]\n',
    )
    assert declared_pins(project) == pins


def test_a_pin_from_an_included_group_counts(project: Path) -> None:
    """PEP 735 installs included groups as part of `lint`, so a pin may live there."""
    pins = declared_pins(project)
    _replace_lint_group(
        project,
        f'lint = [{{include-group = "ruff-pin"}}, "mypy=={pins["mypy"]}"]\n'
        f'ruff-pin = ["ruff=={pins["ruff"]}"]\n',
    )
    assert declared_pins(project) == pins


def test_group_names_are_compared_after_normalization(project: Path) -> None:
    pins = declared_pins(project)
    _replace_lint_group(
        project,
        f'Lint = [{{include-group = "RUFF_PIN"}}, "mypy=={pins["mypy"]}"]\n'
        f'ruff-pin = ["ruff=={pins["ruff"]}"]\n',
    )
    assert declared_pins(project) == pins


def test_group_names_that_normalize_alike_are_rejected(project: Path) -> None:
    pins = declared_pins(project)
    _replace_lint_group(
        project,
        f'lint = ["ruff=={pins["ruff"]}", "mypy=={pins["mypy"]}"]\nLINT = ["ruff>=0.1"]\n',
    )
    with pytest.raises(ValueError, match="`lint` and `LINT` have the same normalized name"):
        declared_pins(project)


@pytest.mark.parametrize(
    "included",
    ['["ruff>=0.16"]', '["ruff==0.16.11"]', '[{include-group = "test"}, "Ruff>=0.1"]'],
    ids=["ranged", "duplicate", "nested-ranged"],
)
def test_included_groups_cannot_bypass_the_exact_pin_rule(project: Path, included: str) -> None:
    pins = declared_pins(project)
    _replace_lint_group(
        project,
        f'lint = ["ruff=={pins["ruff"]}", "mypy=={pins["mypy"]}", {{include-group = "extra"}}]\n'
        f"extra = {included}\n",
    )
    with pytest.raises(ValueError, match="ruff: exactly one exact lint group pin"):
        declared_pins(project)


@pytest.mark.parametrize(
    ("lint", "message"),
    [
        ('lint = [{include-group = "absent"}]\n', "`dependency-groups.absent` is required"),
        (
            'lint = [{include-group = "loop"}]\nloop = [{include-group = "Lint"}]\n',
            "include cycle: lint -> loop -> Lint",
        ),
        ("lint = [{include-group = 1}]\n", "entries must be requirement strings"),
        ('lint = [{include-group = "test", extra = 1}]\n', "entries must be requirement strings"),
    ],
    ids=["missing-group", "cycle", "name-not-a-string", "extra-key"],
)
def test_invalid_include_group_references_fail(project: Path, lint: str, message: str) -> None:
    _replace_lint_group(project, lint)
    with pytest.raises(ValueError, match=re.escape(message)):
        declared_pins(project)


def _python_source_files(root: Path) -> list[str]:
    if (root / ".git").exists():
        try:
            return subprocess.check_output(
                ["git", "ls-files", "--", "*.py", "*.pyi"],
                cwd=root,
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=5,
            ).splitlines()
        except (OSError, subprocess.SubprocessError):
            # Source inventory remains useful when Git cannot be invoked.
            root = root.resolve()

    generated = {
        ".git",
        ".venv",
        "venv",
        ".tox",
        ".nox",
        "build",
        "dist",
        "htmlcov",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".hypothesis",
        ".omx",
    }
    sources = []
    for directory, dirs, files in os.walk(root):
        current = Path(directory)
        dirs[:] = [
            name
            for name in dirs
            if name not in generated
            and not name.endswith(".egg-info")
            and not (current / name / "pyvenv.cfg").is_file()
        ]
        sources.extend(
            (current / name).relative_to(root).as_posix()
            for name in files
            if Path(name).suffix in {".py", ".pyi"}
        )
    return sorted(sources)


def test_shared_scope_covers_every_source_python_file() -> None:
    match = re.search(r"^LINT_PATHS = (.+)$", (ROOT / "Makefile").read_text(), re.MULTILINE)
    assert match is not None
    scope = set(match.group(1).split())
    tracked = _python_source_files(ROOT)
    assert tracked
    uncovered = [name for name in tracked if Path(name).parts[0] not in scope]
    assert not uncovered, f"tracked Python files lost from the shared hook/CLI scope: {uncovered}"


def test_source_archive_inventory_prunes_generated_and_environment_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    expected = {
        "examples/example.py",
        "pycubrid/__init__.py",
        "scripts/tool.pyi",
        ".github/check.py",
    }
    ignored = {
        ".venv/lib/dependency.py",
        "build/generated.py",
        "dist/generated.py",
        "__pycache__/cached.py",
        ".pytest_cache/cached.py",
        "package.egg-info/generated.py",
        "custom-environment/lib/dependency.py",
    }
    for name in expected | ignored:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("value = 1\n")
    (tmp_path / "custom-environment" / "pyvenv.cfg").write_text("home = /unused\n")

    def unavailable_git(*args: object, **kwargs: object) -> str:
        raise AssertionError("archive without .git must not depend on Git")

    monkeypatch.setattr(subprocess, "check_output", unavailable_git)
    assert not (tmp_path / ".git").exists()
    assert set(_python_source_files(tmp_path)) == expected
