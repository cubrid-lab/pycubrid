"""Fail when declared pins, hook revisions, installed tools, or lint scopes drift.

The narrow extraction matches this repository's dev dependency/hook layout and
works on Python 3.10 without a TOML/YAML dependency. pyproject owns tool versions;
Makefile LINT_PATHS owns scope and CI invokes the shared Make targets.
"""

from __future__ import annotations

import re
from importlib import metadata
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
REPOSITORIES = {
    "ruff": "https://github.com/astral-sh/ruff-pre-commit",
    "mypy": "https://github.com/pre-commit/mirrors-mypy",
}
REQUIRED_PATHS = {"pycubrid", "tests", "scripts", "demos"}


def declared_pins(root: Path) -> dict[str, str]:
    content = (root / "pyproject.toml").read_text()
    blocks = re.findall(
        r"^\[project\.optional-dependencies\]\s*\n(.*?)(?=^\[|\Z)",
        content,
        re.MULTILINE | re.DOTALL,
    )
    if len(blocks) != 1:
        raise ValueError("exactly one project.optional-dependencies block is required")
    dev = re.findall(r"^dev\s*=\s*\[(.*?)^\]\s*$", blocks[0], re.MULTILINE | re.DOTALL)
    if len(dev) != 1:
        raise ValueError("exactly one dev dependency list is required")
    pins = {}
    for tool in REPOSITORIES:
        versions = re.findall(rf'^\s*["\']{tool}==([^"\']+)["\']\s*,?\s*$', dev[0], re.MULTILINE)
        if len(versions) != 1:
            raise ValueError(f"{tool}: exactly one exact dev pin is required")
        pins[tool] = versions[0]
    return pins


def check_configuration(root: Path = ROOT) -> dict[str, str]:
    pins = declared_pins(root)
    hooks = (root / ".pre-commit-config.yaml").read_text()
    blocks = re.findall(
        r"^  - repo: ([^\n]+)\n(.*?)(?=^  - repo:|\Z)", hooks, re.MULTILINE | re.DOTALL
    )
    for tool, repository in REPOSITORIES.items():
        bodies = [body for repo, body in blocks if repo.strip() == repository]
        if len(bodies) != 1:
            raise ValueError(f"{tool}: exactly one hook repository is required")
        revisions = re.findall(r"^    rev: v?(\S+)\s*$", bodies[0], re.MULTILINE)
        if revisions != [pins[tool]]:
            raise ValueError(
                f"{tool}: hook revision {revisions} does not match dev pin {pins[tool]}"
            )
        if tool == "ruff" and re.search(r"^\s+files:", bodies[0], re.MULTILINE):
            raise ValueError("Ruff hook files override the shared lint scope")
        if tool == "mypy" and '"pycubrid/"' not in bodies[0]:
            raise ValueError("mypy hook must explicitly check the pycubrid/ package")

    makefile = (root / "Makefile").read_text()
    values = re.findall(r"^LINT_PATHS\s*=\s*(.+)$", makefile, re.MULTILINE)
    if len(values) != 1:
        raise ValueError("exactly one Makefile LINT_PATHS scope is required")
    paths = set(values[0].split())
    if not REQUIRED_PATHS.issubset(paths):
        raise ValueError(f"lint scope omits maintained paths: {sorted(REQUIRED_PATHS - paths)}")
    hook_scopes = re.findall(r'^files: "\^\(([^()]+)\)/"\s*$', hooks, re.MULTILINE)
    if len(hook_scopes) != 1 or set(hook_scopes[0].split("|")) != paths:
        raise ValueError("pre-commit files scope does not match Makefile LINT_PATHS")
    for workflow in ("ci.yml", "maintenance.yml"):
        content = (root / ".github" / "workflows" / workflow).read_text()
        if not re.search(r"^\s+run: make lint\s*$", content, re.MULTILINE):
            raise ValueError(f"{workflow}: CI must invoke shared make lint scope")
        if re.search(r"^\s+(?:run:\s*)?ruff (?:check|format)\b", content, re.MULTILINE):
            raise ValueError(f"{workflow}: independent Ruff scope can drift from Makefile")
    for command in ("check", "format --check", "check --fix", "format"):
        if not re.search(
            rf"^\t\$\(RUFF\) {re.escape(command)} \$\(LINT_PATHS\)\s*$", makefile, re.MULTILINE
        ):
            raise ValueError(f"Makefile Ruff {command} does not use shared lint scope")
    return pins


def check_environment(
    pins: dict[str, str],
    installed: Callable[[str], str] | None = None,
) -> None:
    version = installed or metadata.version
    for tool, expected in pins.items():
        actual = version(tool)
        if actual != expected:
            raise ValueError(
                f"{tool}: installed {actual}, expected {expected}; install .[dev] in the active environment"
            )


def main() -> int:
    try:
        pins = check_configuration()
        check_environment(pins)
    except (ValueError, OSError, metadata.PackageNotFoundError) as exc:
        print(f"Quality-tool consistency failed: {exc}")
        return 1
    print("Quality-tool pins, active environment, hooks, and maintained lint scope are consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
