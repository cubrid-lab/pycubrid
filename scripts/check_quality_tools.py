"""Fail when declared pins, local hooks, installed tools, or lint scopes drift.

The narrow extraction matches this repository's dev dependency/hook layout and
works on Python 3.10 without a TOML/YAML dependency. pyproject owns tool versions;
Ruff/Mypy pre-commit hooks run as `repo: local` / `language: system` entries that
invoke `python3 -m <tool>` against the same active environment, so there is no
separate hook revision to keep in sync with the pyproject pin. Makefile
LINT_PATHS owns scope and CI invokes the shared Make targets.
"""

from __future__ import annotations

import re
from importlib import metadata
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
LOCAL_HOOK_IDS = {
    "ruff": {"ruff", "ruff-format"},
    "mypy": {"mypy"},
}
REQUIRED_PATHS = {"pycubrid", "tests", "scripts", "demos", "examples"}


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
    for tool in LOCAL_HOOK_IDS:
        versions = re.findall(rf'^\s*["\']{tool}==([^"\']+)["\']\s*,?\s*$', dev[0], re.MULTILINE)
        if len(versions) != 1:
            raise ValueError(f"{tool}: exactly one exact dev pin is required")
        pins[tool] = versions[0]
    return pins


def check_configuration(root: Path = ROOT) -> dict[str, str]:
    pins = declared_pins(root)
    config = (root / "pyproject.toml").read_text()
    ruff = re.findall(r"^\[tool\.ruff\]\s*\n(.*?)(?=^\[|\Z)", config, re.MULTILINE | re.DOTALL)
    if len(ruff) != 1 or not re.search(
        r'^include = \["\*\.py", "\*\.pyi"\]\s*$', ruff[0], re.MULTILINE
    ):
        raise ValueError("Ruff CLI discovery must be explicitly Python/pyi-only")
    hooks = (root / ".pre-commit-config.yaml").read_text()
    local_blocks = re.findall(
        r"^  - repo: local\s*\n(.*?)(?=^  - repo:|\Z)", hooks, re.MULTILINE | re.DOTALL
    )
    if len(local_blocks) != 1:
        raise ValueError("exactly one local hook repository is required")
    hook_blocks = re.findall(
        r"^      - id: (\S+)\n(.*?)(?=^      - id:|\Z)",
        local_blocks[0],
        re.MULTILINE | re.DOTALL,
    )
    hooks_by_id = dict(hook_blocks)
    for tool, expected_ids in LOCAL_HOOK_IDS.items():
        present_ids = sorted(expected_ids & hooks_by_id.keys())
        if tool == "ruff":
            if len(present_ids) != 2:
                raise ValueError("both Ruff check and format hooks are required")
        elif len(present_ids) != 1:
            raise ValueError(f"{tool}: exactly one local hook is required")
        for hook_id in present_ids:
            body = hooks_by_id[hook_id]
            if not re.search(r"^        language: system\s*$", body, re.MULTILINE):
                raise ValueError(
                    f"{hook_id}: hook must run via language: system against the active .[dev] "
                    f"environment, matching the pyproject {tool} pin"
                )
            if not re.search(rf"^        entry: python3 -m {tool}\b", body, re.MULTILINE):
                raise ValueError(
                    f"{hook_id}: hook entry must invoke `python3 -m {tool}` so it always runs the "
                    f"pyproject-pinned, actively-installed {tool} (no separate hook revision to drift)"
                )
        if tool == "ruff":
            for hook_id in present_ids:
                body = hooks_by_id[hook_id]
                if re.search(r"^        files:", body, re.MULTILINE):
                    raise ValueError("Ruff hook files override the shared lint scope")
                scopes = re.findall(r"^        types_or: \[([^\]]+)\]\s*$", body, re.MULTILINE)
                if len(scopes) != 1 or set(scopes[0].split(", ")) != {"python", "pyi"}:
                    raise ValueError(f"{hook_id}: Ruff hook types must be exactly python and pyi")
        if tool == "mypy" and '"pycubrid/"' not in hooks_by_id["mypy"]:
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
    if not re.search(
        r"^        entry: python3 scripts/check_quality_tools.py\s*$", hooks, re.MULTILINE
    ):
        raise ValueError("system guard must use python3 like the Makefile")
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
