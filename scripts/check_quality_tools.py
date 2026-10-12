"""Fail when declared pins, local hooks, installed tools, or lint scopes drift.

`pyproject.toml` is parsed with the standard-library `tomllib`, so any valid TOML
formatting of the `lint` dependency group and the `[tool.ruff]` include list is
accepted; the pre-commit YAML, Makefile and workflows are still matched narrowly
because the standard library has no YAML parser. The pyproject `lint`
dependency group owns tool versions;
Ruff/Mypy pre-commit hooks run as `repo: local` / `language: system` entries that
invoke `python3 -m <tool>` against the same active environment, so there is no
separate hook revision to keep in sync with the pyproject pin. Makefile
LINT_PATHS owns scope and CI invokes the shared Make targets.
"""

from __future__ import annotations

import re
import tomllib
from importlib import metadata
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
LOCAL_HOOK_IDS = {
    "ruff": {"ruff", "ruff-format"},
    "mypy": {"mypy"},
}
# Exact required `entry:` per local hook id. Local hook ids carry no inherent
# behavior (unlike a pinned remote hook manifest), so the full command --
# not just the `python3 -m <tool>` prefix -- must be pinned, or a hook could
# drift to the wrong Ruff subcommand while still reporting as configured.
EXPECTED_HOOK_ENTRY = {
    "ruff-format": "python3 -m ruff format",
    "ruff": "python3 -m ruff check --fix",
    "mypy": "python3 -m mypy",
}
REQUIRED_PATHS = {"pycubrid", "tests", "scripts", "demos", "examples"}


# An exact pin is the bare tool name, `==` and one version; extras, markers,
# ranges, wildcards and arbitrary equality (===) are rejected (PEP 508 allows spaces around `==`).
EXACT_PIN = re.compile(
    r"^\s*(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)\s*==\s*(?P<version>[0-9][A-Za-z0-9.+!_-]*)\s*$"
)
REQUIREMENT_NAME = re.compile(r"^\s*(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)")


def _normalized(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _pyproject(root: Path) -> dict[str, Any]:
    try:
        return tomllib.loads((root / "pyproject.toml").read_text())
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"pyproject.toml is not valid TOML: {exc}") from exc


def _table(data: dict[str, Any], *keys: str) -> Any:
    value: Any = data
    for depth, key in enumerate(keys):
        if not isinstance(value, dict) or key not in value:
            name = ".".join(keys[: depth + 1])
            raise ValueError(f"pyproject.toml: `{name}` is required")
        value = value[key]
    return value


def _group_requirements(
    groups: dict[str, tuple[str, Any]], name: str, chain: tuple[str, ...] = ()
) -> list[str]:
    """Requirement strings of a PEP 735 group, with `include-group` entries expanded.

    `groups` maps each normalized group name to its declared name and value.
    """
    if _normalized(name) in map(_normalized, chain):
        raise ValueError(
            f"pyproject.toml: dependency group include cycle: {' -> '.join((*chain, name))}"
        )
    if _normalized(name) not in groups:
        raise ValueError(f"pyproject.toml: `dependency-groups.{name}` is required")
    name, group = groups[_normalized(name)]
    if not isinstance(group, list):
        raise ValueError(f"pyproject.toml: `dependency-groups.{name}` must be an array")
    requirements = []
    for entry in group:
        if isinstance(entry, str):
            requirements.append(entry)
        elif (
            isinstance(entry, dict)
            and set(entry) == {"include-group"}
            and isinstance(entry["include-group"], str)
        ):
            requirements += _group_requirements(groups, entry["include-group"], (*chain, name))
        else:
            raise ValueError(
                f"pyproject.toml: `dependency-groups.{name}` entries must be requirement strings "
                f"or include-group tables, got {entry!r}"
            )
    return requirements


def declared_pins(root: Path) -> dict[str, str]:
    declared = _table(_pyproject(root), "dependency-groups")
    if not isinstance(declared, dict):
        raise ValueError("pyproject.toml: `dependency-groups` must be a table")
    # PEP 735 compares group names after PEP 503 normalization.
    groups: dict[str, tuple[str, Any]] = {}
    for name, value in declared.items():
        if (key := _normalized(name)) in groups:
            raise ValueError(
                f"pyproject.toml: dependency groups `{groups[key][0]}` and `{name}` have the "
                "same normalized name"
            )
        groups[key] = (name, value)
    # The effective lint group, as PEP 735 installs it: included groups count too.
    requirements = _group_requirements(groups, "lint")
    pins = {}
    for tool in LOCAL_HOOK_IDS:
        entries = [
            requirement
            for requirement in requirements
            if (match := REQUIREMENT_NAME.match(requirement)) and _normalized(match["name"]) == tool
        ]
        exact = EXACT_PIN.match(entries[0]) if len(entries) == 1 else None
        if exact is None:
            raise ValueError(f"{tool}: exactly one exact lint group pin is required")
        pins[tool] = exact["version"]
    return pins


def check_configuration(root: Path = ROOT) -> dict[str, str]:
    pins = declared_pins(root)
    include = _table(_pyproject(root), "tool", "ruff", "include")
    if not isinstance(include, list) or sorted(map(str, include)) != ["*.py", "*.pyi"]:
        raise ValueError(
            "Ruff CLI discovery must be explicitly Python/pyi-only: `tool.ruff.include` must "
            f'be exactly ["*.py", "*.pyi"], got {include!r}'
        )
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
    hook_ids = [hook_id for hook_id, _ in hook_blocks]
    duplicates = {hook_id for hook_id in hook_ids if hook_ids.count(hook_id) > 1}
    if duplicates:
        raise ValueError(f"duplicate local hook id(s): {sorted(duplicates)}")
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
                    f"{hook_id}: hook must run via language: system against the active dev group "
                    f"environment, matching the pyproject {tool} pin"
                )
            expected_entry = EXPECTED_HOOK_ENTRY[hook_id]
            if not re.search(
                rf"^        entry: {re.escape(expected_entry)}\s*$", body, re.MULTILINE
            ):
                raise ValueError(
                    f"{hook_id}: hook entry must be exactly `{expected_entry}` so it always runs "
                    f"the pyproject-pinned, actively-installed {tool} (no separate hook revision "
                    "to drift, and no silently-different subcommand)"
                )
        if tool == "ruff":
            for hook_id in present_ids:
                body = hooks_by_id[hook_id]
                if re.search(r"^        files:", body, re.MULTILINE):
                    raise ValueError("Ruff hook files override the shared lint scope")
                scopes = re.findall(r"^        types_or: \[([^\]]+)\]\s*$", body, re.MULTILINE)
                if len(scopes) != 1 or set(scopes[0].split(", ")) != {"python", "pyi"}:
                    raise ValueError(f"{hook_id}: Ruff hook types must be exactly python and pyi")
                if re.search(r"^        args:", body, re.MULTILINE):
                    raise ValueError(
                        f"{hook_id}: Ruff hook must not add args (pre-commit appends them to "
                        "entry, e.g. `--check` on ruff-format silently disables formatting); "
                        "the full command already lives in entry"
                    )
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
                f"{tool}: installed {actual}, expected {expected}; install the dev dependency group in the active environment"
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
