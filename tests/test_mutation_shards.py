"""The weekly mutation shards cover every mutated module exactly once (#750).

``bug-hunt.yml`` splits ``mutmut run`` into matrix shards by mutant name. mutmut 3
matches each name with ``fnmatch`` against ``<module>.<mangled function>``, so a
module missing from every shard is silently never tested, and a module in two
shards is tested twice.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.repo_tooling

JOB = yaml.safe_load((ROOT / ".github/workflows/bug-hunt.yml").read_text())["jobs"]["mutation"]
SHARDS = {
    entry["shard"]: entry["mutants"].split() for entry in JOB["strategy"]["matrix"]["include"]
}
MUTMUT = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["mutmut"]
ONLY_MUTATE = MUTMUT["only_mutate"]


def _module(path: str) -> str:
    return path.removesuffix(".py").replace("/", ".")


def test_every_mutated_module_is_in_exactly_one_shard() -> None:
    # Each pattern must be a whole module (<module>.*): a narrower pattern, or an
    # exact mutant name, would still match a sample but skip the rest of the module.
    patterns = [pattern for shard in SHARDS.values() for pattern in shard]
    assert sorted(patterns) == sorted(f"{_module(path)}.*" for path in ONLY_MUTATE)


def test_shard_names_reach_mutmut_unexpanded() -> None:
    (run,) = [s["run"] for s in JOB["steps"] if s.get("name") == "Run mutation testing"]
    lines = [line.strip() for line in run.splitlines()]
    assert lines.index("set -f") < lines.index("mutmut run $MUTANTS")
    (step,) = [s for s in JOB["steps"] if s.get("name") == "Run mutation testing"]
    assert step["env"] == {"MUTANTS": "${{ matrix.mutants }}"}
    assert JOB["strategy"]["fail-fast"] is False


def test_each_shard_uploads_its_own_cache() -> None:
    (upload,) = [s for s in JOB["steps"] if s.get("name") == "Upload mutation cache"]
    assert upload["with"]["name"] == "mutation-cache-${{ matrix.shard }}"


def test_every_also_copy_entry_exists() -> None:
    # mutmut copies these into mutants/ for the stats run; a renamed or deleted
    # path would make the sandbox differ from the repository again (#1107).
    missing = [entry for entry in MUTMUT["also_copy"] if not (ROOT / entry).exists()]
    assert missing == []


def test_every_deselected_mutation_test_still_exists() -> None:
    # A deselect that no longer names a test silently stops excluding anything;
    # the trampoline-incompatible test would then fail the stats run again (#1107).
    args = MUTMUT["pytest_add_cli_args"]
    targets = [args[i + 1] for i, arg in enumerate(args) if arg == "--deselect"]
    assert targets
    for target in targets:
        path, *names = target.split("::")
        source = (ROOT / path).read_text()
        for name in names:
            keyword = "class" if name[0].isupper() else "def"
            assert f"{keyword} {name}" in source, target
