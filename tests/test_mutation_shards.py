"""The weekly mutation shards cover every mutated module exactly once (#750).

``bug-hunt.yml`` splits ``mutmut run`` into matrix shards by mutant name. mutmut 3
matches each name with ``fnmatch`` against ``<module>.<mangled function>``, so a
module missing from every shard is silently never tested, and a module in two
shards is tested twice.
"""

from __future__ import annotations

import fnmatch
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
ONLY_MUTATE = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["mutmut"]["only_mutate"]


def _module(path: str) -> str:
    return path.removesuffix(".py").replace("/", ".")


def _sample_mutants(module: str) -> list[str]:
    # Plain functions are named x_<name>, methods xǁ<Class>ǁ<name>.
    return [f"{module}.x_helper__mutmut_1", f"{module}.xǁConnectionǁclose__mutmut_1"]


def _shards_for(mutant: str) -> list[str]:
    return [
        shard
        for shard, patterns in SHARDS.items()
        if any(fnmatch.fnmatch(mutant, pattern) for pattern in patterns)
    ]


@pytest.mark.parametrize("path", ONLY_MUTATE)
def test_every_mutated_module_is_in_exactly_one_shard(path: str) -> None:
    for mutant in _sample_mutants(_module(path)):
        assert len(_shards_for(mutant)) == 1, (mutant, _shards_for(mutant))


def test_every_shard_pattern_selects_a_mutated_module() -> None:
    modules = [_module(path) for path in ONLY_MUTATE]
    for shard, patterns in SHARDS.items():
        for pattern in patterns:
            assert any(
                fnmatch.fnmatch(mutant, pattern) for m in modules for mutant in _sample_mutants(m)
            ), (shard, pattern)


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
