"""The offline agent-evaluation grader (#925)."""

from __future__ import annotations

import importlib.util
import json
import shutil
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.repo_tooling

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("agent_eval", ROOT / "scripts/agent_eval.py")
assert _SPEC is not None and _SPEC.loader is not None
agent_eval = importlib.util.module_from_spec(_SPEC)
sys.modules["agent_eval"] = agent_eval
_SPEC.loader.exec_module(agent_eval)

SELFTEST = ROOT / "agent-evals" / "selftest"
CANDIDATES = sorted(p.name for p in (SELFTEST / "candidates").iterdir() if p.is_dir())


def test_self_test_fixtures_grade_as_expected() -> None:
    assert agent_eval.self_test(SELFTEST) == []


def test_fixtures_cover_the_required_cases() -> None:
    # #925: a correct solution, a missing regression test, a misleading
    # validation claim and an unrelated-file change.
    for name in (
        "correct",
        "missing-regression-test",
        "misleading-validation-claim",
        "unrelated-file-change",
    ):
        assert name in CANDIDATES


@pytest.mark.parametrize("name", CANDIDATES)
def test_replaying_a_candidate_gives_identical_bytes(name: str) -> None:
    first = agent_eval.dumps(agent_eval.grade(SELFTEST, SELFTEST / "candidates" / name))
    second = agent_eval.dumps(agent_eval.grade(SELFTEST, SELFTEST / "candidates" / name))
    assert first == second


def test_cli_exit_codes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = tmp_path / "grade.json"
    good = str(SELFTEST / "candidates" / "correct")
    assert agent_eval.main(["grade", str(SELFTEST), good, "--output", str(out)]) == 0
    assert json.loads(out.read_text())["verdict"] == "pass"
    bad = str(SELFTEST / "candidates" / "unrelated-file-change")
    assert agent_eval.main(["grade", str(SELFTEST), bad]) == 1
    assert agent_eval.main(["grade", str(SELFTEST), str(tmp_path / "missing")]) == 2
    assert "patch.diff: not found" in capsys.readouterr().err
    assert agent_eval.main(["self-test"]) == 0


def _task(tmp_path: Path, text: str) -> Path:
    (tmp_path / "task.toml").write_text(text, encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize(
    ("replace", "message"),
    [
        ('base_sha = "0edf209c30456b6ff81457b0225d8d245576d237"', "full 40-character"),
        ('evidence = ["https://github.com/cubrid-lab/pycubrid/issues/383"]', "must link"),
        ("format = 1", "'format' must be 1"),
    ],
)
def test_task_validation(tmp_path: Path, replace: str, message: str) -> None:
    text = (SELFTEST / "task.toml").read_text(encoding="utf-8")
    broken = {
        "base_sha": 'base_sha = "abc"',
        "evidence": "evidence = []",
        "format": "format = 2",
    }[replace.split(" ")[0]]
    with pytest.raises(agent_eval.InputError, match=message):
        agent_eval.load_task(_task(tmp_path, text.replace(replace, broken)))


def test_unknown_rubric_check_is_rejected(tmp_path: Path) -> None:
    text = (SELFTEST / "task.toml").read_text(encoding="utf-8") + '\n[rubric]\nchecks = ["vibes"]\n'
    with pytest.raises(agent_eval.InputError, match="unknown checks"):
        agent_eval.load_task(_task(tmp_path, text))


def test_malformed_evidence_is_rejected(tmp_path: Path) -> None:
    candidate = tmp_path / "c"
    shutil.copytree(SELFTEST / "candidates" / "correct", candidate)
    (candidate / "evidence.json").write_text('[{"command": "x", "tree": "other"}]')
    with pytest.raises(agent_eval.InputError, match="each run needs"):
        agent_eval.grade(SELFTEST, candidate)


def test_parse_diff_handles_new_deleted_and_renamed_files() -> None:
    diff = (
        "--- /dev/null\n+++ b/new.py\n@@ -0,0 +1 @@\n+x = 1\n"
        "--- a/gone.py\n+++ /dev/null\n@@ -1 +0,0 @@\n-y = 2\n"
        "--- a/old.py\n+++ b/renamed.py\n@@ -1 +1 @@\n-z = 3\n+z = 4\n"
    )
    changes = agent_eval.parse_diff(diff)
    assert sorted(changes) == ["gone.py", "new.py", "old.py", "renamed.py"]
    assert changes["new.py"].added == ["x = 1"]
    assert changes["gone.py"].removed == 1


@pytest.mark.parametrize(
    ("line", "unsafe"),
    [
        ('cur.execute(f"SELECT * FROM t WHERE id = {i}")', True),
        ('sql = f"DELETE FROM t WHERE id = {i}"', True),
        ('cur.execute("SELECT * FROM t WHERE id = %s" % i)', True),
        ('cur.execute("SELECT * FROM {}".format(table))', True),
        ('cur.execute("SELECT * FROM t WHERE id = ?", (i,))', False),
        ('message = f"selected {count} rows"', False),
        ('log.info("SELECT finished in %s", elapsed)', False),
    ],
)
def test_sql_interpolation_patterns(line: str, unsafe: bool) -> None:
    changes = {"pycubrid/x.py": agent_eval.FileChange("pycubrid/x.py", added=[line])}
    assert (agent_eval.check_sql_interpolation(changes)["status"] == "fail") is unsafe


def test_sql_interpolation_ignores_non_python_files() -> None:
    line = 'cur.execute(f"SELECT {x}")'
    changes = {"docs/X.md": agent_eval.FileChange("docs/X.md", added=[line])}
    assert agent_eval.check_sql_interpolation(changes)["status"] == "pass"
