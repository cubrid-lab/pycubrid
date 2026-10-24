"""The offline agent-evaluation grader (#925)."""

from __future__ import annotations

import importlib.util
import json
import re
import shutil
import sys
from pathlib import Path
from types import ModuleType

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
    assert capsys.readouterr().out == "pass\n"  # --output prints only the verdict
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
    changes = {"pycubrid/x.py": agent_eval.FileChange("pycubrid/x.py", blocks=[[line]])}
    assert (agent_eval.check_sql_interpolation(changes)["status"] == "fail") is unsafe


def test_sql_interpolation_ignores_non_python_files() -> None:
    line = 'cur.execute(f"SELECT {x}")'
    changes = {"docs/X.md": agent_eval.FileChange("docs/X.md", blocks=[[line]])}
    assert agent_eval.check_sql_interpolation(changes)["status"] == "pass"


def test_git_metadata_only_changes_are_inventoried() -> None:
    # Renames without content, mode-only edits and binary files have no ---/+++ lines.
    diff = (
        "diff --git a/pycubrid/a.py b/pycubrid/b.py\n"
        "similarity index 100%\nrename from pycubrid/a.py\nrename to pycubrid/b.py\n"
        "diff --git a/scripts/run.sh b/scripts/run.sh\nold mode 100644\nnew mode 100755\n"
        "diff --git a/docs/logo.png b/docs/logo.png\nindex 1..2 100644\n"
        "Binary files a/docs/logo.png and b/docs/logo.png differ\n"
    )
    changes = agent_eval.parse_diff(diff)
    assert sorted(changes) == ["docs/logo.png", "pycubrid/a.py", "pycubrid/b.py", "scripts/run.sh"]
    assert changes["pycubrid/a.py"].gone and not changes["pycubrid/b.py"].gone
    assert changes["docs/logo.png"].binary
    task = {"scope": {"allowed": ["pycubrid/*.py"]}}
    outside = agent_eval.check_scope(task, changes)
    assert outside["status"] == "fail"
    assert "out-of-scope change: scripts/run.sh" in outside["details"]


def test_header_like_content_inside_a_hunk_stays_content() -> None:
    diff = (
        "diff --git a/README.md b/README.md\n--- a/README.md\n+++ b/README.md\n"
        "@@ -1,2 +1,2 @@\n--- old note\n+++ heading\n same\n"
    )
    changes = agent_eval.parse_diff(diff)
    assert sorted(changes) == ["README.md"]
    assert changes["README.md"].added == ["++ heading"]
    assert changes["README.md"].removed == 1


@pytest.mark.parametrize(
    "diff",
    [
        "@@ -1 +1 @@\n-a\n+b\n",  # a hunk before any file header
        "diff --git a/x b/x\n--- a/x\n+++ b/x\n@@ -1,2 +1,2 @@\n-a\n",  # truncated hunk
        'diff --git "a/x y" "b/x y"\n',  # a path Git had to quote
    ],
)
def test_unusable_patches_are_rejected(diff: str) -> None:
    with pytest.raises(agent_eval.InputError):
        agent_eval.parse_diff(diff)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ('allowed = ["pycubrid/packet.py",', "allowed = [1,", "list of strings"),
        ("max_files = 3", 'max_files = "3"', "non-negative integer"),
        ("max_files = 3", "max_files = true", "non-negative integer"),
        ("max_minutes = 30", "max_minutes = 30\nmax_cost = 1", "unknown limit"),
    ],
)
def test_manifest_value_types_are_checked(tmp_path: Path, old: str, new: str, message: str) -> None:
    text = (SELFTEST / "task.toml").read_text(encoding="utf-8")
    assert old in text
    with pytest.raises(agent_eval.InputError, match=message):
        agent_eval.load_task(_task(tmp_path, text.replace(old, new)))


def test_rubric_checks_must_be_a_list_of_strings(tmp_path: Path) -> None:
    text = (SELFTEST / "task.toml").read_text(encoding="utf-8") + "\n[rubric]\nchecks = 1\n"
    with pytest.raises(agent_eval.InputError, match="must be list"):
        agent_eval.load_task(_task(tmp_path, text))


def _candidate(tmp_path: Path, **files: str) -> Path:
    candidate = tmp_path / "c"
    shutil.copytree(SELFTEST / "candidates" / "correct", candidate)
    for name, text in files.items():
        (candidate / name.replace("_", ".")).write_text(text, encoding="utf-8")
    return candidate


@pytest.mark.parametrize("code", [2, 3, 4, 5])
def test_a_base_run_must_fail_as_a_test(tmp_path: Path, code: int) -> None:
    evidence = json.loads((SELFTEST / "candidates/correct/evidence.json").read_text())
    evidence[0]["exit_code"] = code  # pytest's collection, usage and no-test codes
    candidate = _candidate(tmp_path, evidence_json=json.dumps(evidence))
    check = agent_eval.grade(SELFTEST, candidate)["checks"]["regression_tests"]
    assert check["status"] == "fail"
    assert f"did not fail as a test on the base tree (exit [{code}])" in check["details"][0]


def test_deleting_a_required_document_does_not_count_as_updating_it(tmp_path: Path) -> None:
    patch = (
        (SELFTEST / "candidates/correct/patch.diff").read_text().split("diff --git a/CHANGELOG")[0]
    )
    patch += (
        "diff --git a/CHANGELOG.md b/CHANGELOG.md\ndeleted file mode 100644\n"
        "--- a/CHANGELOG.md\n+++ /dev/null\n@@ -1,1 +0,0 @@\n-## [Unreleased]\n"
    )
    candidate = _candidate(tmp_path, patch_diff=patch)
    check = agent_eval.grade(SELFTEST, candidate)["checks"]["docs"]
    assert check == {
        "details": ["required documentation deleted or renamed away: CHANGELOG.md"],
        "status": "fail",
    }


@pytest.mark.parametrize(
    "block",
    [
        ['sql = f"""SELECT {value} FROM db_root"""'],
        ["cur.execute(", '    f"""', "    SELECT {value} FROM db_root", '    """', ")"],
        ["sql = (", '    "SELECT * FROM t WHERE id = %s"', "    % value", ")"],
    ],
)
def test_multiline_and_triple_quoted_sql_is_found(block: list[str]) -> None:
    changes = {"pycubrid/x.py": agent_eval.FileChange("pycubrid/x.py", blocks=[block])}
    assert agent_eval.check_sql_interpolation(changes)["status"] == "fail"


@pytest.mark.parametrize(
    ("report", "message"),
    [
        ('{"tests_run": [{"command": "x", "result": "pass"}]}', "'result' (passed|failed)"),
        ('{"tests_run": [{"command": "x"}]}', "'result' (passed|failed)"),
        ('{"tests_run": ["x"]}', "'result' (passed|failed)"),
        ('{"tests_run": {}}', "must be lists"),
        ('{"unrun_checks": [1]}', "list of strings"),
    ],
)
def test_malformed_reports_are_rejected(tmp_path: Path, report: str, message: str) -> None:
    candidate = _candidate(tmp_path, report_json=report)
    with pytest.raises(agent_eval.InputError, match=re.escape(message)):
        agent_eval.grade(SELFTEST, candidate)


def test_a_failing_validation_command_cannot_be_reported_as_unrun(tmp_path: Path) -> None:
    command = "python -m pytest -q tests/test_packet.py"
    evidence = json.loads(
        (SELFTEST / "candidates/misleading-validation-claim/evidence.json").read_text()
    )
    report = json.dumps({"tests_run": [], "unrun_checks": [command]})
    candidate = _candidate(tmp_path, report_json=report, evidence_json=json.dumps(evidence))
    grade = agent_eval.grade(SELFTEST, candidate)
    assert grade["verdict"] == "fail"
    assert grade["checks"]["validation_claims"]["details"] == [
        f"validation command fails on the patched tree (exit [1]): {command}"
    ]


@pytest.mark.parametrize("modified", [False, True])
def test_a_copy_counts_only_its_destination(modified: bool) -> None:
    diff = "diff --git a/docs/old.md b/docs/new.md\nsimilarity index 90%\n"
    diff += "copy from docs/old.md\ncopy to docs/new.md\n"
    if modified:
        diff += "--- a/docs/old.md\n+++ b/docs/new.md\n@@ -1,1 +1,1 @@\n-old\n+new\n"
    changes = agent_eval.parse_diff(diff)
    assert sorted(changes) == ["docs/new.md"]
    task = {"scope": {"allowed": ["docs/new.md"]}, "limits": {"max_files": 1}}
    assert agent_eval.check_scope(task, changes)["status"] == "pass"
    assert agent_eval.check_limits(task, changes)["status"] == "pass"


def test_a_mode_only_change_is_not_a_documentation_update(tmp_path: Path) -> None:
    patch = (
        (SELFTEST / "candidates/correct/patch.diff").read_text().split("diff --git a/CHANGELOG")[0]
    )
    patch += "diff --git a/CHANGELOG.md b/CHANGELOG.md\nold mode 100644\nnew mode 100755\n"
    check = agent_eval.grade(SELFTEST, _candidate(tmp_path, patch_diff=patch))["checks"]["docs"]
    assert check["details"] == ["required documentation has no content change: CHANGELOG.md"]


@pytest.mark.parametrize(
    ("line", "unsafe"),
    [
        # Quotes inside the SQL text do not end the Python string.
        ("sql = \"SELECT * FROM t WHERE name = '{}'\".format(name)", True),
        ("sql = \"SELECT * FROM t WHERE kind = 'user' AND id = %s\" % identifier", True),
        ("cur.execute(\"SELECT * FROM t WHERE note = '% literal'\")", False),
        ("cur.execute(\"SELECT * FROM t WHERE note = 'it''s'\", ())", False),
        ("cur.execute(r'SELECT \\d FROM t WHERE id = ?', (i,))", False),
    ],
)
def test_sql_literals_are_read_as_whole_strings(line: str, unsafe: bool) -> None:
    changes = {"pycubrid/x.py": agent_eval.FileChange("pycubrid/x.py", blocks=[[line]])}
    assert (agent_eval.check_sql_interpolation(changes)["status"] == "fail") is unsafe


TASKS = ROOT / "agent-evals" / "tasks"
TASK_IDS = sorted(p.name for p in TASKS.iterdir() if p.is_dir())


def test_there_are_three_historical_tasks() -> None:
    # #925: a malformed reply length, session generation safety, and a
    # documentation claim with a deliberately unverified condition.
    assert TASK_IDS == ["lab-volume-964", "lob-generation-932", "reply-bounds-383"]


def _enabled(task: dict) -> list[str]:
    return task.get("rubric", {}).get("checks", list(agent_eval.CHECKS))


@pytest.mark.parametrize("task_id", TASK_IDS)
def test_a_task_loads_and_its_reference_fix_passes_the_static_checks(task_id: str) -> None:
    task = agent_eval.load_task(TASKS / task_id)
    assert task["id"] == task_id
    assert re.fullmatch(r"[0-9a-f]{40}", task["expected"]["reference_fix"])
    assert all(
        link.startswith("https://github.com/cubrid-lab/") for link in task["problem"]["evidence"]
    )
    changes = agent_eval.parse_diff((TASKS / task_id / "reference.diff").read_text())
    assert changes
    assert agent_eval.check_scope(task, changes)["status"] == "pass"
    assert agent_eval.check_limits(task, changes)["status"] == "pass"
    assert agent_eval.check_docs(task, changes)["status"] == "pass"
    sql = agent_eval.check_sql_interpolation(changes)["status"]
    # A task turns sql_interpolation off only when its merged fix trips it.
    assert sql == ("pass" if "sql_interpolation" in _enabled(task) else "fail")
    # The reference defines every regression test it names; the runs
    # themselves are recorded only by the opt-in runner.
    evidence = [
        {"command": f"python -m pytest -q {node}", "tree": tree, "exit_code": code}
        for node in task["validation"].get("regression_tests", [])
        for tree, code in (("base", 1), ("candidate", 0))
    ] + [
        {"command": script, "tree": tree, "exit_code": code}
        for script in task["validation"].get("acceptance", [])
        for tree, code in (("base", 1), ("candidate", 0))
    ]
    assert agent_eval.check_regression_tests(task, changes, evidence)["status"] == "pass"


LAB = TASKS / "lab-volume-964"


def test_the_documentation_task_has_a_manual_and_an_acceptance_check() -> None:
    task = agent_eval.load_task(LAB)
    assert task["validation"]["manual"]
    assert task["validation"].get("regression_tests", []) == []
    assert task["validation"]["acceptance"] == ["acceptance.py"]


def _lab_candidate(tmp_path: Path, patch: str, acceptance: tuple[int, int]) -> Path:
    """A lab-volume-964 attempt whose runner evidence gives acceptance exits (base, patched)."""
    task = agent_eval.load_task(LAB)
    commands = task["validation"]["commands"]
    candidate = tmp_path / "lab"
    candidate.mkdir()
    (candidate / "patch.diff").write_text(patch, encoding="utf-8")
    report = {
        "tests_run": [{"command": command, "result": "passed"} for command in commands],
        "unrun_checks": task["validation"]["manual"],
    }
    evidence = [{"command": command, "tree": "candidate", "exit_code": 0} for command in commands]
    evidence += [
        {"command": "acceptance.py", "tree": "base", "exit_code": acceptance[0]},
        {"command": "acceptance.py", "tree": "candidate", "exit_code": acceptance[1]},
    ]
    (candidate / "report.json").write_text(json.dumps(report), encoding="utf-8")
    (candidate / "evidence.json").write_text(json.dumps(evidence), encoding="utf-8")
    return candidate


# Adds an HTML comment to every required document and fixes nothing.
_COMMENT_ONLY = "".join(
    f"diff --git a/{path} b/{path}\n--- a/{path}\n+++ b/{path}\n"
    "@@ -1,0 +2,1 @@\n+<!-- reviewed -->\n"
    for path in ("CHANGELOG.md", "docs/finish-safely.md", "docs/ko/finish-safely.md")
)


def test_the_reference_fix_passes_the_documentation_task(tmp_path: Path) -> None:
    candidate = _lab_candidate(tmp_path, (LAB / "reference.diff").read_text(), (1, 0))
    assert agent_eval.grade(LAB, candidate)["verdict"] == "pass"


@pytest.mark.parametrize(
    ("evidence", "detail"),
    [
        ((1, 1), "acceptance check acceptance.py fails on the patched tree (exit [1])"),
        ((1, None), "no evidence that acceptance check acceptance.py ran on the patched tree"),
    ],
)
def test_a_comment_only_patch_fails_the_documentation_task(
    tmp_path: Path, evidence: tuple[int, int | None], detail: str
) -> None:
    candidate = _lab_candidate(tmp_path, _COMMENT_ONLY, (evidence[0], 1))
    if evidence[1] is None:  # the runner never ran the check on the patched tree
        runs = json.loads((candidate / "evidence.json").read_text())
        runs = [
            r for r in runs if not (r["command"] == "acceptance.py" and r["tree"] == "candidate")
        ]
        (candidate / "evidence.json").write_text(json.dumps(runs))
    grade = agent_eval.grade(LAB, candidate)
    assert grade["checks"]["docs"]["status"] == "pass"  # the comments are content changes
    assert grade["verdict"] == "fail"
    assert grade["checks"]["regression_tests"]["details"] == [detail]


def test_an_acceptance_check_must_find_the_defect_on_the_base(tmp_path: Path) -> None:
    candidate = _lab_candidate(tmp_path, (LAB / "reference.diff").read_text(), (0, 0))
    assert agent_eval.grade(LAB, candidate)["checks"]["regression_tests"]["details"] == [
        "acceptance check acceptance.py did not find the defect on the base tree (exit [0])"
    ]


def _load_acceptance() -> ModuleType:
    spec = importlib.util.spec_from_file_location("lab_acceptance", LAB / "acceptance.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _lab_tree(tmp_path: Path, command: str, explain: bool) -> Path:
    for page in ("docs/quickstart.md", "docs/ko/quickstart.md"):
        (tmp_path / page).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / page).write_text(f"Remove it with `{command}`.\n", encoding="utf-8")
    explanation = {
        "docs/finish-safely.md": "The `-v` also deletes the anonymous volume.",
        "docs/ko/finish-safely.md": "`-v`는 익명 Docker 볼륨도 함께 지웁니다.",
    }
    for page, text in explanation.items():
        body = f"```bash\n{command}\n```\n\n" + (text if explain else "Done.") + "\n"
        (tmp_path / page).write_text(body, encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize(
    ("command", "explain", "code"),
    [
        ("docker rm -f -v pycubrid-quickstart", True, 0),
        ("docker rm --force --volumes pycubrid-quickstart", True, 0),
        ("docker rm -vf pycubrid-quickstart", True, 0),
        ("docker rm -fv pycubrid-quickstart", True, 0),
        ("docker rm -v pycubrid-quickstart", True, 1),  # cannot remove the running lab
        ("docker rm -f pycubrid-quickstart", True, 1),
        ("docker rm -f -v pycubrid-quickstart", False, 1),
    ],
)
def test_the_acceptance_script_reads_the_tutorial_pages(
    tmp_path: Path, command: str, explain: bool, code: int
) -> None:
    acceptance = _load_acceptance()
    assert acceptance.main([str(_lab_tree(tmp_path, command, explain))]) == code


def test_the_acceptance_script_passes_on_the_current_tutorial() -> None:
    acceptance = _load_acceptance()
    assert acceptance.problems(ROOT) == []
    assert acceptance.main([str(ROOT / "missing")]) == 2


def test_an_acceptance_script_must_be_a_file_of_the_task(tmp_path: Path) -> None:
    text = (
        (SELFTEST / "task.toml")
        .read_text(encoding="utf-8")
        .replace("[validation]\n", '[validation]\nacceptance = ["../agent_eval.py"]\n', 1)
    )
    with pytest.raises(agent_eval.InputError, match="acceptance script not in the task"):
        agent_eval.load_task(_task(tmp_path, text))


def _manual_task(tmp_path: Path) -> dict:
    text = (
        (SELFTEST / "task.toml")
        .read_text(encoding="utf-8")
        .replace(
            'regression_tests = ["tests/test_packet.py::test_negative_length_raises"]',
            'regression_tests = ["tests/test_packet.py::test_negative_length_raises"]\n'
            'manual = ["make integration-local"]',
        )
    )
    return agent_eval.load_task(_task(tmp_path, text))


@pytest.mark.parametrize(
    ("report", "expected"),
    [
        ({"unrun_checks": ["make integration-local"]}, []),
        ({}, ["manual check not reported as not run: make integration-local"]),
        (
            {"tests_run": [{"command": "make integration-local", "result": "passed"}]},
            [
                "claimed 'make integration-local' passed, but the runner never ran it",
                "claimed the manual check 'make integration-local' passed; only a person runs it",
            ],
        ),
    ],
)
def test_a_manual_check_must_be_reported_as_not_run(
    tmp_path: Path, report: dict, expected: list[str]
) -> None:
    task = _manual_task(tmp_path)
    command = task["validation"]["commands"][0]
    report = {
        "tests_run": [{"command": command, "result": "passed"}, *report.get("tests_run", [])],
        "unrun_checks": report.get("unrun_checks", []),
    }
    evidence = [{"command": command, "tree": "candidate", "exit_code": 0}]
    details = agent_eval.check_validation_claims(task, report, evidence).get("details", [])
    assert sorted(details) == sorted(expected)


@pytest.mark.parametrize(
    ("addition", "message"),
    [
        ('reference_fix = "abc"', "'reference_fix' must be a full"),
        ("reference_fix = 1", "'reference_fix' must be a full"),
        ("manual = 1", "'manual' must be list"),
    ],
)
def test_reference_fix_and_manual_are_validated(
    tmp_path: Path, addition: str, message: str
) -> None:
    text = (SELFTEST / "task.toml").read_text(encoding="utf-8")
    section = "[validation]" if addition.startswith("manual") else "[expected]"
    text = text.replace(f"{section}\n", f"{section}\n{addition}\n", 1)
    with pytest.raises(agent_eval.InputError, match=re.escape(message)):
        agent_eval.load_task(_task(tmp_path, text))
