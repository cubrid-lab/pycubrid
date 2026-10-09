"""Changed-path selection follows a per-workflow impact table (#761).

Only ``ci.yml`` itself selects the live PR lanes: it defines and runs them. Every
other workflow selects the repository-tooling lane (policy and workflow tests),
because the live lanes of ``ci.yml`` do not execute that workflow's jobs.
``integration-full.yml`` changes are validated by a manual dispatch on the PR head.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.repo_tooling

LIVE = {"risk", "tls", "charset", "official"}


def _filters() -> dict[str, list[str]]:
    steps = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"][
        "detect-changes"
    ]["steps"]
    return yaml.safe_load(steps[-1]["with"]["filters"])


def _glob_re(pattern: str) -> re.Pattern[str]:
    """Translate a paths-filter (picomatch) glob into a regex for these tests."""
    out, i = "", 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out, i = out + "(?:.*/)?", i + 3
        elif pattern.startswith("**", i):
            out, i = out + ".*", i + 2
        elif pattern[i] == "*":
            out, i = out + "[^/]*", i + 1
        elif pattern[i] == "?":
            out, i = out + "[^/]", i + 1
        elif pattern[i] == "{":
            end = pattern.index("}", i)
            alts = [_glob_re(alt).pattern for alt in pattern[i + 1 : end].split(",")]
            out, i = out + "(?:" + "|".join(alts) + ")", end + 1
        else:
            out, i = out + re.escape(pattern[i]), i + 1
    return re.compile(out)


def _selected(path: str) -> set[str]:
    groups = set()
    for group, patterns in _filters().items():
        for pattern in patterns:
            if pattern.startswith("!"):
                hit = not _glob_re(pattern[1:]).fullmatch(path)
            else:
                hit = bool(_glob_re(pattern).fullmatch(path))
            if hit:
                groups.add(group)
                break
    return groups


WORKFLOWS = sorted(p.name for p in (ROOT / ".github/workflows").glob("*.y*ml"))


@pytest.mark.parametrize("name", WORKFLOWS)
def test_workflow_impact_table(name: str) -> None:
    groups = _selected(f".github/workflows/{name}")
    # Every workflow is validated by the tooling lane (policy/workflow tests).
    assert "tooling" in groups, name
    if name == "ci.yml":
        assert LIVE <= groups, "ci.yml defines and runs the live lanes"
    else:
        assert not groups & LIVE, f"{name} does not run in ci.yml's live lanes"


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("pycubrid/aio/connection.py", {"code", "risk", "tls", "charset"}),
        ("pycubrid/compat/__init__.py", {"code", "risk", "official"}),
        ("pycubrid/protocol.py", {"code", "risk", "tls", "charset", "official"}),
        ("tests/test_tls_matrix_offline.py", {"code", "risk", "tls"}),
        ("scripts/wait_for_cubrid.py", {"code", "risk", "tooling", "official"}),
        ("pyproject.toml", {"code", "risk", "tooling"}),
        ("docs/CI_POLICY.md", {"docs"}),
        ("README.md", {"docs"}),
    ],
)
def test_representative_paths_select_the_expected_tiers(path: str, expected: set[str]) -> None:
    assert _selected(path) == expected, path


# The optional ``)`` covers ``Path(".github") / "workflows"``, which ast.unparse
# renders as ``Path('.github') / 'workflows'``.
#
# Non-goals (shapes this guard does not resolve):
# - two-step taint through an intermediate directory constant
#   (``GH = ROOT / ".github"; WF = GH / "workflows"``);
# - glob patterns that only match workflows by wildcard;
# - names imported from another module;
# - tests defined inside a top-level ``if`` or ``try`` block.
# It is conservative the other way: a shadowed name and a plain message string
# that spells a workflow path both count as reads (false positives).
WORKFLOW_REF = re.compile(r"\.github['\"]?\)?\s*[/,]\s*['\"]?workflows")
_TESTCASE_BASES = {"TestCase", "IsolatedAsyncioTestCase"}
_SCOPES = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
_FUNCS = (ast.FunctionDef, ast.AsyncFunctionDef)


def _strip_docstrings(tree: ast.Module) -> None:
    # A docstring is the first statement of a module, class or function; it may
    # mention a workflow without reading one. Comments never reach the AST.
    for node in ast.walk(tree):
        if not isinstance(node, _SCOPES) or not node.body:
            continue
        first = node.body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
            if isinstance(first.value.value, str):
                node.body = node.body[1:] or [ast.Pass()]


def _bound_names(node: ast.stmt) -> set[str]:
    if isinstance(node, (*_FUNCS, ast.ClassDef)):
        return {node.name}
    targets = node.targets if isinstance(node, ast.Assign) else [getattr(node, "target", None)]
    return {n.id for t in targets if t is not None for n in ast.walk(t) if isinstance(n, ast.Name)}


def _reads_workflows(node: ast.AST, tainted: set[str]) -> bool:
    # ast.unparse keeps string constants (``ROOT / '.github' / 'workflows'``)
    # but drops comments; docstrings are stripped beforehand.
    if WORKFLOW_REF.search(ast.unparse(node)):
        return True
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name) and sub.id in tainted:
            return True
        # A fixture is referenced by a test's parameter name.
        if isinstance(sub, ast.arg) and sub.arg in tainted:
            return True
    return False


def _marks_repo_tooling(decorators: list[ast.expr]) -> bool:
    return any("repo_tooling" in ast.unparse(d) for d in decorators)


def _sets_repo_tooling(body: list[ast.stmt]) -> bool:
    # Also inside ``try``/``except``/``else``/``finally`` and ``if`` blocks: the
    # documented ``try: import pytest ... else: pytestmark = ...`` pattern.
    for node in body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if (
                any(getattr(t, "id", "") == "pytestmark" for t in targets)
                and node.value is not None
                and "repo_tooling" in ast.unparse(node.value)
            ):
                return True
        elif isinstance(node, ast.Try):
            blocks = [node.body, node.orelse, node.finalbody, *(h.body for h in node.handlers)]
            if any(_sets_repo_tooling(block) for block in blocks):
                return True
        elif isinstance(node, ast.If):
            if _sets_repo_tooling(node.body) or _sets_repo_tooling(node.orelse):
                return True
    return False


def _is_test(node: ast.stmt) -> bool:
    return isinstance(node, _FUNCS) and node.name.startswith("test")


def _is_test_class(node: ast.stmt) -> bool:
    # pytest collects ``Test*`` classes and ``unittest.TestCase`` subclasses of any name.
    if not isinstance(node, ast.ClassDef):
        return False
    return (
        node.name.startswith("Test")
        or any(_is_test(item) for item in node.body)
        or any(ast.unparse(b).rsplit(".", 1)[-1] in _TESTCASE_BASES for b in node.bases)
    )


def _class_offenders(node: ast.ClassDef, prefix: str, tainted: set[str]) -> list[str]:
    if _marks_repo_tooling(node.decorator_list) or _sets_repo_tooling(node.body):
        return []
    name = f"{prefix}{node.name}"
    offenders = []
    for item in node.body:
        if _is_test_class(item):
            # A nested test class honours its own marks; report the innermost class.
            offenders += _class_offenders(item, f"{name}.", tainted)
        elif not _reads_workflows(item, tainted):
            continue
        elif not _is_test(item):
            offenders.append(f"{name} (mark the class)")
        elif not _marks_repo_tooling(item.decorator_list):
            offenders.append(f"{name}.{item.name}")
    return offenders


def _unmarked_workflow_readers(test_dir: Path) -> list[str]:
    offenders = []
    for path in sorted(test_dir.glob("test_*.py")):
        tree = ast.parse(path.read_text())
        _strip_docstrings(tree)
        if not WORKFLOW_REF.search(ast.unparse(tree)) or _sets_repo_tooling(tree.body):
            continue
        # Module-level constants, helpers and fixtures bound to a workflow path,
        # resolved to a fixed point so helpers of helpers count too.
        bindings = [
            n
            for n in tree.body
            if isinstance(n, (*_FUNCS, ast.ClassDef, ast.Assign, ast.AnnAssign, ast.AugAssign))
            and not _is_test(n)
            and not _is_test_class(n)
        ]
        tainted: set[str] = set()
        changed = True
        while changed:
            changed = False
            for node in bindings:
                names = _bound_names(node) - tainted
                if names and _reads_workflows(node, tainted):
                    tainted |= names
                    changed = True
        for node in tree.body:
            if _is_test(node):
                if _reads_workflows(node, tainted) and not _marks_repo_tooling(node.decorator_list):
                    offenders.append(f"{path.name}:{node.name}")
            elif _is_test_class(node):
                offenders += _class_offenders(node, f"{path.name}:", tainted)
    return offenders


def test_tests_that_read_workflows_run_in_the_tooling_lane() -> None:
    # Non-ci.yml workflow changes select only the tooling lane on PRs, so any test
    # that reads a workflow file must be marked repo_tooling or it runs nowhere.
    assert _unmarked_workflow_readers(ROOT / "tests") == []


@pytest.mark.parametrize(
    "body",
    [
        'P = ROOT / ".github" / "workflows" / "ci.yml"\n\ndef test_x():\n    P.read_text()',
        'W = ".github/workflows/ci.yml"\n\nclass TestX:\n    def test_x(self):\n        W',
        "import os\n\nasync def test_x():\n    os.path.join(ROOT, '.github', 'workflows')",
        'def _ci():\n    return ROOT / ".github/workflows/ci.yml"\n\n'
        "def _jobs():\n    return _ci().read_text()\n\ndef test_x():\n    _jobs()",
        'W = ROOT / ".github/workflows/ci.yml"\n\nclass WorkflowCase(unittest.TestCase):\n'
        "    def test_a(self):\n        W.read_text()",
        'W = Path(".github") / "workflows"\n\ndef test_x():\n    W',
        'W = ".github/workflows"\n\nclass TestOuter:\n    class TestInner:\n'
        "        def test_x(self):\n            W",
    ],
    ids=[
        "split-path-constant",
        "class-method",
        "async-test",
        "helper-function",
        "testcase-subclass",
        "path-call-split",
        "nested-class",
    ],
)
def test_guard_catches_indirect_workflow_readers(tmp_path: Path, body: str) -> None:
    (tmp_path / "test_probe.py").write_text("import pytest\nROOT = None\n" + body + "\n")
    assert len(_unmarked_workflow_readers(tmp_path)) == 1


def test_guard_reports_the_innermost_test_class(tmp_path: Path) -> None:
    body = 'W = ".github/workflows"\n\nclass TestOuter:\n    class TestInner:\n'
    body += "        def helper(self):\n            W\n\n        def test_x(self):\n            W"
    (tmp_path / "test_probe.py").write_text(body + "\n")
    assert _unmarked_workflow_readers(tmp_path) == [
        "test_probe.py:TestOuter.TestInner (mark the class)",
        "test_probe.py:TestOuter.TestInner.test_x",
    ]


@pytest.mark.parametrize(
    "body",
    [
        'def test_x():\n    """Mirrors .github/workflows/ci.yml."""\n    assert True',
        "def test_x():\n    # see .github/workflows/ci.yml\n    assert True",
        'P = ROOT / ".github" / "workflows"\n\n@pytest.mark.repo_tooling\ndef test_x():\n    P',
        'P = ".github/workflows"\n\n@pytest.mark.repo_tooling\nclass TestX:\n'
        "    def test_x(self):\n        P",
        "try:\n    import pytest\nexcept ImportError:\n    pytestmark = []\nelse:\n"
        '    pytestmark = pytest.mark.repo_tooling\n\nP = ".github/workflows"\n\n'
        "def test_x():\n    P",
        'pytestmark: object = pytest.mark.repo_tooling\nP = ".github/workflows"\n\n'
        "def test_x():\n    P",
        'P = ".github/workflows"\n\nclass TestOuter:\n    @pytest.mark.repo_tooling\n'
        "    class TestInner:\n        def test_x(self):\n            P",
    ],
    ids=[
        "docstring-mention",
        "comment-mention",
        "function-marked",
        "class-marked",
        "try-else-pytestmark",
        "annotated-pytestmark",
        "nested-class-marked",
    ],
)
def test_guard_ignores_mentions_and_marked_readers(tmp_path: Path, body: str) -> None:
    (tmp_path / "test_probe.py").write_text("import pytest\nROOT = None\n" + body + "\n")
    assert _unmarked_workflow_readers(tmp_path) == []
