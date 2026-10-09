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


WORKFLOW_REF = re.compile(r"\.github['\"]?\s*[/,]\s*['\"]?workflows")
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
    return any(
        isinstance(node, ast.Assign)
        and any(getattr(t, "id", "") == "pytestmark" for t in node.targets)
        and "repo_tooling" in ast.unparse(node.value)
        for node in body
    )


def _is_test(node: ast.stmt) -> bool:
    return isinstance(node, _FUNCS) and node.name.startswith("test")


def _is_test_class(node: ast.stmt) -> bool:
    return isinstance(node, ast.ClassDef) and node.name.startswith("Test")


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
                if _marks_repo_tooling(node.decorator_list) or _sets_repo_tooling(node.body):
                    continue
                for item in node.body:
                    if not _reads_workflows(item, tainted):
                        continue
                    if not _is_test(item):
                        offenders.append(f"{path.name}:{node.name} (mark the class)")
                    elif not _marks_repo_tooling(item.decorator_list):
                        offenders.append(f"{path.name}:{node.name}.{item.name}")
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
    ],
    ids=["split-path-constant", "class-method", "async-test", "helper-function"],
)
def test_guard_catches_indirect_workflow_readers(tmp_path: Path, body: str) -> None:
    (tmp_path / "test_probe.py").write_text("import pytest\nROOT = None\n" + body + "\n")
    assert len(_unmarked_workflow_readers(tmp_path)) == 1


@pytest.mark.parametrize(
    "body",
    [
        'def test_x():\n    """Mirrors .github/workflows/ci.yml."""\n    assert True',
        "def test_x():\n    # see .github/workflows/ci.yml\n    assert True",
        'P = ROOT / ".github" / "workflows"\n\n@pytest.mark.repo_tooling\ndef test_x():\n    P',
        'P = ".github/workflows"\n\n@pytest.mark.repo_tooling\nclass TestX:\n'
        "    def test_x(self):\n        P",
    ],
    ids=["docstring-mention", "comment-mention", "function-marked", "class-marked"],
)
def test_guard_ignores_mentions_and_marked_readers(tmp_path: Path, body: str) -> None:
    (tmp_path / "test_probe.py").write_text("import pytest\nROOT = None\n" + body + "\n")
    assert _unmarked_workflow_readers(tmp_path) == []
