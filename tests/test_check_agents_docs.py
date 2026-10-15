"""Agent guidance checks: root AGENTS.md and the docs/agents/ guides (#926)."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.repo_tooling

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "check_agents_docs", ROOT / "scripts/check_agents_docs.py"
)
assert _SPEC is not None and _SPEC.loader is not None
check = importlib.util.module_from_spec(_SPEC)
sys.modules["check_agents_docs"] = check
_SPEC.loader.exec_module(check)

ROOT_DOC = """# AGENTS.md

## Task Routing

| Task | Read |
|---|---|
| Protocol | [Wire protocol guide](docs/agents/protocol.md#wire-protocol-summary) |

## Release Process

Run `make test` and `python scripts/tool.py`. Versions: [matrix](docs/SUPPORT_MATRIX.md).
"""

GUIDE = """# Wire protocol guide

> Moved from the root [AGENTS.md](../../AGENTS.md#release-process).

## Wire Protocol Summary

CUBRID 10.2 behavior; see the [support matrix](../SUPPORT_MATRIX.md).
"""


def _tree(tmp_path: Path, root_doc: str = ROOT_DOC, guide: str = GUIDE) -> Path:
    (tmp_path / "docs/agents").mkdir(parents=True)
    (tmp_path / "scripts").mkdir()
    (tmp_path / "AGENTS.md").write_text(root_doc, encoding="utf-8")
    (tmp_path / "docs/agents/protocol.md").write_text(guide, encoding="utf-8")
    (tmp_path / "docs/SUPPORT_MATRIX.md").write_text("# Support\n", encoding="utf-8")
    (tmp_path / "scripts/tool.py").write_text("", encoding="utf-8")
    (tmp_path / "Makefile").write_text("test: ## Run tests\n\tpytest\n", encoding="utf-8")
    return tmp_path


def test_repository_agent_guidance_is_valid() -> None:
    assert check.run(ROOT) == []


def test_valid_fixture_passes(tmp_path: Path) -> None:
    assert check.run(_tree(tmp_path)) == []


def test_broken_relative_link_reports_file_and_line(tmp_path: Path) -> None:
    root = _tree(tmp_path, ROOT_DOC + "\nSee [gone](docs/GONE.md).\n")
    assert check.run(root) == ["AGENTS.md:13: link target 'docs/GONE.md' does not exist"]


def test_missing_anchor_is_reported(tmp_path: Path) -> None:
    guide = GUIDE.replace("#release-process", "#release-processes")
    (problem,) = check.run(_tree(tmp_path, guide=guide))
    assert problem == (
        "docs/agents/protocol.md:3: anchor #release-processes is not a heading of AGENTS.md"
    )


def test_external_links_and_code_spans_are_not_checked(tmp_path: Path) -> None:
    extra = (
        "\n[site](https://example.invalid/x) and `[x](docs/NOPE.md)`\n\n```\n[y](nope.md)\n```\n"
    )
    assert check.run(_tree(tmp_path, ROOT_DOC + extra)) == []


def test_titled_links_are_checked(tmp_path: Path) -> None:
    root_doc = ROOT_DOC.replace(
        "(docs/agents/protocol.md#wire-protocol-summary)",
        '(docs/agents/protocol.md#wire-protocol-summary "Protocol")',
    )
    assert check.run(_tree(tmp_path, root_doc)) == []
    broken = (
        root_doc + "\nSee [gone](docs/GONE.md 'title') and [x](<docs/agents/protocol.md#nope>).\n"
    )
    assert check.run(_tree(tmp_path / "broken", broken)) == [
        "AGENTS.md:13: link target 'docs/GONE.md' does not exist",
        "AGENTS.md:13: anchor #nope is not a heading of docs/agents/protocol.md",
    ]


def test_guide_named_only_in_code_is_not_reachable(tmp_path: Path) -> None:
    root_doc = ROOT_DOC.replace(
        "[Wire protocol guide](docs/agents/protocol.md#wire-protocol-summary)",
        "`[Wire protocol guide](docs/agents/protocol.md)`",
    )
    assert check.run(_tree(tmp_path, root_doc)) == [
        "docs/agents/protocol.md: not linked from AGENTS.md"
    ]
    fenced = root_doc + "\n```\n[Wire protocol guide](docs/agents/protocol.md)\n```\n"
    assert check.run(_tree(tmp_path / "fenced", fenced)) == [
        "docs/agents/protocol.md: not linked from AGENTS.md"
    ]


@pytest.mark.parametrize(
    "owner",
    [
        "`[support matrix](../SUPPORT_MATRIX.md)`",
        "\n```\n[support matrix](../SUPPORT_MATRIX.md)\n```",
    ],
)
def test_version_owner_link_in_code_does_not_count(tmp_path: Path, owner: str) -> None:
    guide = GUIDE.replace("[support matrix](../SUPPORT_MATRIX.md)", owner)
    (problem,) = check.run(_tree(tmp_path, guide=guide))
    assert "names 'CUBRID 10.2' but links neither" in problem


def test_guide_not_linked_from_root_is_reported(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    (root / "docs/agents/release.md").write_text("# Release guide\n", encoding="utf-8")
    assert check.run(root) == ["docs/agents/release.md: not linked from AGENTS.md"]


def test_unknown_make_target_and_missing_script_are_reported(tmp_path: Path) -> None:
    root = _tree(tmp_path, ROOT_DOC + "\n```bash\nmake tset\npython scripts/missing.py\n```\n")
    assert check.run(root) == [
        "AGENTS.md:14: `make tset` is not a Makefile target",
        "AGENTS.md:15: scripts/missing.py does not exist",
    ]


def test_version_without_an_owning_document_link_is_reported(tmp_path: Path) -> None:
    guide = GUIDE.replace("; see the [support matrix](../SUPPORT_MATRIX.md)", "")
    (problem,) = check.run(_tree(tmp_path, guide=guide))
    assert problem.startswith("docs/agents/protocol.md:7: names 'CUBRID 10.2' but links neither")


def test_duplicate_headings_get_github_suffixes(tmp_path: Path) -> None:
    path = tmp_path / "x.md"
    path.write_text("# A\n## Same\n## Same\n```\n## Not a heading\n```\n", encoding="utf-8")
    assert check.anchors(path) == {"a", "same", "same-1"}
    assert check.slugify("Development Workflow (cubrid-lab org standard)") == (
        "development-workflow-cubrid-lab-org-standard"
    )


def _git_tree(tmp_path: Path) -> Path:
    root = _tree(tmp_path)
    for args in (
        ["init", "-q"],
        ["add", "-A"],
        ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "base"],
    ):
        subprocess.run(["git", "-C", str(root), *args], check=True)
    return root


def test_moved_rule_with_rebased_link_is_retained(tmp_path: Path) -> None:
    root = _git_tree(tmp_path)
    moved = "Versions: [matrix](docs/SUPPORT_MATRIX.md)."
    (root / "AGENTS.md").write_text(
        ROOT_DOC.replace("Run `make test` and `python scripts/tool.py`. " + moved, "Run.\n"),
        encoding="utf-8",
    )
    (root / "docs/agents/protocol.md").write_text(
        GUIDE + "\nRun `make test` and `python scripts/tool.py`. "
        "Versions: [matrix](../SUPPORT_MATRIX.md).\n",
        encoding="utf-8",
    )
    assert check.run(root, "HEAD") == []


def test_dropped_rule_is_reported(tmp_path: Path) -> None:
    root = _git_tree(tmp_path)
    (root / "AGENTS.md").write_text(
        ROOT_DOC.replace("Run `make test` and", "Run"), encoding="utf-8"
    )
    (problem,) = check.run(root, "HEAD")
    assert problem.startswith("HEAD:AGENTS.md:11: not retained: Run `make test` and")


def test_cli_exit_status(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert check.main(["--root", str(_tree(tmp_path))]) == 0
    assert "OK: 2 agent guidance files" in capsys.readouterr().out
    root = tmp_path / "bad"
    root.mkdir()
    _tree(root, ROOT_DOC.replace("protocol.md#", "protocol2.md#"))
    assert check.main(["--root", str(root)]) == 1
    assert "does not exist" in capsys.readouterr().out
