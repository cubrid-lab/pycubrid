"""Offline checks for the PR-title workflow (.github/workflows/pr-title.yml).

The validator lives only inside the workflow (it runs without a checkout), so
these tests extract the embedded script and execute it with PR_TITLE set.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path

try:
    import pytest
except ModuleNotFoundError:
    # Not currently run with bare unittest in CI, but kept importable without
    # pytest for the same reason as test_docs_reason.py (#558): the marker is
    # then meaningless there anyway.
    pytestmark: list[object] = []
else:
    pytestmark = pytest.mark.repo_tooling

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "pr-title.yml"
BEGIN = "# BEGIN pr-title-validator"
END = "# END pr-title-validator"
TYPES = (
    "feat",
    "fix",
    "docs",
    "test",
    "perf",
    "refactor",
    "ci",
    "build",
    "chore",
    "style",
    "revert",
)

VALID = (
    "fix: preserve cursor state after END_TRAN",
    "feat(aio): add charset connection option",
    "refactor(compiler)!: drop legacy LIMIT rendering",
    "feat!: require Python 3.10",
    "chore(deps): bump ruff from 0.16.8 to 0.16.9",
    "chore(deps-dev): bump pytest from 8.0.0 to 8.1.0",
    "chore: release v1.8.0",
    "chore(release): unify release workflow and add RELEASING.md",
    "ci: pin remaining workflow actions to full commit SHAs",
    "docs: document JSON as_numeric() input limits",
    "fix: coerce catalog COUNT(*) in has_table()/has_index()",
    "fix(types): compile BINARY, VARBINARY and UUID to types CUBRID accepts",
    "feat: CUBRID Skills — domain knowledge resources",
    "revert: restore the previous escape-mode probe",
    "build: switch the sdist backend to hatchling",
    "style: apply ruff format",
    "perf: reuse the packet buffer in fetch",
    "test: model transactional DDL in the connection state machine",
)

INVALID = (
    "",
    "Fix: preserve cursor state",
    "fix:preserve cursor state",
    "fix:  preserve cursor state",
    "fix : preserve cursor state",
    "fix: preserve cursor state.",
    "fix: preserve cursor state ",
    "fix: ",
    "[Bug]: cursor state is lost",
    "[WIP] fix: preserve cursor state",
    "fix: WIP preserve cursor state",
    "feature: add charset option",
    "epic: official driver parity",
    "security: reject blank passwords",
    "ci+docs: SBOM on releases",
    "Track: refresh 06_lob CLOB goldens",
    "check_docs_sync: scan pitfalls/* example directories",
    "feat: add charset connection option (#86)",
    "fix: resolve #415 docs: align support policy",
    "fix(Compiler): render Boolean IS",
    "fix(compiler engine): render Boolean IS",
    "fix(): render Boolean IS",
    "fix: 커서 상태 보존",
    "Keep new issue metadata visible without requiring reporter label access",
)


def _validator() -> str:
    text = WORKFLOW.read_text(encoding="utf-8")
    start = text.index(BEGIN)
    stop = text.index(END, start)
    line_start = text.rindex("\n", 0, start) + 1
    return textwrap.dedent(text[line_start:stop])


def _run(title: str, body: str = "") -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "PR_TITLE": title,
        "PR_BODY": body,
        "GITHUB_REPOSITORY": "cubrid-lab/pycubrid",
    }
    return subprocess.run(
        [sys.executable, "-c", _validator()],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


class PrTitleValidatorTest(unittest.TestCase):
    def test_valid_titles_pass(self) -> None:
        for title in VALID:
            with self.subTest(title=title):
                result = _run(title)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertNotIn("::error", result.stdout)

    def test_invalid_titles_fail_with_guidance(self) -> None:
        for title in INVALID:
            with self.subTest(title=title):
                result = _run(title)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn("::error title=PR title::", result.stdout)
                self.assertIn("Allowed types: " + ", ".join(TYPES), result.stdout)

    def test_capitalized_description_only_warns(self) -> None:
        result = _run("docs: Korean site pages")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("::warning title=PR title::", result.stdout)

    def test_invalid_header_error_lists_all_title_forms(self) -> None:
        result = _run("improve cursor handling")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(
            "'type: description', 'type(scope): description', "
            "'type!: description', or 'type(scope)!: description'",
            result.stdout,
        )
        self.assertIn(
            "Format: type: description | type(scope): description | "
            "type!: description | type(scope)!: description",
            result.stdout,
        )

    def test_workflow_is_unprivileged_and_stable(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("  pull_request:\n    types: [opened, edited, reopened, synchronize]", text)
        self.assertNotRegex(text, r"(?m)^\s*pull_request_target:")
        self.assertIn("\npermissions: {}\n", text)
        self.assertNotIn("actions/checkout", text)
        self.assertIn("PR_TITLE: ${{ github.event.pull_request.title }}", text)
        self.assertIn("PR_BODY: ${{ github.event.pull_request.body }}", text)
        self.assertRegex(text, r"\n    name: PR title\n")
        run_block = text.split("run: |", 1)[1]
        self.assertNotIn("${{", run_block)

    def test_type_list_matches_workflow(self) -> None:
        match = re.search(r"TYPES = \((.*?)\)", _validator(), re.DOTALL)
        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(tuple(re.findall(r'"([a-z]+)"', match[1])), TYPES)

    def _assert_rejected(self, body: str, reference: str = "another-org/another-repo#42") -> None:
        result = _run("docs: align issue ownership", body)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("Cross-repository issue closure", result.stdout)
        self.assertIn(reference, result.stdout)

    def _assert_accepted(self, body: str) -> None:
        result = _run("docs: align issue ownership", body)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_foreign_repository_closing_keywords_fail(self) -> None:
        for body in (
            "Resolves another-org/another-repo#42",
            "Fixes: another-org/another-repo#42",
            "FIXES: another-org/another-repo#42",
            "Fixes https://github.com/another-org/another-repo/issues/42",
            "Closes http://github.com/another-org/another-repo/issues/42",
            "Closes www.github.com/another-org/another-repo/issues/42",
            "Closes <https://github.com/another-org/another-repo/issues/42>",
            "Closes [the bug](https://github.com/another-org/another-repo/issues/42)",
        ):
            with self.subTest(body=body):
                self._assert_rejected(body)

    def test_emphasized_or_marked_up_keywords_fail(self) -> None:
        for body in (
            "**Closes** another-org/another-repo#42",
            "*Fixes* another-org/another-repo#42",
            "__Resolves__ another-org/another-repo#42",
            "_Closes another-org/another-repo#42_",
            "__Fixes another-org/another-repo#42__",
            "**Closes:** another-org/another-repo#42",
            "Closes **another-org/another-repo#42**",
            "~~Closes another-org/another-repo#42~~",
            "<b>Closes</b> another-org/another-repo#42",
            "Clo<i></i>ses another-org/another-repo#42",
            "Closes&nbsp;another-org/another-repo#42",
            "Clo&#115;es another-org/another-repo#42",
            "Closes another-org/another-repo\\#42",
        ):
            with self.subTest(body=body):
                self._assert_rejected(body)

    def test_local_closures_and_foreign_refs_pass(self) -> None:
        for body in (
            "Closes #815. Refs cubrid-lab/sqlalchemy-cubrid#810.",
            "Closes cubrid-lab/pycubrid#833",
            "Closes https://github.com/cubrid-lab/pycubrid/issues/833",
            "**Closes** cubrid-lab/pycubrid#833",
            "Refs #815 and cubrid-lab/cubrid-cookbook-python#240.",
            "Encloses another-org/another-repo#42 as a reference.",
        ):
            with self.subTest(body=body):
                self._assert_accepted(body)

    def test_inert_markdown_is_ignored(self) -> None:
        fence = chr(96) * 3
        for body in (
            "<!-- Closes another-org/another-repo#42 -->",
            "<!--\nCloses another-org/another-repo#42\n-->",
            "<!-- Use Closes #N.\n     Closes another-org/another-repo#42 -->\nCloses #815",
            "Intro <!-- Closes another-org/another-repo#42 --> outro",
            f"<!-- {fence} -->\nCloses #815",
            f"{fence}markdown\nCloses another-org/another-repo#42\n{fence}",
            f"{chr(96) * 4}markdown\nCloses another-org/another-repo#42\n{chr(96) * 5}",
            "~~~~yaml\nCloses another-org/another-repo#42\n~~~~~",
            f"{chr(96) * 4}md\nCloses another-org/another-repo#42\n~~~\n{chr(96) * 4}",
            f"<details>\n\n{fence}\nCloses another-org/another-repo#42\n{fence}\n\n</details>",
            "    Closes another-org/another-repo#42",
            "\tCloses another-org/another-repo#42",
            "Intro\n\n    Closes another-org/another-repo#42",
            "# Example\n    Closes another-org/another-repo#42",
            "Example: `Closes another-org/another-repo#42`.",
            f"Use {chr(96) * 2}Closes another-org/another-repo#42{chr(96) * 2} only there.",
            "- Summary\nlazy\n\nend\n\n    Closes another-org/another-repo#42",
        ):
            with self.subTest(body=body):
                self._assert_accepted(body)

    def test_blocks_under_a_list_item_are_not_over_blocked(self) -> None:
        fence = chr(96) * 3
        for body in (
            "- Item\n# Heading\n    Closes another-org/another-repo#42",
            "- Item\n# Heading\n\n    Closes another-org/another-repo#42",
            "- Item\n> Quote\n\n    Closes another-org/another-repo#42",
            "- Item\n---\n    Closes another-org/another-repo#42",
            f"- Item\n{fence}\nCloses another-org/another-repo#42\n{fence}",
            f"- Item\n{fence}\ncode\n{fence}\n    Closes another-org/another-repo#42",
            f"- Item\n  {fence}\n  Closes another-org/another-repo#42\n  {fence}",
            f"- {fence}\n  Closes another-org/another-repo#42\n  {fence}",
            f"1. Item\n\n    {fence}\n    Closes another-org/another-repo#42\n    {fence}",
            "- Item\n\n      Closes another-org/another-repo#42",
            "- Item\n  - Nested\n\n        Closes another-org/another-repo#42",
        ):
            with self.subTest(body=body):
                self._assert_accepted(body)

    def test_inert_markdown_cannot_hide_a_live_closure(self) -> None:
        fence = chr(96) * 3
        for body in (
            # Fences and comments are scanned in one pass.
            f"{fence}\n<!--\n{fence}\nCloses another-org/another-repo#42\n-->",
            f"<!--\n{fence}\n-->\nCloses another-org/another-repo#42",
            f"<!-- {fence} -->\nCloses another-org/another-repo#42",
            f"{fence}html\n<!-- example\n{fence}\nCloses another-org/another-repo#42\n<!-- -->",
            f"<!-- note -->\n{fence}\n<!--\n{fence}\nCloses another-org/another-repo#42",
            f"{chr(96) * 4}markdown\nx\n{chr(96) * 4}\nCloses another-org/another-repo#42",
            # A backtick fence's info string cannot contain a backtick.
            f"{fence} a{chr(96)}b\nCloses another-org/another-repo#42",
            f"{fence}a{chr(96)}b\nCloses another-org/another-repo#42",
            # Comments: line-start blocks, inline, empty, escaped and in code spans.
            "<!-- a --> <!-- b\nCloses another-org/another-repo#42\n-->",
            "<!-->Closes another-org/another-repo#42 -->",
            "<!-->\nCloses another-org/another-repo#42",
            "Intro <!--> Closes another-org/another-repo#42 -->",
            "Use <!-- to start a comment.\n\nCloses another-org/another-repo#42",
            "\\<!-- Closes another-org/another-repo#42 -->",
            f"{chr(96)}<!--{chr(96)} Closes another-org/another-repo#42 {chr(96)}-->{chr(96)}",
            # Code spans: unclosed, escaped, and closed on a later line.
            "Unclosed `code and Closes another-org/another-repo#42",
            f"\\{chr(96)}Closes another-org/another-repo#42\\{chr(96)}",
            f"Text {chr(96)}a\nb{chr(96)} Closes another-org/another-repo#42 {chr(96)}c{chr(96)}",
            # Indentation that is paragraph or list text, not code.
            "Summary text\n    Closes another-org/another-repo#42",
            "- Item\n    - Closes another-org/another-repo#42",
            "- Summary\n\n    Closes another-org/another-repo#42",
            "1. Summary\n\n    Closes another-org/another-repo#42",
            "- a\n\n    - Closes another-org/another-repo#42",
            "- Summary\nlazy continuation\n\n    Closes another-org/another-repo#42",
            "- Item\n  - Nested\nlazy\n\n      Closes another-org/another-repo#42",
            # A less indented line ends the list item and its fence or comment.
            f"- a\n  {fence}\n  code\nCloses another-org/another-repo#42\n{fence}",
            "- a\n  <!--\nCloses another-org/another-repo#42\n-->",
            # Only CR and LF end a Markdown line.
            "Intro\n\n\u2028    Closes another-org/another-repo#42",
            # Raw HTML blocks render fences and code spans as text.
            f"<details>\n{fence}\nCloses another-org/another-repo#42\n{fence}\n</details>",
            f"<pre>\n\n{fence}\nCloses another-org/another-repo#42\n{fence}\n</pre>",
            "<div>\n`Closes another-org/another-repo#42`\n</div>",
        ):
            with self.subTest(body=body):
                self._assert_rejected(body)


if __name__ == "__main__":
    unittest.main()
