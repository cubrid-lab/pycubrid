"""Offline checks for the PR-title workflow (.github/workflows/pr-title.yml).

The validator lives only inside the workflow (it runs without a checkout), so
these tests extract the embedded script and execute it with PR_TITLE set.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
import unittest.mock
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


def _run(
    title: str, body: str = "", commits: str | None = None
) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "PR_TITLE": title,
        "PR_BODY": body,
        "GITHUB_REPOSITORY": "cubrid-lab/pycubrid",
    }
    env.pop("PR_COMMITS_FILE", None)
    if commits is not None:
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as handle:
            handle.write(commits)
        env["PR_COMMITS_FILE"] = handle.name
    try:
        return subprocess.run(
            [sys.executable, "-c", _validator()],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
    finally:
        if commits is not None:
            os.unlink(env["PR_COMMITS_FILE"])


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
        job = text.split("\njobs:\n", 1)[1]
        self.assertEqual(
            re.findall(r"(?m)^ +permissions:.*(?:\n {6,}\S.*)*", job),
            ["    permissions:\n      pull-requests: read"],
        )
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
            "x*Closes another-org/another-repo#42",
            "Clo<i title='>'></i>ses another-org/another-repo#42",
            "Closes\\\nanother-org/another-repo#42",
            "Closes [the [bug]](https://github.com/another-org/another-repo/issues/42)",
            "Closes [another-org/another-repo#42]\n\n"
            "[another-org/another-repo#42]: https://github.com/another-org/another-repo/issues/42",
            "Closes [bug][1]\n\n[1]: https://github.com/another-org/another-repo/issues/42",
            "Closes [x][a\\]b]\n\n[a\\]b]: https://github.com/another-org/another-repo/issues/42",
            "Closes [x][a b]\n\n[a\nb]: https://github.com/another-org/another-repo/issues/42",
            "Closes [x][\u1e9e]\n\n[SS]: https://github.com/another-org/another-repo/issues/42",
            "Closes [x\ny](https://github.com/another-org/another-repo/issues/42)",
            'Closes <a href="https://github.com/another-org/another-repo/issues/42">x</a>',
            "Closes _another-org_/another-repo#42",
            "Closes __another-org__/another-repo#42",
            "Closes another-org/another-repo#_42_",
            "Closes https://github.com/another-org/another-repo/issues/__42__",
            "Clo<!-- -->ses another-org/another-repo#42",
            "Closes another-org/another-repo#<!-- -->42",
            "Closes another-org/another-repo#042",
        ):
            with self.subTest(body=body):
                self._assert_rejected(body)

    def test_enterprise_managed_owner_fails(self) -> None:
        self._assert_rejected("Closes alice_acme/another-repo#42", "alice_acme/another-repo#42")

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

    def test_foreign_closure_in_a_commit_message_fails(self) -> None:
        commits = "fix: keep cursor state\n\nCloses another-org/another-repo#42\n"
        result = _run("docs: align issue ownership", "Closes #815", commits)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("in a commit message", result.stdout)
        # Commit messages are plain text: Markdown code does not hide a keyword.
        self._assert_rejected_commits("docs: x\n\n`Closes another-org/another-repo#42`")
        self._assert_accepted_commits("fix: a\n\nCloses #815\nRefs another-org/another-repo#42\n")

    def test_foreign_closure_in_the_title_fails(self) -> None:
        result = _run("fix: closes https://github.com/another-org/another-repo/issues/42")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("in a PR title", result.stdout)

    def test_too_many_commits_to_list_fails(self) -> None:
        env = {**os.environ, "PR_COMMIT_COUNT": "250"}
        with unittest.mock.patch.dict(os.environ, env):
            result = _run("docs: align issue ownership", "", "fix: a\n")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("250 or more commits", result.stdout)

    def _assert_rejected_commits(self, commits: str) -> None:
        result = _run("docs: align issue ownership", "", commits)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def _assert_accepted_commits(self, commits: str) -> None:
        result = _run("docs: align issue ownership", "", commits)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

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
            "Intro <!--\nCloses another-org/another-repo#42\n-->",
            "> ~~~\n> Closes another-org/another-repo#42\n> ~~~",
            "> - Item\n>\n>       Closes another-org/another-repo#42",
            "-\n\n    Closes another-org/another-repo#42",
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
            f"<? >\n{fence}Closes another-org/another-repo#42",
            "> <A\n\tCloses another-org/another-repo#42",
            f">`\n<e>\n1) {fence}Closes another-org/another-repo#42",
            "<div><!-- a --!> Closes another-org/another-repo#42 --></div>",
            # Only an ordered list starting at 1 interrupts a paragraph.
            f"Summary\n2. {fence}\n   Closes another-org/another-repo#42",
            f"- Summary\n  2. {fence}\n     Closes another-org/another-repo#42",
            # An invalid fence line is lazy text and keeps the list item open.
            f"- Summary\n{fence} a{chr(96)}b\n\n    Closes another-org/another-repo#42",
            # Fence and list indentation is relative to the enclosing container.
            f"  {fence}\ncode\n  {fence}\n  Closes another-org/another-repo#42",
            f"- a\n   {fence}\n  code\n   {fence}\n   Closes another-org/another-repo#42",
            f">- {fence}\n>  Closes another-org/another-repo#42",
            # An item that starts empty ends at a blank line, unless it is indented.
            "-\n  \n    Closes another-org/another-repo#42",
            "2.\n\n   ->\n\t-     Closes another-org/another-repo#42",
            # Code spans across quoted lines, after escapes, in tables and around HTML.
            "> Run `make\n> test` then Closes another-org/another-repo#42 `ok`",
            "a `x\n<3 `Closes another-org/another-repo#42` c",
            f"a `x\n{fence}b` Closes another-org/another-repo#42 `c`",
            "\\\\`x` Closes another-org/another-repo#42 `y`",
            "\\\\<!X`>`[Closes another-org/another-repo#42`",
            "| a | b | c |\n|---|---|---|\n| `x | Closes another-org/another-repo#42 | y` |",
            "Note <span title='`'>Closes another-org/another-repo#42</span> `x`",
            "See <https://x.test/`> Closes another-org/another-repo#42 `y`",
            "http://x`Closes another-org/another-repo#42`",
            # Comments that are text under some CommonMark version.
            "Text <!-- Closes another-org/another-repo#42 -- note -->",
            "Text <!-- Closes another-org/another-repo#42 --->",
            # Link reference definitions change how the rest of a paragraph reads.
            "[r]:``\nCloses another-org/another-repo#42``",
            "[r]:`\n-\n    Closes another-org/another-repo#42",
            "[r]:`\n---\n    Closes another-org/another-repo#42",
            # Link destinations, titles and labels hold no code spans or comments.
            "[a](b`c) Closes another-org/another-repo#42 `d",
            '[a](b "`") Closes another-org/another-repo#42 `"',
            "![a](b`c) Closes another-org/another-repo#42 `d",
            "[a][b`c] Closes another-org/another-repo#42 `d\n\n[b`c]: /u",
            "[a](b<!--) Closes another-org/another-repo#42 -->",
            "[a](b\\)`) Closes another-org/another-repo#42 `",
            "[a](b (x\\)`)) Closes another-org/another-repo#42 `",
            "[a][b\\]`] Closes another-org/another-repo#42 `\n\n[b\\]`]: /u",
            "[a](b\\)<!--) Closes another-org/another-repo#42 -->",
            # Footnote definitions are containers that interrupt paragraphs.
            "Text[^n]\n\n[^n]: Note\n\n    Closes another-org/another-repo#42",
            "[^1]:\n\n\tCloses another-org/another-repo#42[^1]",
            "`<!--Closes another-org/another-repo#42\n[^1]:-->",
            "``\n[^1]:\nCloses another-org/another-repo#42``",
        ):
            with self.subTest(body=body):
                self._assert_rejected(body)


if __name__ == "__main__":
    unittest.main()
