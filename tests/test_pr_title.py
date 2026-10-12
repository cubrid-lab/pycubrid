"""Offline checks for the PR-title workflow (.github/workflows/pr-title.yml).

The validator lives only inside the workflow (it runs without a checkout), so
these tests extract the embedded script and execute it with PR_TITLE set.
"""

from __future__ import annotations

import hashlib
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

TICK = chr(96)
FENCE = TICK * 3
GUARD_SHA256 = "a44a7759b0897b3bc790605175e29ffb86421f7a245b882a52b70c399817178d"


def _validator() -> str:
    text = WORKFLOW.read_text(encoding="utf-8")
    start = text.index(BEGIN)
    stop = text.index(END, start)
    line_start = text.rindex("\n", 0, start) + 1
    return textwrap.dedent(text[line_start:stop])


def _run(
    title: str, body: str = "", commits: str | None = None, timeout: float | None = None
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
            timeout=timeout,
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

    def test_more_foreign_closing_forms_fail(self) -> None:
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

    def test_more_local_closures_and_refs_pass(self) -> None:
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
            f"- Item\n{fence}\nCloses another-org/another-repo#42\n{fence}",
            f"- Item\n{fence}\ncode\n{fence}\n    Closes another-org/another-repo#42",
        ):
            with self.subTest(body=body):
                self._assert_accepted(body)

    def test_uncertain_markdown_is_checked_on_purpose(self) -> None:
        # The shared guard hides only what it can delimit exactly: inline code,
        # inline comments, quoted and list-item fences, code inside list items, and
        # indented text after an empty list item or a thematic break stay checked.
        # These trade a missed closure for a false positive.
        fence = chr(96) * 3
        for body in (
            "Intro <!-- Closes another-org/another-repo#42 --> outro",
            "Example: `Closes another-org/another-repo#42`.",
            f"Use {chr(96) * 2}Closes another-org/another-repo#42{chr(96) * 2} only there.",
            "Intro <!--\nCloses another-org/another-repo#42\n-->",
            "> ~~~\n> Closes another-org/another-repo#42\n> ~~~",
            "> - Item\n>\n>       Closes another-org/another-repo#42",
            "-\n\n    Closes another-org/another-repo#42",
            "- Item\n---\n    Closes another-org/another-repo#42",
            f"- Item\n  {fence}\n  Closes another-org/another-repo#42\n  {fence}",
            f"- {fence}\n  Closes another-org/another-repo#42\n  {fence}",
            f"1. Item\n\n    {fence}\n    Closes another-org/another-repo#42\n    {fence}",
            "- Item\n\n      Closes another-org/another-repo#42",
            "- Item\n  - Nested\n\n        Closes another-org/another-repo#42",
        ):
            with self.subTest(body=body):
                self._assert_rejected(body)

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

    def assert_blocked(self, *bodies: str, issue: str = "another-org/another-repo#43") -> None:
        for body in bodies:
            with self.subTest(body=body[:200]):
                result = _run("docs: align issue ownership", body)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn("Cross-repository issue closure", result.stdout)
                self.assertIn(issue, result.stdout)
                self.assertNotIn("Traceback", result.stderr)

    def assert_allowed(self, *bodies: str) -> None:
        for body in bodies:
            with self.subTest(body=body[:200]):
                result = _run("docs: align issue ownership", body)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_guard_block_matches_the_shared_copy(self) -> None:
        # This block is meant to be identical in sqlalchemy-cubrid, cubrid-mcp-server
        # and cubrid-cookbook-python; change every copy and this hash together.
        text = WORKFLOW.read_text(encoding="utf-8")
        start = text.index("# BEGIN cross-repo-closing-guard")
        stop = text.index("# END cross-repo-closing-guard")
        digest = hashlib.sha256(text[start:stop].encode("utf-8")).hexdigest()
        self.assertEqual(digest, GUARD_SHA256)

    def test_foreign_repository_closing_keywords_fail(self) -> None:
        self.assert_blocked(
            "Closes another-org/another-repo#43.",
            "Fixes https://github.com/another-org/another-repo/issues/43",
            "Resolves another-org/another-repo#43",
            "Closes: another-org/another-repo#43",
            "FIXES: another-org/another-repo#43",
            "Closes http://github.com/another-org/another-repo/issues/43",
            "Closes www.github.com/another-org/another-repo/issues/43",
            "Closes github.com/another-org/another-repo/issues/43",
            "Closes <https://github.com/another-org/another-repo/issues/43>",
        )

    def test_sibling_repository_closures_fail(self) -> None:
        # The realistic accident: closing a sibling cubrid-lab repository's issue.
        self.assert_blocked(
            "Closes cubrid-lab/sqlalchemy-cubrid#833.",
            "Fixes https://github.com/cubrid-lab/sqlalchemy-cubrid/issues/833",
            issue="cubrid-lab/sqlalchemy-cubrid#833",
        )

    def test_enterprise_managed_owner_fails(self) -> None:
        # Enterprise managed user accounts carry an "_shortcode" suffix.
        self.assert_blocked(
            "Closes alice_acme/another-repo#42",
            "Closes [bug](https://github.com/alice_acme/another-repo/issues/42)",
            issue="alice_acme/another-repo#42",
        )
        result = _run("docs: x", "Closes alice_acme/another-repo#42")
        self.assertNotIn("aliceacme", result.stdout)

    def test_foreign_closure_in_a_commit_message_fails(self) -> None:
        commits = "fix: keep cursor state\n\nCloses another-org/another-repo#43\n"
        result = _run("docs: align issue ownership", "Closes #815", commits)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("in a commit message", result.stdout)
        # Commit messages are plain text: Markdown code does not hide a keyword.
        result = _run("docs: x", "", f"docs: x\n\n{TICK}Closes another-org/another-repo#43{TICK}")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        result = _run("docs: x", "", "fix: a\n\nCloses #815\nRefs another-org/another-repo#43\n")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_foreign_closure_in_the_title_fails(self) -> None:
        result = _run("fix: closes https://github.com/another-org/another-repo/issues/43")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("in a PR title", result.stdout)

    def test_commit_messages_are_listed_before_the_validator(self) -> None:
        # Without this step the commit-message and 250-commit checks never run.
        text = WORKFLOW.read_text(encoding="utf-8")
        listing = (
            'export PR_COMMITS_FILE="$RUNNER_TEMP/pr-commit-messages.txt"\n'
            '          gh api "repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER/commits" --paginate \\\n'
            '            --jq \'.[].commit.message + "\\u0000"\' > "$PR_COMMITS_FILE"\n'
            "          python3 - <<'PY'\n"
        )
        self.assertIn(listing, text)
        for line in (
            "PR_NUMBER: ${{ github.event.pull_request.number }}",
            "PR_COMMIT_COUNT: ${{ github.event.pull_request.commits }}",
            "GH_TOKEN: ${{ github.token }}",
        ):
            self.assertIn(line, text)

    def test_each_commit_message_is_checked_on_its_own(self) -> None:
        # A bracket in one message must not turn on link checks for another.
        commits = "fix: a\n\nCloses [x]\n\0docs: b\n\nRefs https://github.com/o/r/issues/5\n\0"
        result = _run("docs: align issue ownership", "", commits)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_too_many_commits_to_list_fails(self) -> None:
        with unittest.mock.patch.dict(os.environ, {"PR_COMMIT_COUNT": "250"}):
            result = _run("docs: align issue ownership", "", "fix: a\n")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("250 or more commits", result.stdout)
        with unittest.mock.patch.dict(os.environ, {"PR_COMMIT_COUNT": "249"}):
            result = _run("docs: align issue ownership", "", "fix: a\n")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_local_closures_and_foreign_refs_pass(self) -> None:
        self.assert_allowed(
            "Closes #815. Refs another-org/another-repo#43.",
            "Closes #815. Refs cubrid-lab/sqlalchemy-cubrid#833 and cubrid-lab/cubrid-mcp-server#240.",
            "Closes cubrid-lab/pycubrid#815",
            "Closes CUBRID-LAB/PYCUBRID#815",
            "Closes https://github.com/cubrid-lab/pycubrid/issues/815",
            "Refs #815 and another-org/another-repo#43.",
            "Refs **another-org/another-repo#43**",
            "Closes #815, refs another-org/another-repo#43",
            "Fixed in another-org/another-repo#43",
            "Disclosed another-org/another-repo#43",
            "Closes #815.\n\nanother-org/another-repo#43 is related.",
            "## Related Issues\n\n<!-- Use Closes #N for a LOCAL issue.\n"
            "     Cite sibling dependencies as Refs owner/repo#N. -->\nCloses #815",
        )

    def test_emphasis_and_markup_around_a_closure_fail(self) -> None:
        self.assert_blocked(
            "**Closes** another-org/another-repo#43",
            "*Fixes* another-org/another-repo#43",
            "__Resolves__ another-org/another-repo#43",
            "_Closes another-org/another-repo#43_",
            "__Fixes another-org/another-repo#43__",
            "**Closes:** another-org/another-repo#43",
            "**Fixes**: another-org/another-repo#43",
            "Closes **another-org/another-repo#43**",
            "Closes _another-org/another-repo#43_",
            "***Closes*** ***another-org/another-repo#43***",
            "~~Closes~~ another-org/another-repo#43",
            "<b>Closes</b> <code>another-org/another-repo#43</code>",
        )

    def test_escapes_entities_and_lists_of_issues_fail(self) -> None:
        self.assert_blocked(
            "Closes another-org/another-repo\\#43",
            "Closes another-org/another-repo&#35;43",
            "Closes&nbsp;another-org/another-repo#43",
            "Closes #815, another-org/another-repo#43",
            "Fixes #815 and **another-org/another-repo#43**",
            "Closes GH-1, another-org/another-repo#43",
            "Closes #1 & another-org/another-repo#43",
        )

    def test_each_foreign_issue_is_reported_once(self) -> None:
        result = _run(
            "docs: align issue ownership",
            "Closes another-org/another-repo#43 and fixes another-org/another-repo#043",
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(result.stdout.count("::error"), 1, result.stdout)

    def test_a_very_long_issue_number_is_reported_without_a_crash(self) -> None:
        self.assert_blocked(
            "Closes another-org/another-repo#" + "4" * 5000,
            issue="another-org/another-repo#4444",
        )

    def test_code_blocks_and_comment_blocks_are_ignored(self) -> None:
        self.assert_allowed(
            "<!-- Closes another-org/another-repo#43 -->",
            "<!--\nCloses another-org/another-repo#43\n-->",
            "<!-- Use Closes #N.\n     Closes another-org/another-repo#43 -->\nCloses #815",
            f"<!-- {FENCE} -->\nCloses #815",
            f"{FENCE}markdown\nCloses another-org/another-repo#43\n{FENCE}",
            f"{TICK * 4}markdown\nCloses another-org/another-repo#43\n{TICK * 5}",
            "~~~~yaml\nCloses another-org/another-repo#43\n~~~~~",
            f"{TICK * 4}md\nCloses another-org/another-repo#43\n~~~\nCloses x/y#1\n{TICK * 4}",
            "    Closes another-org/another-repo#43",
            "\tCloses another-org/another-repo#43",
            "# Example\n    Closes another-org/another-repo#43",
            f"<details>\n\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}\n\n</details>",
        )

    def test_foreign_closure_after_long_fence_is_rejected(self) -> None:
        self.assert_blocked(
            f"{TICK * 4}markdown\nCloses x/y#1\n{TICK * 4}\nCloses another-org/another-repo#43"
        )

    def test_fences_and_comments_cannot_hide_each_other(self) -> None:
        self.assert_blocked(
            f"{FENCE}\n<!--\n{FENCE}\nCloses another-org/another-repo#43\n-->",
            f"{FENCE}html\n<!-- example\n{FENCE}\nCloses another-org/another-repo#43\n<!-- -->",
            f"<!-- note -->\n{FENCE}\n<!--\n{FENCE}\nCloses another-org/another-repo#43",
            f"<!--\n{FENCE}\n-->\nCloses another-org/another-repo#43",
            f"<!-- {FENCE} -->\nCloses another-org/another-repo#43",
            "<!-- a --> <!-- b\nCloses another-org/another-repo#43\n-->",
            f"{FENCE} a{TICK}b\nCloses another-org/another-repo#43",
        )

    def test_comment_ends_and_inline_comments_stay_live(self) -> None:
        # Only a comment block is hidden; "-->", "--!>", "<!-->" and an inline
        # or escaped comment opener all leave the closure visible.
        self.assert_blocked(
            "<!-->Closes another-org/another-repo#43 -->",
            "<!-->\nCloses another-org/another-repo#43",
            "<!-- a --!> Closes another-org/another-repo#43 -->",
            f"<!-- a --!>\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}\n-->",
            "Intro <!--> Closes another-org/another-repo#43 -->",
            "Intro <!-- Closes another-org/another-repo#43 --> outro",
            "Use <!-- to start a comment.\n\nCloses another-org/another-repo#43",
            "\\<!-- Closes another-org/another-repo#43 -->",
            '<a title="<!--">Closes another-org/another-repo#43<a title="-->">',
            '<div title="\n\n<!--\n">Closes another-org/another-repo#43<a title="\n-->',
        )

    def test_inline_code_stays_live(self) -> None:
        # Code spans need inline parsing (link destinations, HTML attributes and
        # autolinks take precedence over backticks), so they are always scanned.
        self.assert_blocked(
            f"Example: {TICK}Closes another-org/another-repo#43{TICK}.",
            f"Use {TICK * 2}Closes another-org/another-repo#43{TICK * 2} only there.",
            f"Unclosed {TICK}code and Closes another-org/another-repo#43",
            f"\\{TICK}Closes another-org/another-repo#43\\{TICK}",
            f"{TICK}<!--{TICK} Closes another-org/another-repo#43 {TICK}-->{TICK}",
            f"[a](b{TICK}c) Closes another-org/another-repo#43 {TICK}d)",
        )

    def test_html_blocks_keep_fences_and_code_live(self) -> None:
        self.assert_blocked(
            f"<details>\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}\n</details>",
            f"<pre>\n\n{FENCE}\n</pre>\nCloses another-org/another-repo#43\n{FENCE}",
            f"<?x\n\n{FENCE}\n?>\nCloses another-org/another-repo#43\n{FENCE}",
            "<pre>\n\n    Closes another-org/another-repo#43\n</pre>",
            f"<div>\n{TICK}Closes another-org/another-repo#43{TICK}\n</div>",
            f"- <div>\n  {FENCE}\n  Closes another-org/another-repo#43\n  </div>",
        )

    def test_raw_text_elements_end_all_hiding(self) -> None:
        self.assert_blocked(
            f"<textarea>\n\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
            "<textarea>\n\n<!--\nCloses another-org/another-repo#43\n-->",
            f"<select>\n\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
            "Text <select> here\n\n    Closes another-org/another-repo#43",
        )

    def test_paragraph_continuation_stays_live(self) -> None:
        self.assert_blocked("Paragraph text\n    Closes another-org/another-repo#43")

    def test_list_items_keep_their_text_live(self) -> None:
        self.assert_blocked(
            "- Summary\n\n    Closes another-org/another-repo#43",
            "1. Summary\n\n    Closes another-org/another-repo#43",
            "- a\n\n    - Closes another-org/another-repo#43",
            "- Item\n    - Closes another-org/another-repo#43",
            "- Summary\nlazy continuation\n\n    Closes another-org/another-repo#43",
            "- a\n<span>\n\n    Closes another-org/another-repo#43",
            "- a\n # h\n\n  Closes another-org/another-repo#43",
            "- a\n# h\n- b\n\n    Closes another-org/another-repo#43",
            "Text[^1]\n\n[^1]: Note\n\n    Closes another-org/another-repo#43",
            f"- a\n  {FENCE}\n  code\nCloses another-org/another-repo#43\n{FENCE}",
            f"1. {FENCE}\n   {FENCE}\n   Closes another-org/another-repo#43",
            f"- a\n    {FENCE}\n   {FENCE}\n   Closes another-org/another-repo#43",
            f"- a\n  {FENCE}\n  <!--\n  {FENCE}\nCloses another-org/another-repo#43\n-->",
        )

    def test_block_after_a_list_item_ends_the_list(self) -> None:
        # A heading, quote, fence, thematic break or comment block cannot be a
        # lazy continuation, so indented code after it is a real code block.
        self.assert_allowed(
            "- Summary\nlazy\n\nend\n\n    Closes another-org/another-repo#43",
            "- a\n# Heading\n\n    Closes another-org/another-repo#43",
            "- a\n# Heading\n    Closes another-org/another-repo#43",
            "- a\n> quote\n\n    Closes another-org/another-repo#43",
            f"- a\n{FENCE}\nCloses x/y#1\n{FENCE}\n\n    Closes another-org/another-repo#43",
            "- a\n---\n\n    Closes another-org/another-repo#43",
            "1. a\n  # Heading\n    Closes another-org/another-repo#43",
            "- a\n<!-- c -->\n\n    Closes another-org/another-repo#43",
            "- a\n\n<!--\nCloses another-org/another-repo#43\n-->",
        )

    def test_a_fence_of_unknown_extent_stops_all_hiding(self) -> None:
        # A fence in a list item, a quote or raw HTML is not tracked, so its
        # closing line could look like an opener; later fences and comments stay live.
        self.assert_blocked(
            f"-  a\n  {FENCE}\n  x\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
            f"-  a\n  {FENCE}\n<!--\n{FENCE}\nCloses another-org/another-repo#43\n-->",
            f"- a\n  {FENCE}\n  {FENCE}\nz\n  {FENCE}\nx\n{FENCE}\n"
            f"Closes another-org/another-repo#43\n{FENCE}",
            f"* * *\n  {FENCE}\nx\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
            f"- - -\n  {FENCE}\nx\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
            f"- a\n<pre>\n</pre>\n  {FENCE}\nx\n{FENCE}\n"
            f"Closes another-org/another-repo#43\n{FENCE}",
            f"[^1]: note\n\n {FENCE}\nx\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
            f"> {FENCE}\n> x\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
            f"<b>hi</b> there\n{FENCE}\n\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
            f"para\n<span>\n{FENCE}\n\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
            f"<!x\n{FENCE}\n>\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
            f"<!x\n<details>\n{FENCE}\nCloses another-org/another-repo#43",
            f"<pre\x0c>\n\n{FENCE}\n</pre>\nCloses another-org/another-repo#43\n{FENCE}",
            f"<pre\x0b>\n\n{FENCE}\n</pre>\nCloses another-org/another-repo#43\n{FENCE}",
        )
        self.assert_allowed(
            f"- step:\n  {FENCE}bash\n  make test\n  {FENCE}\n\n"
            "Closes #815. Refs another-org/another-repo#43.",
        )

    def test_inline_markup_between_or_inside_words_fails(self) -> None:
        self.assert_blocked(
            "Closes <!-- x --> another-org/another-repo#43",
            "Closes <?x?> another-org/another-repo#43",
            "Closes <![CDATA[x]]> another-org/another-repo#43",
            "Closes <!X y> another-org/another-repo#43",
            "Closes <b title='>'>another-org/another-repo#43</b>",
            'Closes <b title="<">another-org/another-repo#43</b>',
            "Close**s** another-org/another-repo#43",
            "Clo<b>ses</b> another-org/another-repo#43",
            "Closes\\\nanother-org/another-repo#43",
            "Closes another-org/another-repo#043",
            "Closes <!-- a <? b --> another-org/another-repo#43",
            "Closes <!-- " + "x" * 4100 + " --> another-org/another-repo#43",
            "Closes <?" + "x" * 4100 + "?> another-org/another-repo#43",
            # An unclosed opener in an earlier paragraph is plain text.
            "Use <? to open.\n\nCloses <?x?> another-org/another-repo#43",
            "a <!-- b\n\nCloses <!-- c --> another-org/another-repo#43",
            "a <!x\n\nCloses <!-- c --> another-org/another-repo#43",
            "a <![CDATA[ b\n\nCloses <![CDATA[x]]> another-org/another-repo#43",
            "a <!-- b -- c Closes <!-- x --> another-org/another-repo#43",
            # A browser ends "<?" and "<![CDATA[" at the first ">".
            "Closes <?x> another-org/another-repo#43 ?>",
            "Closes <?> another-org/another-repo#43 ?>",
            "Closes <![CDATA[> another-org/another-repo#43 ]]>",
            "Closes&#32;<?\n</b>another-org/another-repo#43<?x?><span>)",
        )

    def test_a_closing_link_fails_when_any_foreign_issue_is_linked(self) -> None:
        # Inline, reference, nested, image and raw HTML links, then absolute
        # and relative issue paths: the link text hides its target.
        self.assert_blocked(
            "Closes [the bug](https://github.com/another-org/another-repo/issues/43)",
            "Closes [another-org/another-repo#43](https://example.com)",
            "Closes [the bug][1]\n\n[1]: https://github.com/another-org/another-repo/issues/43",
            "Closes [bug]\n\n[Bug]: <https://github.com/another-org/another-repo/issues/43>",
            "Closes [a `]` b](https://github.com/another-org/another-repo/issues/43)",
            "Closes [a [b] c](https://github.com/another-org/another-repo/issues/43)",
            "Closes [![badge](https://x/y.png)]"
            "(https://github.com/another-org/another-repo/issues/43)",
            "Closes [bug][1]\n\n> [1]: https://github.com/another-org/another-repo/issues/43",
            "Closes [bug][1]\n\n- [1]: https://github.com/another-org/another-repo/issues/43",
            "Closes [bug][a\\]b]\n\n[a\\]b]: https://github.com/another-org/another-repo/issues/43",
            "Closes [ẞ]\n\n[SS]: https://github.com/another-org/another-repo/issues/43",
            "Closes [a`](x)`](https://github.com/another-org/another-repo/issues/43)",
            "Closes [a\\](x)](https://github.com/another-org/another-repo/issues/43)",
            'Closes <a href="https://github.com/another-org/another-repo/issues/43">bug</a>',
            # Fails closed: a closing link with any foreign issue link in the body.
            "Closes [#815](https://github.com/cubrid-lab/pycubrid/issues/815).\n\n"
            "See https://github.com/another-org/another-repo/issues/43.",
            "Closes [bug](/another-org/another-repo/issues/43)",
            "Closes [bug](../../../another-org/another-repo/issues/43)",
        )
        self.assert_blocked(
            "Closes [bug](../../other-repo/issues/43)", issue="cubrid-lab/other-repo#43"
        )
        self.assert_allowed(
            "Closes [the bug](../issues/815)",
            "Closes [the bug](../../pycubrid/issues/815)",
            "Closes [#815](/cubrid-lab/pycubrid/issues/815)",
            "Closes [#815](https://github.com/cubrid-lab/pycubrid/issues/815). "
            "Refs another-org/another-repo#43.",
        )

    def test_a_closing_keyword_as_link_text_fails(self) -> None:
        # "[Closes](url) owner/repo#N" renders as "Closes owner/repo#N".
        self.assert_blocked(
            "[Closes](https://example.com) another-org/another-repo#43",
            "[Closes](a(b)c) another-org/another-repo#43",
            "[Closes][1] another-org/another-repo#43\n\n[1]: /x",
            "[Closes] another-org/another-repo#43",
            '[Fixes](x "t") [another-org/another-repo#43](y)',
            "![Closes](i.png) another-org/another-repo#43",
            "[**Closes**](x) another-org/another-repo#43",
            # Quoted continuation lines and images show nothing between the words.
            "> [Closes](u)\n> another-org/another-repo#43",
            "> Closes\n> another-org/another-repo#43",
            "- > [Closes](u)\n  > another-org/another-repo#43",
            "Closes ![](i) another-org/another-repo#43",
            "Closes ![x](i) another-org/another-repo#43",
            "[Closes ![i](p)](u) another-org/another-repo#43",
            # A link reference definition line renders as nothing.
            "Closes\n[a]: /i\nanother-org/another-repo#43",
            "Closes ![a]\n\n[a]: /i\nanother-org/another-repo#43",
            "Closes\n[a]:\n  /target\nanother-org/another-repo#43",
            'Closes\n[a]:\n  /target\n  "title"\nanother-org/another-repo#43',
            'Closes\n[a]: /t "1\n2\n3\n4"\nanother-org/another-repo#43',
            'Closes ![a]\n\n[a]:\n /t\n "one\ntwo"\nanother-org/another-repo#43',
            'Closes ![a]\n\n[a]: /t\n "one\ntwo\nthree"\nanother-org/another-repo#43',
        )
        self.assert_allowed(
            "[Closes](https://example.com) the bug. Refs another-org/another-repo#43",
            "See [the fix](x) for another-org/another-repo#43",
            "Ports the upstream [fix](https://github.com/o/r/pull/12).\n\n"
            "Refs https://github.com/another-org/another-repo/issues/43",
            "\n".join(
                f"- [Fix](https://github.com/o/r/pull/{i}) type {i} (notes)" for i in range(300)
            ),
        )

    def test_a_leading_byte_order_mark_is_dropped(self) -> None:
        self.assert_blocked(
            f"\ufeff{FENCE}\nx\n{FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
            "\ufeff<div>\n# h\n    Closes another-org/another-repo#43",
            f"\ufeff<pre>\n\n{FENCE}\n</pre>\nCloses another-org/another-repo#43\n{FENCE}",
        )

    def test_only_cr_and_lf_split_lines(self) -> None:
        # GitHub keeps U+2028 and form feeds inside a line, so they cannot
        # start a fence on what str.splitlines() would call a new line.
        self.assert_blocked(
            f"x {FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
            f"x\x0c{FENCE}\nCloses another-org/another-repo#43\n{FENCE}",
        )

    def test_only_spaces_and_tabs_count_as_whitespace(self) -> None:
        # A no-break space neither makes a line blank nor ends a heading marker.
        self.assert_blocked(
            "Para\n \n    Closes another-org/another-repo#43",
            "# x\n    Closes another-org/another-repo#43",
        )

    def test_long_adversarial_bodies_finish_quickly(self) -> None:
        # GitHub caps a PR body at 65,536 characters; each runs well under a second.
        for body in (
            "Closes" + "*" * 65000 + "x",
            "Closes" + "<b>" * 21000 + "x",
            "[" * 65000,
            "<?" * 32000,
            "<![CDATA[" * 7000,
            "Closes [" * 8000,
            "Closes <!--" * 6000,
            "Closes <?" * 7000,
            "fix<?" * 5000 + ">" + " " * 35000,
            "fix<!--" * 4000 + "-->" + " " * 35000,
            "fix](" * 5000 + ")" * 40536,
            "Closes][" * 4000 + "]" * 33536,
            "Closes ![" * 7000 + "]" * 10000,
            "Closes #1" + ", #2" * 15000 + ", a",
            f"- a\n  {FENCE}\n" * 6000,
        ):
            with self.subTest(body=body[:20]):
                # A regex blow-up raises subprocess.TimeoutExpired instead of hanging.
                result = _run("docs: align issue ownership", body, timeout=5)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
