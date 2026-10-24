"""Offline checks of ``scripts/report_results.py`` (#1024): outcomes, schema and secrets."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import report_results as rr


def _junit(tmp_path: Path, *cases: str) -> Path:
    path = tmp_path / "junit.xml"
    path.write_text(f"<testsuites><testsuite>{''.join(cases)}</testsuite></testsuites>")
    return path


PASS = '<testcase classname="t" name="ok"/>'
FAIL = '<testcase classname="t" name="bad"><failure message="x"/></testcase>'
ERROR = '<testcase classname="t" name="bad"><error message="x"/></testcase>'
SKIP = '<testcase classname="t" name="s"><skipped type="pytest.skip"/></testcase>'
XFAIL = '<testcase classname="t" name="x"><skipped type="pytest.xfail"/></testcase>'


@pytest.mark.parametrize(
    ("cases", "exit_code", "status"),
    [
        ((PASS, SKIP, XFAIL), 0, "PASS"),
        ((PASS,), 1, "FAIL"),  # e.g. the coverage floor was missed
        ((PASS, FAIL), 0, "FAIL"),
        ((PASS, ERROR), 0, "FAIL"),
        ((SKIP, XFAIL), 0, "INCOMPLETE"),
        ((), 0, "INCOMPLETE"),
    ],
)
def test_status(tmp_path: Path, cases: tuple[str, ...], exit_code: int, status: str) -> None:
    summary = rr.build_summary("test", _junit(tmp_path, *cases), None, exit_code)
    assert summary["status"] == status


def test_missing_or_malformed_junit_is_never_pass(tmp_path: Path) -> None:
    missing = tmp_path / "none.xml"
    assert rr.build_summary("test", missing, None, 0)["status"] == "INCOMPLETE"
    assert rr.build_summary("test", missing, None, 2)["status"] == "FAIL"
    bad = tmp_path / "bad.xml"
    bad.write_text("<testsuite><testcase")
    assert rr.build_summary("test", bad, None, 0)["status"] == "INCOMPLETE"


def test_dtd_and_entity_declarations_are_refused(tmp_path: Path) -> None:
    bomb = tmp_path / "bomb.xml"
    bomb.write_text('<!DOCTYPE x [<!ENTITY a "b">]><testsuite><testcase name="a"/></testsuite>')
    assert rr.parse_junit(bomb) is None
    assert rr.parse_coverage(bomb) is None


def test_a_test_with_failing_teardown_counts_once_with_its_worst_outcome(tmp_path: Path) -> None:
    path = _junit(tmp_path, FAIL, ERROR)
    assert rr.parse_junit(path) == {
        "selected": 1,
        "passed": 0,
        "failed": 0,
        "errors": 1,
        "skipped": 0,
        "xfailed": 0,
        "xpassed": 0,
    }


def test_coverage_and_console_line_match_summary_json(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    junit = _junit(tmp_path, PASS, SKIP)
    (tmp_path / "coverage.xml").write_text('<coverage line-rate="0.9712"/>')
    assert (
        rr.main(
            [
                "summarize",
                "--lane",
                "test",
                "--results-dir",
                str(tmp_path),
                "--exit-code",
                "0",
                "--coverage",
                str(tmp_path / "coverage.xml"),
            ]
        )
        == 0
    )
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert junit.is_file() and summary["schema_version"] == 1
    assert summary["coverage"] == {"line_percent": 97.12}
    assert capsys.readouterr().out.startswith(rr.console_line(summary))
    assert "selected=2 passed=1" in rr.console_line(summary)
    assert "PASS" in rr.step_summary(tmp_path, "Unit tests")


def test_environment_json_holds_no_process_environment(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    for name in ("CUBRID_PASSWORD", "CUBRID_TEST_PASSWORD", "CUBRID_TEST_URL"):
        monkeypatch.setenv(name, "s3cret-value")
    assert (
        rr.main(
            [
                "environment",
                "--lane",
                "integration",
                "--results-dir",
                str(tmp_path),
                "--command",
                "make integration",
                "--cubrid-version",
                "11.2",
            ]
        )
        == 0
    )
    text = (tmp_path / "environment.json").read_text()
    data = json.loads(text)
    assert "s3cret-value" not in text
    assert data["cubrid_version"] == "11.2" and data["package"]["name"] == "pycubrid"
    assert data["package"]["version"] == rr.package_version() != "unknown"
    assert set(data) == {
        "schema_version",
        "lane",
        "command",
        "python",
        "package",
        "cubrid_version",
        "platform",
    }


def test_step_summary_without_summary_json(tmp_path: Path) -> None:
    assert "No readable" in rr.step_summary(tmp_path, None)


def test_step_summary_cli_prints_a_titled_console_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    junit = _junit(tmp_path, PASS, FAIL)
    rr.main(["summarize", "--lane", "test", "--results-dir", str(tmp_path), "--exit-code", "1"])
    capsys.readouterr()
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert junit.is_file()
    assert rr.main(["step-summary", "--results-dir", str(tmp_path), "--title", "Unit"]) == 0
    assert capsys.readouterr().out == f"### Unit\n\n`{rr.console_line(summary)}`\n"


@pytest.mark.parametrize("content", ["{not json", '{"lane": "t"}'])
def test_step_summary_reports_an_unreadable_summary_and_exits_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], content: str
) -> None:
    (tmp_path / "summary.json").write_text(content)
    assert rr.main(["step-summary", "--results-dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert out.startswith("### Test results\n\n") and "No readable" in out
