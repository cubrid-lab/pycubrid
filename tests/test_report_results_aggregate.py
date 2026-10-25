"""Offline checks of ``make verify``'s aggregate in ``scripts/report_results.py`` (#1053)."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from scripts import report_results as rr

ROOT = Path(__file__).resolve().parents[1]


def _lane(tmp_path: Path, name: str, content: object) -> Path:
    path = tmp_path / name / "summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content if isinstance(content, str) else json.dumps(content))
    return path


def _statuses(summary: dict[str, object]) -> tuple[object, list[object]]:
    phases = summary["phases"]
    assert isinstance(phases, list)
    return summary["status"], [phase["status"] for phase in phases]


def test_every_phase_passing_is_pass(tmp_path: Path) -> None:
    test = _lane(tmp_path, "test", {"status": "PASS"})
    live = _lane(tmp_path, "integration", {"status": "PASS"})
    summary = rr.build_aggregate([("check", 0, None), ("test", 0, test), ("integration", 0, live)])
    assert _statuses(summary) == ("PASS", ["PASS", "PASS", "PASS"])
    assert summary["problems"] == []
    assert summary["schema_version"] == rr.SCHEMA_VERSION and summary["lane"] == "verify"
    assert summary["phases"][1] == {
        "name": "test",
        "status": "PASS",
        "exit_code": 0,
        "summary": str(test),
        "summary_status": "PASS",
    }


def test_a_failed_phase_fails_and_later_phases_are_not_run(tmp_path: Path) -> None:
    test = _lane(tmp_path, "test", {"status": "FAIL"})
    summary = rr.build_aggregate(
        [("check", 0, None), ("test", 1, test), ("integration", None, tmp_path / "none.json")]
    )
    assert _statuses(summary) == ("FAIL", ["PASS", "FAIL", "NOT_RUN"])
    assert "integration did not run" in summary["problems"]
    assert summary["phases"][2]["summary_status"] is None


def test_a_phase_that_did_not_run_without_a_failure_is_incomplete() -> None:
    summary = rr.build_aggregate([("check", 0, None), ("test", None, None)])
    assert _statuses(summary) == ("INCOMPLETE", ["PASS", "NOT_RUN"])


def test_a_lane_summary_saying_fail_fails_even_with_exit_zero(tmp_path: Path) -> None:
    summary = rr.build_aggregate([("test", 0, _lane(tmp_path, "test", {"status": "FAIL"}))])
    assert _statuses(summary) == ("FAIL", ["FAIL"])


@pytest.mark.parametrize(
    "content",
    [
        None,  # missing
        "{not json",
        "[]",
        {"lane": "test"},
        {"status": 0},
        {"status": ["PASS"]},
        {"status": None},
        {"status": "pass"},
        {"status": "NOT_RUN"},
    ],
    ids=[
        "missing",
        "malformed",
        "not-an-object",
        "no-status",
        "int",
        "list",
        "null",
        "lowercase",
        "unknown",
    ],
)
def test_a_missing_unreadable_or_non_string_lane_status_is_never_pass(
    tmp_path: Path, content: object
) -> None:
    path = tmp_path / "test" / "summary.json"
    if content is not None:
        path = _lane(tmp_path, "test", content)
    summary = rr.build_aggregate([("check", 0, None), ("test", 0, path)])
    assert _statuses(summary) == ("INCOMPLETE", ["PASS", "INCOMPLETE"])
    assert summary["phases"][1]["summary_status"] is None
    assert any(str(path) in str(problem) for problem in summary["problems"])


def test_an_incomplete_lane_with_exit_zero_is_incomplete(tmp_path: Path) -> None:
    summary = rr.build_aggregate([("test", 0, _lane(tmp_path, "t", {"status": "INCOMPLETE"}))])
    assert _statuses(summary) == ("INCOMPLETE", ["INCOMPLETE"])
    assert summary["problems"] == ["test is INCOMPLETE"]


def test_a_copy_problem_or_no_phase_is_never_pass() -> None:
    assert rr.build_aggregate([("check", 0, None)], ["could not copy x"])["status"] == "INCOMPLETE"
    assert rr.build_aggregate([])["status"] == "INCOMPLETE"


def _passing_lanes(tmp_path: Path, junit: str) -> tuple[list[rr.Phase], list[tuple[str, Path]]]:
    phases: list[rr.Phase] = [("check", 0, None)]
    copies = []
    for name in ("test", "integration"):
        summary = _lane(tmp_path, name, {"status": "PASS"})
        (summary.parent / "junit.xml").write_text(junit)
        phases.append((name, 0, summary))
        copies.append((name, summary.parent / "junit.xml"))
    return phases, copies


def test_aggregate_writes_masked_copies_and_a_console_line_matching_summary_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    secret = 'p&w"<x> y'
    environ = {
        "CUBRID_TEST_PASSWORD": secret,
        "CUBRID_TLS_TEST_PASSWORD": "tls-pw",
        "CUBRID_TEST_URL": "cubrid://dba:u%40rl@localhost:33000/testdb",
    }
    forms = [
        secret,
        "p&amp;w&quot;&lt;x&gt; y",  # XML attribute
        'p&amp;w"&lt;x&gt; y',  # XML text
        json.dumps(secret)[1:-1],  # JSON string
        "p%26w%22%3Cx%3E%20y",  # percent-encoded
        "tls-pw",
        "u%40rl",
        "u@rl",
    ]
    phases, copies = _passing_lanes(tmp_path, "<testsuite>" + " | ".join(forms) + "</testsuite>")
    environment = tmp_path / "integration" / "environment.json"
    environment.write_text(json.dumps({"command": f"make integration {secret}"}))
    results = tmp_path / "verify"
    summary = rr.aggregate(results, phases, copies, environment, environ)
    assert summary["status"] == "PASS"
    assert json.loads((results / "summary.json").read_text()) == summary
    written = sorted(p.relative_to(results).as_posix() for p in results.rglob("*") if p.is_file())
    assert written == [
        "environment.json",
        "junit/integration.xml",
        "junit/test.xml",
        "summary.json",
    ]
    for name in ("junit/test.xml", "junit/integration.xml", "environment.json"):
        text = (results / name).read_text()
        assert "p&" not in text and "pw" not in text and "rl" not in text, (name, text)
    assert (results / "junit" / "test.xml").read_text().count("***") == len(forms)
    assert json.loads((results / "environment.json").read_text()) == {
        "command": "make integration ***"
    }
    out = capsys.readouterr().out
    assert out == f"{rr.console_line(summary)} -> {results / 'summary.json'}\n"
    assert out.startswith("verify: PASS check=PASS test=PASS integration=PASS -> ")


def test_aggregate_skips_missing_sources_and_reports_failed_copies(tmp_path: Path) -> None:
    phases, copies = _passing_lanes(tmp_path, "<testsuite/>")
    results = tmp_path / "verify"
    results.mkdir()
    (results / "junit").write_text("a file where the junit directory goes")
    summary = rr.aggregate(results, phases, copies, tmp_path / "no-environment.json", {})
    assert summary["status"] == "INCOMPLETE"
    assert _statuses(summary)[1] == ["PASS", "PASS", "PASS"]
    assert len(summary["problems"]) == 2
    assert all(p.startswith("could not copy ") for p in summary["problems"])
    assert not (results / "environment.json").exists()


def test_aggregate_cli_and_step_summary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    test = _lane(tmp_path, "test", {"status": "FAIL"})
    (test.parent / "junit.xml").write_text("<testsuite/>")
    results = tmp_path / "verify"
    code = rr.main(
        [
            "aggregate",
            "--results-dir",
            str(results),
            "--phase",
            "check=0",
            "--phase",
            f"test=1:{test}",
            "--phase",
            f"integration=not-run:{tmp_path / 'integration' / 'summary.json'}",
            "--copy-junit",
            f"test={test.parent / 'junit.xml'}",
        ]
    )
    assert code == 0  # the reporter never changes a status; scripts/run_verify.py does
    summary = json.loads((results / "summary.json").read_text())
    assert _statuses(summary) == ("FAIL", ["PASS", "FAIL", "NOT_RUN"])
    assert (results / "junit" / "test.xml").is_file()
    line = "verify: FAIL check=PASS test=FAIL integration=NOT_RUN [integration did not run]"
    assert capsys.readouterr().out.startswith(line + " -> ")
    assert rr.step_summary(results, "Verify") == f"### Verify\n\n`{line}`\n"


@pytest.mark.parametrize("bad", ["check", "check=x", "=0", "check=1:"])
def test_aggregate_cli_rejects_a_malformed_phase(bad: str, tmp_path: Path) -> None:
    if bad == "check=1:":
        assert rr._phase_arg(bad) == ("check", 1, None)
        return
    with pytest.raises(SystemExit):
        rr.main(["aggregate", "--results-dir", str(tmp_path), "--phase", bad])


def test_configured_secrets_ignore_empty_values_and_a_malformed_url() -> None:
    assert rr.configured_secrets({}) == []
    assert rr.configured_secrets({"CUBRID_TEST_PASSWORD": "", "CUBRID_TEST_URL": "1"}) == []
    assert rr.configured_secrets({"CUBRID_TEST_URL": "cubrid://[::1"}) == []
    forms = rr.configured_secrets({"CUBRID_PASSWORD": "a b"})
    assert forms == sorted(forms, key=len, reverse=True)
    assert set(forms) == {"a b", "a%20b"}


# Every password or URL variable the tests and scripts read.
_SECRET_LITERAL = re.compile(r"""["'](CUBRID_\w*(?:PASSWORD|URL))["']""")


def test_secret_variables_match_what_the_suite_reads_and_what_commands_mask() -> None:
    """One secret list (#1053): the masked variables are every credential variable read.

    The endpoint, TLS tests and scripts read these variables; a new one must be added to
    SECRET_VARIABLES or URL_VARIABLES, and each password variable is also masked by
    redact_command when it appears on a saved command line.
    """
    read = set()
    for path in [*ROOT.glob("tests/*.py"), *ROOT.glob("scripts/*.py")]:
        if path.name.startswith(("test_report_results", "test_makefile")):
            continue  # fixtures of the masking tests themselves
        read.update(_SECRET_LITERAL.findall(path.read_text(encoding="utf-8")))
    assert {"CUBRID_TEST_PASSWORD", "CUBRID_TEST_URL", "CUBRID_TLS_TEST_PASSWORD"} <= read
    assert read <= {*rr.SECRET_VARIABLES, *rr.URL_VARIABLES}
    for name in rr.SECRET_VARIABLES:
        assert rr.redact_command(f"{name}=value") == f"{name}=***"
    for name in rr.URL_VARIABLES:
        command = f"{name}=cubrid://dba:value@localhost/testdb"
        assert "value" not in rr.redact_command(command)


def _serialized_junit(path: Path, secret: str) -> None:
    """A JUnit file written the way pytest writes one, with *secret* in text and attributes."""
    from xml.etree import ElementTree as ET

    suite = ET.Element("testsuite", name="pytest")
    case = ET.SubElement(suite, "testcase", classname="tests.t", name="test_a")
    ET.SubElement(case, "failure", message=f"connect failed: {secret}").text = f"pw={secret}"
    ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)


@pytest.mark.parametrize("secret", ['it\'s "quoted" & <odd>', "test", "testsuite"])
def test_copies_stay_valid_xml_and_json_and_mask_serialized_values(
    tmp_path: Path, secret: str
) -> None:
    """Copilot review of #1054: mask parsed values, never markup or JSON syntax."""
    from xml.etree import ElementTree as ET

    junit = tmp_path / "junit.xml"
    _serialized_junit(junit, secret)
    environment = tmp_path / "environment.json"
    environment.write_text(json.dumps({"lane": "integration", "command": f"x {secret}"}))
    secrets = rr.configured_secrets({"CUBRID_TEST_PASSWORD": secret})

    root = ET.fromstring(rr.mask_copy(junit, secrets))
    assert root.tag == "testsuite" and root.find("testcase") is not None
    failure = root.find("testcase/failure")
    assert failure is not None
    assert failure.get("message") == "connect failed: ***"
    assert failure.text == "pw=***"
    document = json.loads(rr.mask_copy(environment, secrets))
    assert document == {"lane": "integration", "command": "x ***"}


def test_an_unparsable_copy_is_still_masked_byte_for_byte(tmp_path: Path) -> None:
    broken = tmp_path / "junit.xml"
    broken.write_text("<testsuite><testcase name='s3cret'")
    masked = rr.mask_copy(broken, rr.configured_secrets({"CUBRID_PASSWORD": "s3cret"}))
    assert masked == b"<testsuite><testcase name='***'"
    assert rr.mask_copy(broken, []) == broken.read_bytes()
