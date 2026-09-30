"""Regression checks for executable lane coverage and explicit skip exceptions."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.check_integration_lanes import (
    OFFICIAL_SKIP_REASON,
    ROOT,
    skip_category,
    verify_results,
    verify_workflows,
)


def test_workflows_have_executable_normal_slow_and_tls_paths() -> None:
    verify_workflows()


def test_missing_slow_selector_fails_workflow_audit(tmp_path: Path) -> None:
    target = tmp_path / ".github" / "workflows"
    target.mkdir(parents=True)
    for name in ("ci.yml", "integration-full.yml", "bug-hunt.yml"):
        content = (ROOT / ".github" / "workflows" / name).read_text()
        if name == "bug-hunt.yml":
            content = content.replace("integration and slow and not tls", "integration")
        (target / name).write_text(content)
    with pytest.raises(ValueError, match="no executable slow"):
        verify_workflows(tmp_path)


@pytest.mark.parametrize(
    "reason", ["CUBRID instance not available", "TLS broker unavailable", "new unreviewed skip"]
)
def test_broker_tls_and_unknown_skips_cannot_pass(reason: str) -> None:
    with pytest.raises(ValueError, match="unclassified"):
        skip_category("tests/test_integration.py::test_query", reason)


def test_official_differential_skip_is_classified_only_for_its_module() -> None:
    reason = OFFICIAL_SKIP_REASON
    for identity in (
        "tests/test_official_differential.py::test_official_claim[fetch-integer]",
        "tests.test_official_differential::test_official_claim[fetch-integer]",
    ):
        assert skip_category(identity, reason) == "official-lane-only"
    with pytest.raises(ValueError, match="unclassified"):
        skip_category("tests/test_integration.py::test_query", reason)
    with pytest.raises(ValueError, match="unclassified"):
        skip_category(
            "tests/test_official_differential.py::test_official_claim[fetch-integer]",
            "CUBRID instance not available",
        )


def test_official_lane_rejects_every_skip(tmp_path: Path) -> None:
    report = tmp_path / "official.xml"
    report.write_text(
        '<testsuite><testcase classname="tests.test_official_differential" name="a"/>'
        '<testcase classname="tests.test_official_differential" name="b">'
        f'<skipped message="{OFFICIAL_SKIP_REASON}"/></testcase></testsuite>'
    )
    assert verify_results(report)["classified_skips"][0]["category"] == "official-lane-only"
    with pytest.raises(ValueError, match="accepts no skips"):
        verify_results(report, "official")


def test_official_lane_accepts_a_clean_report(tmp_path: Path) -> None:
    report = tmp_path / "official.xml"
    report.write_text(
        '<testsuite><testcase classname="tests.test_official_differential" name="a"/></testsuite>'
    )
    assert verify_results(report, "official")["tests"] == 1


@pytest.mark.parametrize("workflow", ["ci.yml", "integration-full.yml"])
def test_missing_official_lane_fails_workflow_audit(tmp_path: Path, workflow: str) -> None:
    target = tmp_path / ".github" / "workflows"
    target.mkdir(parents=True)
    for name in ("ci.yml", "integration-full.yml", "bug-hunt.yml"):
        content = (ROOT / ".github" / "workflows" / name).read_text()
        if name == workflow:
            content = content.replace("integration and official_differential", "integration")
        (target / name).write_text(content)
    with pytest.raises(ValueError, match="no executable official"):
        verify_workflows(tmp_path)


def test_charset_lane_skip_is_classified_only_for_its_module() -> None:
    reason = "requires an EUC-KR database (integration-charset lane)"
    assert (
        skip_category("tests.test_integration_charset::test_json[sync]", reason)
        == "charset-lane-only"
    )
    assert (
        skip_category("tests/test_integration_charset.py::test_json[sync]", reason)
        == "charset-lane-only"
    )
    for other in (
        "tests.test_integration::test_query",
        "tests.test_integration_charset_fallback::test_query",
        "tests/test_integration.py::test_integration_charset",
    ):
        with pytest.raises(ValueError, match="unclassified"):
            skip_category(other, reason)


def test_missing_charset_lane_fails_workflow_audit(tmp_path: Path) -> None:
    target = tmp_path / ".github" / "workflows"
    target.mkdir(parents=True)
    for name in ("ci.yml", "integration-full.yml", "bug-hunt.yml"):
        content = (ROOT / ".github" / "workflows" / name).read_text()
        if name == "ci.yml":
            content = content.replace("tests/test_integration_charset.py", "tests/")
        (target / name).write_text(content)
    with pytest.raises(ValueError, match="no executable EUC-KR charset lane"):
        verify_workflows(tmp_path)


def test_junit_unknown_skip_cannot_hide_behind_passed_tests(tmp_path: Path) -> None:
    report = tmp_path / "results.xml"
    report.write_text(
        '<testsuite><testcase name="ok"/><testcase name="missing"><skipped message="CUBRID instance not available"/></testcase></testsuite>'
    )
    with pytest.raises(ValueError, match="unclassified"):
        verify_results(report)


def test_empty_result_report_does_not_count_as_validation(tmp_path: Path) -> None:
    report = tmp_path / "results.xml"
    report.write_text("<testsuite/>")
    with pytest.raises(ValueError, match="no test cases"):
        verify_results(report)


def test_junit_entity_declarations_are_rejected(tmp_path: Path) -> None:
    report = tmp_path / "results.xml"
    report.write_text(
        '<!DOCTYPE testsuite [<!ENTITY injected "value">]><testsuite><testcase name="&injected;"/></testsuite>'
    )
    with pytest.raises(ValueError, match="DTD/entity declarations are forbidden"):
        verify_results(report)


def test_tls_matrix_fd_skip_is_classified_but_broker_skip_is_not() -> None:
    node = "tests.test_tls_matrix_integration::test_tls_connect_failures_and_cycles_do_not_leak_fds[sync]"
    assert (
        skip_category(node, "cannot count file descriptors on this platform")
        == "platform-without-proc"
    )
    with pytest.raises(ValueError, match="unclassified"):
        skip_category(node, "TLS-enabled CUBRID broker not available")


def test_version_lane_skip_is_classified_only_for_its_module() -> None:
    reason = "CUBRID_VERSION_MATRIX not set: the version differential runs only in the multi-version lane"
    assert (
        skip_category("tests.test_version_differential::test_scalar_expression_agrees", reason)
        == "version-lane-only"
    )
    assert (
        skip_category("tests/test_version_differential.py::test_scalar_expression_agrees", reason)
        == "version-lane-only"
    )
    with pytest.raises(ValueError, match="unclassified"):
        skip_category("tests.test_integration::test_query", reason)


def test_missing_version_differential_lane_fails_workflow_audit(tmp_path: Path) -> None:
    target = tmp_path / ".github" / "workflows"
    target.mkdir(parents=True)
    for name in ("ci.yml", "integration-full.yml", "bug-hunt.yml"):
        content = (ROOT / ".github" / "workflows" / name).read_text()
        if name == "integration-full.yml":
            content = content.replace('-m "integration and version_matrix"', '-m "integration"')
        (target / name).write_text(content)
    with pytest.raises(ValueError, match="no executable version marker selection"):
        verify_workflows(tmp_path)
