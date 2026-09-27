"""Regression checks for executable lane coverage and explicit skip exceptions."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.check_integration_lanes import ROOT, skip_category, verify_results, verify_workflows


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


def test_optional_native_comparison_skip_is_classified() -> None:
    assert (
        skip_category(
            "tests/test_cubriddb_differential.py", "official CUBRIDdb C-extension not installed"
        )
        == "optional-native-driver"
    )


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
