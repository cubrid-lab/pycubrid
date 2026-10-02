"""Fail-closed evidence checks for the nightly downstream corpus (#356)."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.downstream_corpus import collect_reports, main, verify_driver_origin


SHA = "a" * 40
ORIGIN = "https://github.com/cubrid-lab/pycubrid.git"


def _distribution(tmp_path: Path, *, origin: str = ORIGIN, sha: str = SHA):
    package = tmp_path / "site-packages" / "pycubrid" / "__init__.py"
    package.parent.mkdir(parents=True)
    package.write_text("", encoding="utf-8")
    direct_url = json.dumps(
        {
            "url": origin,
            "vcs_info": {"vcs": "git", "requested_revision": sha, "commit_id": sha},
        }
    )
    return SimpleNamespace(
        version="1.8.0",
        read_text=lambda name: direct_url if name == "direct_url.json" else None,
        locate_file=lambda name: tmp_path / "site-packages" / name,
    ), package


def _junit(path: Path, cases: str) -> None:
    path.write_text(f"<testsuites><testsuite>{cases}</testsuite></testsuites>", encoding="utf-8")


def test_driver_origin_requires_exact_commit_repo_and_import_location(tmp_path: Path) -> None:
    dist, package = _distribution(tmp_path)
    evidence = verify_driver_origin(SHA, distribution=dist, import_file=package)
    assert evidence["commit"] == SHA
    assert evidence["origin"] == ORIGIN
    assert evidence["version"] == "1.8.0"


@pytest.mark.parametrize("change", ["commit", "requested", "origin", "missing", "local"])
def test_driver_origin_rejects_substitution(tmp_path: Path, change: str) -> None:
    dist, package = _distribution(tmp_path)
    original = dist.read_text("direct_url.json")
    assert original is not None
    record = json.loads(original)
    if change == "commit":
        record["vcs_info"]["commit_id"] = "b" * 40
    elif change == "requested":
        record["vcs_info"]["requested_revision"] = "main"
    elif change == "origin":
        record["url"] = "https://example.invalid/pycubrid.git"
    elif change == "local":
        record = {"url": "file:///tmp/pycubrid", "dir_info": {"editable": True}}
    dist.read_text = lambda name: None if change == "missing" else json.dumps(record)
    with pytest.raises(ValueError):
        verify_driver_origin(SHA, distribution=dist, import_file=package)


def test_driver_origin_rejects_source_checkout_shadowing(tmp_path: Path) -> None:
    dist, _package = _distribution(tmp_path)
    checkout = tmp_path / "driver" / "pycubrid" / "__init__.py"
    checkout.parent.mkdir(parents=True)
    checkout.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="import"):
        verify_driver_origin(SHA, distribution=dist, import_file=checkout)


def test_each_required_workload_needs_a_pass(tmp_path: Path) -> None:
    _junit(tmp_path / "ai.xml", '<testcase name="agent"/>')
    _junit(tmp_path / "worker.xml", '<testcase name="worker"><skipped message="no DB"/></testcase>')
    summary = collect_reports("cookbook", tmp_path)
    assert summary["status"] == "failure"
    assert any("worker" in reason for reason in summary["reasons"])


def test_mcp_known_empty_schema_skips_are_visible(tmp_path: Path) -> None:
    _junit(
        tmp_path / "mcp.xml",
        '<testcase name="connect"/><testcase name="list"><skipped message="no user tables in database"/></testcase>',
    )
    summary = collect_reports("mcp", tmp_path)
    assert summary["status"] == "success"
    assert summary["workloads"]["mcp"]["passed"] == 1
    assert summary["workloads"]["mcp"]["skipped"] == 1


@pytest.mark.parametrize(
    "cases",
    [
        "",
        '<testcase name="x"><failure message="wrong"/></testcase>',
        '<testcase name="x"><error message="broken"/></testcase>',
        '<testcase name="x"><skipped message="broker unavailable"/></testcase>',
    ],
)
def test_mcp_missing_pass_or_failure_cannot_be_green(tmp_path: Path, cases: str) -> None:
    _junit(tmp_path / "mcp.xml", cases)
    assert collect_reports("mcp", tmp_path)["status"] == "failure"


def test_missing_or_entity_junit_cannot_be_green(tmp_path: Path) -> None:
    assert collect_reports("mcp", tmp_path)["status"] == "failure"
    (tmp_path / "mcp.xml").write_text(
        '<!DOCTYPE testsuite [<!ENTITY x "bad">]><testsuite><testcase name="x"/></testsuite>',
        encoding="utf-8",
    )
    assert collect_reports("mcp", tmp_path)["status"] == "failure"


def test_failed_report_keeps_real_case_counts_in_json_and_markdown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _junit(
        tmp_path / "mcp.xml",
        '<testcase name="connect"/><testcase name="query"><failure message="wrong row"/></testcase>',
    )
    (tmp_path / "driver.json").write_text(
        json.dumps(
            {"commit": SHA, "origin": ORIGIN, "version": "1.8.0", "server_version": "11.4.0"}
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.downstream_corpus.metadata.version", lambda _name: "4.0.0")
    code = main(
        [
            "report",
            "--lane",
            "mcp",
            "--report-dir",
            str(tmp_path),
            "--driver-evidence",
            str(tmp_path / "driver.json"),
            "--expected-sha",
            SHA,
            "--downstream-sha",
            "b" * 40,
            "--job-status",
            "failure",
            "--test-outcome",
            "failure",
            "--out",
            str(tmp_path / "summary.json"),
            "--markdown",
            str(tmp_path / "summary.md"),
        ]
    )
    assert code == 1
    result = json.loads((tmp_path / "summary.json").read_text())
    assert result["workloads"]["mcp"]["cases"] == 2
    assert result["workloads"]["mcp"]["passed"] == 1
    assert result["workloads"]["mcp"]["failures"] == 1
    assert "| mcp | 2 | 1 | 0 | 1 | 0 |" in (tmp_path / "summary.md").read_text()


def test_unexpected_skip_preserves_positive_and_skip_counts(tmp_path: Path) -> None:
    _junit(
        tmp_path / "mcp.xml",
        '<testcase name="connect"/><testcase name="query"><skipped message="broker unavailable"/></testcase>',
    )
    result = collect_reports("mcp", tmp_path)
    assert result["status"] == "failure"
    assert result["workloads"]["mcp"]["cases"] == 2
    assert result["workloads"]["mcp"]["passed"] == 1
    assert result["workloads"]["mcp"]["skipped"] == 1
    assert "broker unavailable" in " ".join(result["reasons"])


def test_sa_and_cookbook_reject_even_partial_skips(tmp_path: Path) -> None:
    _junit(tmp_path / "dogfood.xml", '<testcase name="a"/>')
    _junit(
        tmp_path / "pool.xml", '<testcase name="b"><skipped message="not configured"/></testcase>'
    )
    assert collect_reports("sqlalchemy", tmp_path)["status"] == "failure"
    _junit(tmp_path / "ai.xml", '<testcase name="a"/>')
    _junit(
        tmp_path / "worker.xml", '<testcase name="b"><skipped message="not configured"/></testcase>'
    )
    assert collect_reports("cookbook", tmp_path)["status"] == "failure"


def test_two_positive_sa_workloads_are_required(tmp_path: Path) -> None:
    _junit(tmp_path / "dogfood.xml", '<testcase name="orm"/>')
    assert collect_reports("sqlalchemy", tmp_path)["status"] == "failure"
    _junit(tmp_path / "pool.xml", '<testcase name="pool"/>')
    summary = collect_reports("sqlalchemy", tmp_path)
    assert summary["status"] == "success"
    assert set(summary["workloads"]) == {"dogfood", "pool"}


def test_failure_still_writes_machine_and_human_evidence(tmp_path: Path) -> None:
    report_dir = tmp_path / "reports"
    code = main(
        [
            "report",
            "--lane",
            "cookbook",
            "--report-dir",
            str(report_dir),
            "--driver-evidence",
            str(report_dir / "missing-driver.json"),
            "--expected-sha",
            SHA,
            "--job-status",
            "failure",
            "--test-outcome",
            "failure",
            "--out",
            str(report_dir / "summary.json"),
            "--markdown",
            str(report_dir / "summary.md"),
        ]
    )
    assert code == 1
    assert json.loads((report_dir / "summary.json").read_text())["status"] == "failure"
    assert "driver evidence unavailable" in (report_dir / "summary.md").read_text()


def test_report_rejects_forged_origin_even_with_passing_junit(tmp_path: Path) -> None:
    _junit(tmp_path / "mcp.xml", '<testcase name="connect"/>')
    (tmp_path / "driver.json").write_text(
        json.dumps(
            {
                "commit": SHA,
                "origin": "package index",
                "version": "1.8.0",
                "server_version": "11.4.0",
            }
        ),
        encoding="utf-8",
    )
    code = main(
        [
            "report",
            "--lane",
            "mcp",
            "--report-dir",
            str(tmp_path),
            "--driver-evidence",
            str(tmp_path / "driver.json"),
            "--expected-sha",
            SHA,
            "--downstream-sha",
            "b" * 40,
            "--job-status",
            "success",
            "--test-outcome",
            "success",
            "--out",
            str(tmp_path / "summary.json"),
            "--markdown",
            str(tmp_path / "summary.md"),
        ]
    )
    assert code == 1
    assert "origin" in " ".join(json.loads((tmp_path / "summary.json").read_text())["reasons"])


def test_report_records_python_and_downstream_package_versions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _junit(tmp_path / "mcp.xml", '<testcase name="connect"/>')
    (tmp_path / "driver.json").write_text(
        json.dumps(
            {"commit": SHA, "origin": ORIGIN, "version": "1.8.0", "server_version": "11.4.0"}
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.downstream_corpus.metadata.version", lambda _name: "4.0.0")
    code = main(
        [
            "report",
            "--lane",
            "mcp",
            "--report-dir",
            str(tmp_path),
            "--driver-evidence",
            str(tmp_path / "driver.json"),
            "--expected-sha",
            SHA,
            "--downstream-sha",
            "b" * 40,
            "--job-status",
            "success",
            "--test-outcome",
            "success",
            "--out",
            str(tmp_path / "summary.json"),
            "--markdown",
            str(tmp_path / "summary.md"),
        ]
    )
    assert code == 0
    result = json.loads((tmp_path / "summary.json").read_text())
    assert result["python_version"]
    assert result["package_versions"] == {
        "cubrid-mcp-server": "4.0.0",
        "fastmcp": "4.0.0",
        "pytest": "4.0.0",
    }


@pytest.mark.parametrize("outcome", ["failure", "skipped", ""])
def test_report_rejects_non_success_step_outcome_with_passing_junit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    _junit(tmp_path / "mcp.xml", '<testcase name="connect"/>')
    (tmp_path / "driver.json").write_text(
        json.dumps(
            {"commit": SHA, "origin": ORIGIN, "version": "1.8.0", "server_version": "11.4.0"}
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.downstream_corpus.metadata.version", lambda _name: "4.0.0")
    code = main(
        [
            "report",
            "--lane",
            "mcp",
            "--report-dir",
            str(tmp_path),
            "--driver-evidence",
            str(tmp_path / "driver.json"),
            "--expected-sha",
            SHA,
            "--downstream-sha",
            "b" * 40,
            "--job-status",
            "success",
            "--test-outcome",
            outcome,
            "--out",
            str(tmp_path / "summary.json"),
            "--markdown",
            str(tmp_path / "summary.md"),
        ]
    )
    assert code == 1
    result = json.loads((tmp_path / "summary.json").read_text())
    assert result["workloads"]["mcp"]["passed"] == 1
    assert "selected test step outcome" in " ".join(result["reasons"])


def test_bug_hunt_runs_only_selected_advisory_downstream_workloads() -> None:
    workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/bug-hunt.yml").read_text()
    corpus = workflow.split("  downstream-corpus:\n", 1)[1]
    assert "continue-on-error: true" in corpus
    for lane in ("sqlalchemy", "mcp", "cookbook"):
        assert f"- lane: {lane}" in corpus
    assert "ref: ${{ github.sha }}" in corpus
    assert "path: driver" in corpus and "path: downstream" in corpus
    assert "export PIP_CONSTRAINT=" in corpus
    assert corpus.index("Install all dependencies") < corpus.index("Verify installed driver origin")
    assert "python driver/scripts/wait_for_cubrid.py" in corpus
    for selected in (
        "test/test_dogfood_orm.py",
        "test/test_stress_pool.py",
        "tests/test_integration.py",
        "tests/test_ai_agent.py",
        "templates/async-worker/tests",
    ):
        assert selected in corpus
    assert "python -m pytest test/ " not in corpus
    for step_id in ("sa_tests", "mcp_tests", "cookbook_tests"):
        assert f"id: {step_id}" in corpus
        assert f"steps.{step_id}.outcome" in corpus
    assert '--test-outcome "$test_outcome"' in corpus
    assert "if: always()" in corpus.split("- name: Summarize downstream evidence", 1)[1]
    assert "if: always()" in corpus.split("- name: Upload downstream evidence", 1)[1]
