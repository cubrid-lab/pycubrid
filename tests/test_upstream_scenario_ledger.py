"""Check the inventory's accounting, references and independent evidence fields."""

from __future__ import annotations

import csv
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / "tests/fixtures/upstream_scenarios.csv"
CLASSIFICATIONS = {
    "unknown",
    "duplicate_candidate",
    "related",
    "assertion_equivalent",
    "unsupported",
}
IDENTITIES = ("verification_commit", "server", "python", "mode", "artifact")


def _validate(rows: list[dict[str, str]], nodes: set[str]) -> None:
    identifiers = {row["id"] for row in rows}
    if len(identifiers) != len(rows):
        raise ValueError("duplicate scenario id")
    for row in rows:
        source = (
            row["path"] + "::" + (row["class"] + "::" if row["class"] else "") + row["function"]
        )
        expected_id = source + ("#" + row["subcase"] if row["subcase"] else "")
        if row["id"] != expected_id or not re.fullmatch(r"test_\w*", row["function"]):
            raise ValueError("invalid source identifier")
        if not re.fullmatch(r"(?:tests|tests2|tests3)/.+\.py", row["path"]):
            raise ValueError("invalid source path")
        if row["kind"] not in {"declaration", "assertion"} or int(row["line"]) < 1:
            raise ValueError("invalid source accounting")
        if (row["kind"] == "assertion") != bool(row["subcase"]) or source not in identifiers:
            raise ValueError("missing declaration for assertion subcase")
        if row["classification"] not in CLASSIFICATIONS:
            raise ValueError("unknown classification")
        if not all(
            row[field].strip()
            for field in ("owner", "family", "expected", "gap_issues", "gap_reason")
        ):
            raise ValueError("missing gap or ownership explanation")
        for candidate in filter(None, row["duplicate_candidate_of"].split("|")):
            if candidate == row["id"] or candidate not in identifiers:
                raise ValueError("broken duplicate candidate reference")
        mapped = set(filter(None, row["local_nodes"].split("|")))
        related = set(filter(None, row["related_nodes"].split("|")))
        if not (mapped | related) <= nodes:
            raise ValueError("broken local pytest node")
        if bool(mapped) != (row["classification"] == "assertion_equivalent"):
            raise ValueError("equivalent assertion requires mapped nodes only")
        if mapped or related:
            if not re.fullmatch(r"[0-9a-f]{40}", row["local_revision"]):
                raise ValueError("missing reviewed local revision")
        status = row["evidence_status"]
        if status == "not_run":
            if row["result"] or any(row[field] for field in IDENTITIES) or row["skip_reason"]:
                raise ValueError("not-run record cannot contain execution claims")
        elif status in {"observed", "verified_pass"}:
            if not all(row[field].strip() for field in IDENTITIES):
                raise ValueError("missing execution identity")
            if not re.fullmatch(r"[0-9a-f]{40}", row["verification_commit"]):
                raise ValueError("invalid verification commit")
            if row["result"] not in {"passed", "failed", "skipped"}:
                raise ValueError("invalid execution result")
            if row["result"] == "skipped" and not row["skip_reason"].strip():
                raise ValueError("unexplained skip")
            if status == "verified_pass" and (not mapped or row["result"] != "passed"):
                raise ValueError("failed, skipped or unmapped record is not a verified pass")
        else:
            raise ValueError("unknown evidence status")


@pytest.fixture(scope="module")
def ledger() -> list[dict[str, str]]:
    with LEDGER.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


@pytest.fixture(scope="module")
def collected_nodes(ledger: list[dict[str, str]]) -> set[str]:
    references = {
        node
        for row in ledger
        for field in ("local_nodes", "related_nodes")
        for node in row[field].split("|")
        if node
    }
    files = sorted({node.split("::", 1)[0] for node in references})
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-o", "addopts=", *files],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return {
        line for line in result.stdout.splitlines() if line.startswith("tests/") and "::" in line
    }


def test_ledger_accounting_and_references(
    ledger: list[dict[str, str]], collected_nodes: set[str]
) -> None:
    _validate(ledger, collected_nodes)
    declarations = [row for row in ledger if row["kind"] == "declaration"]
    assert Counter(row["path"].split("/", 1)[0] for row in declarations) == {
        "tests": 68,
        "tests2": 198,
        "tests3": 184,
    }
    related = {node for row in ledger for node in row["related_nodes"].split("|") if node}
    assert len(related) == 41
    assert any(row["function"] == "test_" for row in declarations)


@pytest.mark.parametrize(
    "changes",
    [
        {"gap_reason": ""},
        {"local_nodes": "tests/missing.py::test_missing"},
        {"evidence_status": "verified_pass", "result": "failed"},
        {"evidence_status": "observed", "result": "skipped"},
        {"evidence_status": "verified_pass", "artifact": ""},
        {"classification": "unsupported", "local_nodes": "", "evidence_status": "verified_pass"},
    ],
)
def test_reject_incomplete_claims(
    ledger: list[dict[str, str]], collected_nodes: set[str], changes: dict[str, str]
) -> None:
    modified = [dict(row) for row in ledger]
    row = next(row for row in modified if row["classification"] == "assertion_equivalent")
    row.update(
        evidence_status="observed",
        result="passed",
        verification_commit="f" * 40,
        server="unit-fixture",
        python="unit-fixture",
        mode="unit-fixture",
        artifact="unit-fixture.xml",
    )
    row.update(changes)
    with pytest.raises(ValueError):
        _validate(modified, collected_nodes)


def test_honest_failed_and_explained_skipped_observations_are_not_passes(
    ledger: list[dict[str, str]], collected_nodes: set[str]
) -> None:
    for result, explanation in (("failed", ""), ("skipped", "synthetic unavailable-server case")):
        modified = [dict(row) for row in ledger]
        row = next(row for row in modified if row["classification"] == "assertion_equivalent")
        row.update(
            evidence_status="observed",
            result=result,
            skip_reason=explanation,
            verification_commit="f" * 40,
            server="unit-fixture",
            python="unit-fixture",
            mode="unit-fixture",
            artifact="unit-fixture.xml",
        )
        _validate(modified, collected_nodes)
        assert row["evidence_status"] != "verified_pass"


def test_duplicate_ids_are_rejected(ledger: list[dict[str, str]]) -> None:
    with pytest.raises(ValueError, match="duplicate scenario id"):
        _validate([ledger[0], ledger[0]], set())
