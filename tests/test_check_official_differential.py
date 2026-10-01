"""Offline checks for the official-behavior claims ledger and its evidence gate (#446)."""

from __future__ import annotations

import copy
import hashlib
import json
import sysconfig
from pathlib import Path
from typing import Any

import pytest

from scripts import build_official_oracle as oracle
from scripts import check_official_differential as gate
from scripts.check_integration_lanes import OFFICIAL_SKIP_REASON
from tests.test_official_differential import SKIP_REASON, render

pytestmark = pytest.mark.repo_tooling

DOC = gate.load_claims()
INVENTORY = gate.inventory_ids()
SCENARIOS = gate.scenario_ids()
CASES = gate.case_ids()
PINS = gate.build_pins()


def _errors(doc: dict[str, Any], cases: list[str] | None = None) -> list[str]:
    return gate.check_claims(
        doc,
        inventory=INVENTORY,
        scenarios=SCENARIOS,
        cases=CASES if cases is None else cases,
        pins=PINS,
    )


def _claim(doc: dict[str, Any], cid: str) -> dict[str, Any]:
    claim: dict[str, Any] = next(c for c in doc["claims"] if c["id"] == cid)
    return claim


def _evidence(doc: dict[str, Any] = DOC) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for server in ("10.2.18.9024", "11.4.6.1963"):
        records.append(
            {
                "record": "environment",
                "python": "3.10.18",
                "pycubrid_commit": "0" * 40,
                "server_version": server,
                "cubrid_python_commit": doc["oracle"]["cubrid_python_commit"],
                "cci_commit": doc["oracle"]["cci_commit"],
                "extension_sha256": "a" * 64,
            }
        )
        for claim in doc["claims"]:
            match = claim["classification"] == "match"
            records.append(
                {
                    "record": "case",
                    "claim": claim["id"],
                    "classification": claim["classification"],
                    "server_version": server,
                    "pycubrid": "int(1)" if match else claim["expected"]["pycubrid"],
                    "native": "int(1)" if match else claim["expected"]["native"],
                    "outcome": "match" if match else "classified-deviation",
                }
            )
    return records


def test_committed_ledger_is_consistent_and_non_trivial() -> None:
    assert _errors(DOC) == []
    classes = {c["classification"] for c in DOC["claims"]}
    assert classes == {"match", "deviation"}
    assert gate.check_docs(DOC) == []


def test_every_case_is_claimed_and_skip_reason_matches_lane_audit() -> None:
    assert sorted(CASES) == sorted(c["id"] for c in DOC["claims"])
    assert SKIP_REASON == OFFICIAL_SKIP_REASON


def test_oracle_pins_match_the_build_script() -> None:
    assert DOC["oracle"]["cubrid_python_commit"] == oracle.CUBRID_PYTHON_COMMIT
    assert DOC["oracle"]["cci_commit"] == oracle.CCI_COMMIT


def test_zero_claims_fail() -> None:
    doc = copy.deepcopy(DOC)
    doc["claims"] = []
    assert "the claims ledger has zero claims" in _errors(doc, [])


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda c: c.update(id="Bad_ID"), "kebab-case"),
        (lambda c: c.update(surface="odbc"), "surface must be"),
        (lambda c: c.update(classification="probably"), "classification must be"),
        (lambda c: c.update(summary=" "), "summary is required"),
        (lambda c: c.update(issue="446"), "issue must look like"),
        (lambda c: c.update(inventory_ids=[]), "at least one inventory id"),
        (lambda c: c.update(inventory_ids=["CUBRIDdb.nope"]), "unknown inventory id"),
        (lambda c: c.update(scenario_ids=["tests3/none.py::x"]), "unknown scenario id"),
        (lambda c: c.update(scenario_ids=[]), "needs a scenario_gap"),
        (lambda c: c.update(expected={"pycubrid": "x", "native": "y"}), "must not carry"),
    ],
)
def test_malformed_match_claims_fail(mutate: Any, message: str) -> None:
    doc = copy.deepcopy(DOC)
    mutate(_claim(doc, "fetch-integer"))
    assert any(message in error for error in _errors(doc)), _errors(doc)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda c: c.pop("reason"), "needs a reason"),
        (lambda c: c.pop("expected"), "needs expected pycubrid and native"),
        (lambda c: c["expected"].update(native=""), "needs expected pycubrid and native"),
        (lambda c: c["expected"].update(native=c["expected"]["pycubrid"]), "must differ"),
    ],
)
def test_unclassified_deviations_fail(mutate: Any, message: str) -> None:
    doc = copy.deepcopy(DOC)
    mutate(_claim(doc, "fetch-monetary"))
    assert any(message in error for error in _errors(doc)), _errors(doc)


def test_duplicate_ids_and_case_drift_fail() -> None:
    doc = copy.deepcopy(DOC)
    doc["claims"].append(copy.deepcopy(_claim(doc, "fetch-integer")))
    assert any("duplicate id" in e for e in _errors(doc))
    errors = _errors(DOC, [c for c in CASES if c != "fetch-json"] + ["orphan-case", "orphan-case"])
    assert "claim fetch-json: no differential case" in errors
    assert "case orphan-case: no claim in the ledger" in errors
    assert "case orphan-case: defined more than once" in errors


def test_pin_and_server_drift_fail() -> None:
    doc = copy.deepcopy(DOC)
    doc["oracle"]["cci_commit"] = "0" * 40
    doc["oracle"]["python"] = "3"
    doc["required_servers"] = []
    errors = _errors(doc)
    assert "oracle.cci_commit differs from build_official_oracle.py" in errors
    assert "oracle.python must be a 3.x version" in errors
    assert "required_servers must be a non-empty list" in errors


def test_case_ids_require_a_literal_cases_dict(tmp_path: Path) -> None:
    module = tmp_path / "cases.py"
    module.write_text("CASES = {'a': 1, KEY: 2}\n")
    with pytest.raises(ValueError, match="string literals"):
        gate.case_ids(module)
    module.write_text("OTHER = {}\n")
    with pytest.raises(ValueError, match="no module-level CASES"):
        gate.case_ids(module)
    module.write_text("CASES: dict[str, int] = {'a': 1}\n")
    assert gate.case_ids(module) == ["a"]


def test_generated_docs_are_rewritten_and_stale_blocks_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    docs = {}
    for lang, path in gate.DOCS.items():
        copy_path = tmp_path / f"{lang}.md"
        text = path.read_text(encoding="utf-8")
        copy_path.write_text(text.replace("| 12 |", "| 99 |", 1), encoding="utf-8")
        docs[lang] = copy_path
    monkeypatch.setattr(gate, "DOCS", docs)
    monkeypatch.setattr(gate, "ROOT", tmp_path)
    assert len(gate.check_docs(DOC)) == 2
    assert gate.check_docs(DOC, write=True) == []
    assert gate.check_docs(DOC) == []
    assert "**18**" in docs["en"].read_text(encoding="utf-8")
    docs["ko"].write_text("no markers", encoding="utf-8")
    with pytest.raises(ValueError, match="markers are missing"):
        gate.check_docs(DOC)


def test_rendered_counts_come_from_the_ledger() -> None:
    doc = copy.deepcopy(DOC)
    doc["claims"] = doc["claims"][:2]
    block = gate.render_block(doc, "en")
    assert "| Wrapper (`CUBRIDdb`) | 2 | 0 | 2 |" in block
    assert "| **Total** | **2** | **0** | **2** |" in block
    assert "표면" in gate.render_block(DOC, "ko")


def test_complete_evidence_passes_and_summarizes() -> None:
    summary = gate.check_evidence(DOC, _evidence())
    total = len(DOC["claims"])
    deviations = sum(c["classification"] == "deviation" for c in DOC["claims"])
    assert summary["claims"] == total
    assert summary["results"]["10.2"] == {
        "classified-deviation": deviations,
        "match": total - deviations,
    }
    text = gate.summary_markdown(summary)
    assert "10.2.18.9024" in text and f"{total} claims" in text


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda r: [x for x in r if x["record"] != "case"], "zero differential cases"),
        (
            lambda r: [x for x in r if not str(x["server_version"]).startswith("11.4")],
            "CUBRID 11.4: no official driver run",
        ),
        (
            lambda r: [x for x in r if x.get("claim") != "fetch-json"],
            "fetch-json: no result (skipped or not run)",
        ),
        (lambda r: r + [dict(r[1])], "recorded more than once"),
        (lambda r: r + [dict(r[1], claim="ghost")], "unknown claim ghost"),
        (
            lambda r: [
                dict(x, outcome="mismatch") if x.get("claim") == "fetch-integer" else x for x in r
            ],
            "fetch-integer: recorded outcome 'mismatch'",
        ),
        (
            lambda r: [
                dict(x, outcome="match") if x.get("claim") == "fetch-monetary" else x for x in r
            ],
            "fetch-monetary: recorded outcome 'match'",
        ),
        (
            # A divergent match record cannot certify itself with its own outcome label.
            lambda r: [
                dict(x, native="int(2)") if x.get("claim") == "fetch-integer" else x for x in r
            ],
            "fetch-integer: mismatch",
        ),
        (
            lambda r: [dict(x, native="") if x.get("claim") == "fetch-integer" else x for x in r],
            "fetch-integer: mismatch",
        ),
        (
            lambda r: [
                dict(x, native="int(99)", pycubrid="int(99)")
                if x.get("claim") == "fetch-monetary"
                else x
                for x in r
            ],
            "fetch-monetary: mismatch",
        ),
        (
            lambda r: [
                dict(x, classification="deviation") if x.get("claim") == "fetch-integer" else x
                for x in r
            ],
            "fetch-integer: recorded classification 'deviation' differs",
        ),
        (
            lambda r: [
                dict(x, extension_sha256="") if x["record"] == "environment" else x for x in r
            ],
            "official driver SHA-256 missing",
        ),
        (
            lambda r: [
                dict(x, cci_commit="0" * 40) if x["record"] == "environment" else x for x in r
            ],
            "cci_commit=",
        ),
        (
            lambda r: [dict(x, python="3.12.1") if x["record"] == "environment" else x for x in r],
            "Python 3.12.1",
        ),
    ],
)
def test_incomplete_or_divergent_evidence_fails(mutate: Any, message: str) -> None:
    with pytest.raises(ValueError, match=message.replace("(", r"\(").replace(")", r"\)")):
        gate.check_evidence(DOC, mutate(_evidence()))


def test_main_checks_structure_and_evidence(tmp_path: Path) -> None:
    assert gate.main([]) == 0
    evidence = tmp_path / "evidence.jsonl"
    evidence.write_text("".join(json.dumps(r) + "\n" for r in _evidence()))
    summary_json, summary_md = tmp_path / "summary.json", tmp_path / "summary.md"
    argv = ["--evidence", str(evidence), "--json-out", str(summary_json)]
    assert gate.main(argv + ["--summary-out", str(summary_md)]) == 0
    assert json.loads(summary_json.read_text())["claims"] == len(DOC["claims"])
    assert "Official driver differential evidence" in summary_md.read_text()
    evidence.write_text("{not json\n")
    assert gate.main(["--evidence", str(evidence)]) == 1
    evidence.write_text("")
    assert gate.main(["--evidence", str(evidence)]) == 1


def test_render_tags_types() -> None:
    assert render(1) != render(True)
    assert render((None,)) == "(NoneType(None),)"
    assert render([(1, "a")]) == "[(int(1), str('a'))]"


def _fake_oracle(out: Path) -> dict[str, Any]:
    out.mkdir()
    (out / "CUBRIDdb").mkdir()
    (out / "CUBRIDdb" / "__init__.py").write_text("")
    extension = out / ("_cubrid" + str(sysconfig.get_config_var("EXT_SUFFIX")))
    extension.write_bytes(b"not really an extension")
    manifest = {
        "cubrid_python_commit": oracle.CUBRID_PYTHON_COMMIT,
        "cci_commit": oracle.CCI_COMMIT,
        "python": oracle.platform.python_version(),
        "extension": extension.name,
        "extension_sha256": hashlib.sha256(extension.read_bytes()).hexdigest(),
    }
    (out / oracle.MANIFEST).write_text(json.dumps(manifest))
    return manifest


def test_oracle_verify_accepts_matching_build(tmp_path: Path) -> None:
    manifest = _fake_oracle(tmp_path / "oracle")
    assert oracle.verify(tmp_path / "oracle") == manifest


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("cubrid_python_commit", "0" * 40, "cubrid_python_commit"),
        ("cci_commit", "0" * 40, "cci_commit"),
        ("python", "2.7.18", "built for Python 2.7.18"),
        ("extension", "_cubrid.so", "missing or mismatched"),
        ("extension_sha256", "0" * 64, "SHA-256 does not match"),
    ],
)
def test_oracle_verify_rejects_drift(tmp_path: Path, key: str, value: str, message: str) -> None:
    out = tmp_path / "oracle"
    manifest = _fake_oracle(out)
    manifest[key] = value
    (out / oracle.MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(SystemExit, match=message):
        oracle.verify(out)


def test_oracle_verify_requires_the_wrapper_package(tmp_path: Path) -> None:
    out = tmp_path / "oracle"
    _fake_oracle(out)
    (out / "CUBRIDdb" / "__init__.py").unlink()
    with pytest.raises(SystemExit, match="CUBRIDdb package missing"):
        oracle.verify(out)


def test_oracle_build_refuses_unsupported_platforms(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(oracle.platform, "system", lambda: "Darwin")
    with pytest.raises(SystemExit, match="Linux x86_64 only"):
        oracle.build(tmp_path / "out", tmp_path / "work")
