#!/usr/bin/env python3
"""Write ``test-results/<lane>/summary.json`` and ``environment.json`` (#1024).

``make test`` and ``make integration`` write their pytest JUnit XML (and, for
``make test``, ``coverage.xml``) under ``test-results/<lane>/``. This helper turns
those files into one machine-readable ``summary.json`` and prints the same status and
counts as a single console line. It never changes a command's exit status: callers
keep the status pytest returned and pass it in with ``--exit-code``.

Status:

- ``FAIL``: a test failed or errored, or pytest/the run exited non-zero (a coverage
  floor miss, a service that never started, a missing JUnit file after a failure).
- ``INCOMPLETE``: nothing proves the lane ran: no JUnit file, nothing collected, or no
  test passed (every selected test skipped or xfailed). Never ``PASS``.
- ``PASS``: at least one test passed, none failed or errored, and the exit status is 0.

JUnit XML cannot tell an unexpected pass of a non-strict xfail from an ordinary pass,
so ``xpassed`` is always 0 (a strict xpass is reported as a failure by pytest).

``environment.json`` records the Python, package and CUBRID versions, the platform and
the command. It is built from those values only, never from the process environment,
so an exported ``CUBRID_PASSWORD`` or other secret variable never reaches it. The
command is caller text, so ``redact_command`` masks the credentials it can recognize
before it is saved: the password of an authenticated URL (``scheme://user:***@host``)
and the value of a ``password``/``passwd``/``pwd``/``token``/``secret``/``api_key``
key (``--password=***``, ``--password ***``, ``CUBRID_PASSWORD=***``). This is a
best-effort pattern match, not a secret scanner: a credential in any other form (a
positional argument, a header, a short flag such as ``-p``, a URL password containing an
unencoded ``/``, ``?``, ``#``, quote or space) is saved as given, so never
put real credentials on a command line; pass them through the environment instead.

``aggregate`` writes ``make verify``'s ``test-results/verify/`` (#1053; see
``scripts/run_verify.py``): ``summary.json`` with one entry per phase, in the order given
(``--phase NAME=EXIT``, ``NAME=not-run``, optionally ``:SUMMARY`` naming the lane's own
``summary.json``), ``junit/<lane>.xml`` copies (``--copy-junit NAME=PATH``) and a copy of the
live lane's ``environment.json`` (``--environment PATH``). A phase is ``PASS`` only when it
exited 0 and its lane summary, when it has one, is readable and ``PASS``; one that never
started is ``NOT_RUN``. The overall status is ``FAIL`` when a phase failed, ``INCOMPLETE``
when a phase did not run or proved nothing, a lane summary is missing or unreadable, or a
result file could not be copied, and ``PASS`` otherwise. Every copy has the configured
passwords (``SECRET_VARIABLES`` and the URL passwords of ``URL_VARIABLES``, also in their
percent-encoded, XML- and JSON-escaped forms) replaced with ``***`` in the parsed text,
attribute and string values, so the copies stay valid XML/JSON.

Usage::

    python3 scripts/report_results.py summarize --lane test --results-dir test-results/test \
        --exit-code 0 [--coverage test-results/test/coverage.xml]
    python3 scripts/report_results.py step-summary --results-dir test-results/test \
        --title "Unit tests (Python 3.12)" >> "$GITHUB_STEP_SUMMARY"
    python3 scripts/report_results.py environment --lane integration \
        --results-dir test-results/integration --command "..." --cubrid-version 11.2
    python3 scripts/report_results.py aggregate --results-dir test-results/verify \
        --phase check=0 --phase test=1:test-results/test/summary.json \
        --phase integration=not-run:test-results/integration/summary.json \
        --copy-junit test=test-results/test/junit.xml
"""

from __future__ import annotations

import argparse
import contextlib
import html
import json
import os
import platform
import re
import urllib.parse
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET  # nosec B405 - local reports only; DTD/entities rejected below

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = 1
COUNTS = ("selected", "passed", "failed", "errors", "skipped", "xfailed", "xpassed")


def _outcome(case: ET.Element) -> str:
    if case.find("error") is not None:
        return "errors"
    if case.find("failure") is not None:
        return "failed"
    if (skipped := case.find("skipped")) is not None:
        return "xfailed" if skipped.get("type") == "pytest.xfail" else "skipped"
    return "passed"


# When a test fails and its teardown also errors, pytest writes two <testcase>
# records for it; count each test once, with its worst outcome.
_SEVERITY = {"passed": 0, "xfailed": 1, "skipped": 2, "failed": 3, "errors": 4}


def _load_xml(path: Path) -> ET.Element:
    """Parse a local report; DTD and entity declarations are refused (as in check_integration_lanes)."""
    payload = path.read_text(encoding="utf-8-sig")
    if "\x00" in payload or "<!DOCTYPE" in payload.upper() or "<!ENTITY" in payload.upper():
        raise ET.ParseError(f"{path}: DTD/entity declarations are forbidden")
    return ET.fromstring(payload)  # nosec B314 - DTD/entity declarations rejected above


def parse_junit(path: Path) -> dict[str, int] | None:
    """Count the tests of a JUnit file; None if it is missing or unreadable."""
    try:
        root = _load_xml(path)
    except (OSError, ValueError, ET.ParseError):
        return None
    outcomes: dict[tuple[str, str], str] = {}
    for case in root.iter("testcase"):
        key = (case.get("classname", ""), case.get("name", ""))
        outcome = _outcome(case)
        if key not in outcomes or _SEVERITY[outcome] > _SEVERITY[outcomes[key]]:
            outcomes[key] = outcome
    counts = dict.fromkeys(COUNTS, 0)
    counts["selected"] = len(outcomes)
    for outcome in outcomes.values():
        counts[outcome] += 1
    return counts


def parse_coverage(path: Path) -> float | None:
    """Line coverage percent from a Cobertura ``coverage.xml``; None if unavailable."""
    try:
        rate = _load_xml(path).get("line-rate")
        return None if rate is None else round(float(rate) * 100, 2)
    except (OSError, ET.ParseError, ValueError):
        return None


def decide(counts: dict[str, int] | None, exit_code: int) -> str:
    if counts is None:
        return "FAIL" if exit_code else "INCOMPLETE"
    if counts["failed"] or counts["errors"]:
        return "FAIL"
    if counts["passed"] == 0:
        return "INCOMPLETE"
    return "FAIL" if exit_code else "PASS"


def build_summary(
    lane: str, junit: Path, coverage: Path | None, exit_code: int, reason: str = ""
) -> dict[str, Any]:
    counts = parse_junit(junit)
    summary: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "lane": lane,
        "status": decide(counts, exit_code),
        "exit_code": exit_code,
        "counts": counts if counts is not None else dict.fromkeys(COUNTS, 0),
    }
    if reason:
        summary["reason"] = reason
    if coverage is not None:
        percent = parse_coverage(coverage)
        if percent is not None:
            summary["coverage"] = {"line_percent": percent}
    return summary


def console_line(summary: dict[str, Any]) -> str:
    if "phases" in summary:
        return aggregate_line(summary)
    counts = summary["counts"]
    parts = [f"{name}={counts[name]}" for name in COUNTS]
    if "coverage" in summary:
        parts.append(f"coverage={summary['coverage']['line_percent']:.2f}%")
    return f"{summary['lane']}: {summary['status']} " + " ".join(parts)


def step_summary(results: Path, title: str | None) -> str:
    """Markdown for ``$GITHUB_STEP_SUMMARY``: the console line of ``summary.json``."""
    heading = f"### {title or 'Test results'}\n\n"
    try:
        summary = json.loads((results / "summary.json").read_text())
        line = console_line(summary)
    except (OSError, ValueError, KeyError, TypeError):
        return f"{heading}No readable `{results / 'summary.json'}` was written (the run stopped early).\n"
    return f"{heading}`{line}`\n"


def package_version() -> str:
    text = (ROOT / "pycubrid" / "__init__.py").read_text()
    match = re.search(r'^__version__ = "([^"]+)"', text, re.M)
    return match.group(1) if match else "unknown"


# Credential masking for the saved command. Plain regexes rather than shlex keep the
# rest of the command byte-identical (quoting, spacing, unparsable shell text).
_MASK = "***"
# scheme://user:SECRET@host -> the password ends at the last "@" before the path, query,
# fragment, whitespace or a quote, so an unescaped "@" in it is masked too; a
# percent-encoded one is ordinary text. A URL without a password (user@host,
# host:port) does not match.
_URL_PASSWORD = re.compile(
    r"(?P<prefix>(?<![A-Za-z0-9+.-])[A-Za-z][A-Za-z0-9+.-]*://[^\s:/?#@'\"]*:)[^\s/?#'\"]+(?=@)"
)
# A key whose name ends with a credential word: CUBRID_PASSWORD, --db-password, api-key.
# The word must end the key, so --password-file, --tokenizer and password_test do not match.
_SECRET_KEY = r"[A-Za-z0-9_.-]*?(?:password|passwd|pwd|token|secret|api[_-]?key)"
# One shell word: quoted chunks or characters that are not unquoted shell metacharacters.
_VALUE = r"(?:\"(?:[^\"\\]|\\.)*\"|'[^']*'|[^\s'\"&;|<>()])+"
_ASSIGNMENT = re.compile(
    # KEY=value / --key=value (also a URL query such as ?password=value) ...
    rf"(?<![\w.-])(?P<key>-{{0,2}}{_SECRET_KEY})=(?P<value>{_VALUE})"
    # ... or --key value, only after a dashed option and never taking the next option.
    rf"|(?<![\w.-])(?P<flag>--?{_SECRET_KEY})(?P<space>\s+)(?!-)(?P<flag_value>{_VALUE})",
    re.IGNORECASE,
)


def _mask_assignment(match: re.Match[str]) -> str:
    if match.group("key") is not None:
        head, value = f"{match.group('key')}=", match.group("value")
    else:
        head, value = match.group("flag") + match.group("space"), match.group("flag_value")
    quote = value[0] if value[0] in "'\"" else ""
    return f"{head}{quote}{_MASK}{quote}"


def redact_command(command: str) -> str:
    """Mask URL passwords and credential-key values; any other text is returned unchanged."""
    if "://" in command:
        command = _URL_PASSWORD.sub(rf"\g<prefix>{_MASK}", command)
    return _ASSIGNMENT.sub(_mask_assignment, command)


def build_environment(lane: str, command: str, cubrid_version: str | None) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "lane": lane,
        "command": redact_command(command),
        "python": platform.python_version(),
        "package": {"name": "pycubrid", "version": package_version()},
        "cubrid_version": cubrid_version,
        "platform": platform.platform(),
    }


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


# make verify's aggregate (#1053). Variables that can carry a database password into a
# result file: the test endpoint (tests/_cubrid_endpoint.py), the TLS lane and the
# conventional CUBRID_PASSWORD. Each name also matches _SECRET_KEY, so redact_command masks
# it on a command line too (tests/test_report_results_aggregate.py keeps both in step).
SECRET_VARIABLES = ("CUBRID_PASSWORD", "CUBRID_TEST_PASSWORD", "CUBRID_TLS_TEST_PASSWORD")
URL_VARIABLES = ("CUBRID_TEST_URL",)
LANE_STATUSES = ("PASS", "FAIL", "INCOMPLETE")


def configured_secrets(environ: Mapping[str, str]) -> list[str]:
    """The configured passwords in every form a result file may hold, longest first."""
    raw = [environ.get(name, "") for name in SECRET_VARIABLES]
    for name in URL_VARIABLES:
        try:
            raw.append(urllib.parse.urlsplit(environ.get(name, "")).password or "")
        except ValueError:
            continue
    forms: set[str] = set()
    for secret in filter(None, raw):
        for form in {secret, urllib.parse.unquote(secret), urllib.parse.quote(secret, safe="")}:
            forms.update(
                (
                    form,
                    html.escape(form),
                    html.escape(form, quote=False),
                    # ElementTree escapes '"' (not "'") in attributes and neither in text.
                    form.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace('"', "&quot;"),
                    json.dumps(form)[1:-1],
                )
            )
    return sorted(filter(None, forms), key=len, reverse=True)


def scrub(data: bytes, secrets: Sequence[str]) -> bytes:
    """Replace every occurrence of *secrets* in *data* with ``***``."""
    for secret in secrets:
        data = data.replace(secret.encode(), _MASK.encode())
    return data


def _mask_text(text: str, secrets: Sequence[str]) -> str:
    for secret in secrets:
        text = text.replace(secret, _MASK)
    return text


def _mask_json(value: Any, secrets: Sequence[str]) -> Any:
    if isinstance(value, str):
        return _mask_text(value, secrets)
    if isinstance(value, list):
        return [_mask_json(item, secrets) for item in value]
    if isinstance(value, dict):
        return {key: _mask_json(item, secrets) for key, item in value.items()}
    return value


def mask_copy(source: Path, secrets: Sequence[str]) -> bytes:
    """The masked bytes of a JUnit or JSON result file.

    A file that parses is masked in its text, attribute and string values and serialized
    again, so masking never touches markup or JSON syntax (a password such as ``test``
    must not turn ``<testsuite>`` into ``<***suite>``). A file that does not parse is
    masked byte for byte in every escaped form.
    """
    data = source.read_bytes()
    if not secrets:
        return data
    try:
        if source.suffix == ".json":
            document = _mask_json(json.loads(data.decode("utf-8")), secrets)
            return (json.dumps(document, indent=2) + "\n").encode()
        root = _load_xml(source)
    except (UnicodeDecodeError, ValueError, ET.ParseError):
        return scrub(data, secrets)
    for element in root.iter():
        if element.text:
            element.text = _mask_text(element.text, secrets)
        if element.tail:
            element.tail = _mask_text(element.tail, secrets)
        for key, value in element.attrib.items():
            element.set(key, _mask_text(value, secrets))
    return bytes(ET.tostring(root, encoding="utf-8", xml_declaration=True))


def _lane_status(path: Path) -> tuple[str | None, str]:
    """The ``status`` of a lane's ``summary.json``, or None and why it is unusable."""
    try:
        status = json.loads(path.read_text(encoding="utf-8"))["status"]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return None, f"cannot read {path}: {exc}"
    if not isinstance(status, str) or status not in LANE_STATUSES:
        return None, f"{path} has an unknown status {status!r}"
    return status, ""


Phase = tuple[str, int | None, Path | None]


def build_aggregate(phases: Sequence[Phase], problems: Sequence[str] = ()) -> dict[str, Any]:
    """``make verify``'s summary; *problems* (e.g. a failed copy) keep it from ``PASS``."""
    found = list(problems)
    entries: list[dict[str, Any]] = []
    for name, exit_code, summary_path in phases:
        lane_status: str | None = None
        if exit_code is None:
            status = "NOT_RUN"
            found.append(f"{name} did not run")
        else:
            if summary_path is not None:
                lane_status, problem = _lane_status(summary_path)
                if problem:
                    found.append(f"{name}: {problem}")
            if exit_code != 0 or lane_status == "FAIL":
                status = "FAIL"
            elif summary_path is not None and lane_status != "PASS":
                status = "INCOMPLETE"
                if lane_status == "INCOMPLETE":
                    found.append(f"{name} is INCOMPLETE")
            else:
                status = "PASS"
        entries.append(
            {
                "name": name,
                "status": status,
                "exit_code": exit_code,
                "summary": None if summary_path is None else str(summary_path),
                "summary_status": lane_status,
            }
        )
    statuses = {entry["status"] for entry in entries}
    if not entries:
        overall = "INCOMPLETE"
        found.append("no phase was given")
    elif "FAIL" in statuses:
        overall = "FAIL"
    elif statuses == {"PASS"} and not problems:
        overall = "PASS"
    else:
        overall = "INCOMPLETE"
    return {
        "schema_version": SCHEMA_VERSION,
        "lane": "verify",
        "status": overall,
        "phases": entries,
        "problems": found,
    }


def aggregate_line(summary: dict[str, Any]) -> str:
    parts = [f"{phase['name']}={phase['status']}" for phase in summary["phases"]]
    line = f"{summary['lane']}: {summary['status']} " + " ".join(parts)
    if summary["problems"]:
        line += " [" + "; ".join(str(problem) for problem in summary["problems"]) + "]"
    return line


def aggregate(
    results: Path,
    phases: Sequence[Phase],
    junit: Sequence[tuple[str, Path]] = (),
    environment: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Write ``summary.json`` and the masked copies under *results*; print the console line.

    A source that does not exist is not copied (the phase summary already says why); one
    that exists but cannot be copied is a problem that keeps the summary from ``PASS``.
    """
    results.mkdir(parents=True, exist_ok=True)
    secrets = configured_secrets(os.environ if environ is None else environ)
    copies = [(source, results / "junit" / f"{name}.xml") for name, source in junit]
    if environment is not None:
        copies.append((environment, results / "environment.json"))
    problems = []
    for source, target in copies:
        if not source.exists():
            continue
        try:
            target.parent.mkdir(exist_ok=True)
            target.write_bytes(mask_copy(source, secrets))
        except OSError as exc:
            with contextlib.suppress(OSError):  # e.g. a partial copy; never a stale one
                target.unlink(missing_ok=True)
            problems.append(f"could not copy {source}: {exc}")
    summary = build_aggregate(phases, problems)
    _write_json(results / "summary.json", summary)
    print(f"{aggregate_line(summary)} -> {results / 'summary.json'}", flush=True)
    return summary


def _phase_arg(text: str) -> Phase:
    name, sep, rest = text.partition("=")
    if not name or not sep:
        raise argparse.ArgumentTypeError(f"expected NAME=EXIT[:SUMMARY], got {text!r}")
    code, _, path = rest.partition(":")
    exit_code: int | None = None
    if code != "not-run":
        try:
            exit_code = int(code)
        except ValueError:
            raise argparse.ArgumentTypeError(f"bad exit status in {text!r}") from None
    return name, exit_code, Path(path) if path else None


def _pair_arg(text: str) -> tuple[str, Path]:
    name, sep, path = text.partition("=")
    if not name or not sep or not path:
        raise argparse.ArgumentTypeError(f"expected NAME=PATH, got {text!r}")
    return name, Path(path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n", 1)[0])
    sub = parser.add_subparsers(dest="action", required=True)
    summarize = sub.add_parser("summarize", help="write summary.json and print one line")
    environment = sub.add_parser("environment", help="write environment.json")
    step = sub.add_parser("step-summary", help="print summary.json as Markdown")
    agg = sub.add_parser("aggregate", help="write make verify's summary.json and copies")
    agg.add_argument("--results-dir", required=True, type=Path)
    agg.add_argument("--phase", action="append", default=[], type=_phase_arg)
    agg.add_argument("--copy-junit", action="append", default=[], type=_pair_arg)
    agg.add_argument("--environment", type=Path, help="the live lane's environment.json")
    step.add_argument("--results-dir", required=True, type=Path)
    step.add_argument("--title", help="heading, e.g. the matrix entry")
    for p in (summarize, environment):
        p.add_argument("--lane", required=True)
        p.add_argument("--results-dir", required=True, type=Path)
    summarize.add_argument("--exit-code", required=True, type=int)
    summarize.add_argument("--junit", type=Path, help="default: <results-dir>/junit.xml")
    summarize.add_argument("--reason", default="", help="why the run stopped early")
    summarize.add_argument("--coverage", type=Path, help="coverage.xml, when measured")
    environment.add_argument("--command", required=True)
    environment.add_argument("--cubrid-version")
    args = parser.parse_args(argv)

    results: Path = args.results_dir
    if args.action == "step-summary":
        print(step_summary(results, args.title), end="")
        return 0
    if args.action == "aggregate":
        aggregate(results, args.phase, args.copy_junit, args.environment)
        return 0
    if args.action == "environment":
        data = build_environment(args.lane, args.command, args.cubrid_version)
        _write_json(results / "environment.json", data)
        return 0
    summary = build_summary(
        args.lane,
        args.junit or results / "junit.xml",
        args.coverage,
        args.exit_code,
        args.reason,
    )
    _write_json(results / "summary.json", summary)
    print(f"{console_line(summary)} -> {results / 'summary.json'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
