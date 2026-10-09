#!/usr/bin/env python3
"""Collect sanitized, bounded diagnostics after a bug-hunt failure.

Repeated --junit inputs supply xunit1 failure identities. --server-info reads
the readiness observation, never contacting a broker. Exit zero means diagnostic
collection was attempted, not that a failed or absent test lane passed.
Hypothesis examples are copied as before; arbitrary binary examples are not
claimed to be credential-sanitized. Raw JUnit output is never copied.
"""

from __future__ import annotations

import argparse
import importlib.util
import ipaddress
import json
import os
import platform
import re
import shlex
import shutil
import sys
from pathlib import Path
from urllib.parse import quote, quote_plus, unquote, urlsplit, urlunsplit
from xml.etree import ElementTree  # nosec B405 - bounded UTF-8 JUnit; NUL/DTD/entities rejected

REPRO_DIR = Path("bug-hunt-repro")
HYPOTHESIS_DB = Path(".hypothesis")
_XML_LIMIT = 10 * 1024 * 1024
_DETAIL_LIMIT = 64 * 1024
_BRACKETED_HOST = re.compile(r"\[([^\[\]]+)\](?::[^:\[\]]*)?")
_URL_PASSWORD = re.compile(r"(?<=://)([^\s/@:?#]*:)([^\s/?#]*)(@)")


def _url_passwords(raw_url: str) -> set[str]:
    """Return the configured URL's password, or every fail-closed candidate.

    Only an authority that urllib parsed with its userinfo is trusted. Any
    other URL with an "@" (no "://", a bracket error, "/" or "#" in the
    password) splits its userinfo at the last "@" and treats the text after
    the first, and after the second, ":" as the password. That covers both
    "user:password" and "scheme:user:password". Each text after a "/" in a
    candidate is a candidate too, because urllib reads a password's "/" as the
    start of the path. Candidates may over-redact a password suffix but never
    miss the password itself.
    """
    try:
        parts = urlsplit(raw_url)
        if "@" in parts.netloc:
            return {parts.password} if parts.password else set()
    except ValueError:
        pass
    userinfo, at, _ = raw_url.rpartition("@")
    if not at:
        return set()
    fields = userinfo.split(":", 2)
    candidates = {":".join(fields[index:]) for index in (1, 2) if index < len(fields)}
    for candidate in list(candidates):
        pieces = candidate.split("/")
        candidates.update("/".join(pieces[index:]) for index in range(1, len(pieces)))
    return {candidate for candidate in candidates if candidate}


def sanitize(text: str) -> str:
    """Redact configured raw/encoded passwords and credential-bearing URLs."""
    passwords = {os.environ.get("CUBRID_TEST_PASSWORD", "")}
    for password in _url_passwords(os.environ.get("CUBRID_TEST_URL", "")):
        passwords.update((password, unquote(password)))
    # An environment password is literal, unlike URL userinfo. Keep raw
    # percent characters/case exact; only derived encodings fold hex digits.
    variants = {value: False for value in passwords if value}
    for value in passwords:
        if value:
            for encoded in (quote(value, safe=""), quote_plus(value, safe="")):
                variants[encoded] = True
    for password in sorted(variants, key=len, reverse=True):
        # Percent hex digits may vary in case independently; literal password
        # characters, including Unicode, must not become case-insensitive.
        pattern = (
            re.sub(
                r"%([0-9a-fA-F]{2})",
                lambda match: (
                    "%"
                    + "".join(
                        f"[{char.lower()}{char.upper()}]" if char.isalpha() else char
                        for char in match[1]
                    )
                ),
                re.escape(password),
            )
            if variants[password]
            else re.escape(password)
        )
        text = re.sub(pattern, "***", text)
    # The greedy password group ends at the last @ within this authority only.
    return _URL_PASSWORD.sub(r"\1***\3", text)


def _driver_version() -> str:
    try:
        import pycubrid

        return getattr(pycubrid, "__version__", "unknown")
    except Exception:  # noqa: BLE001 - best-effort collector, never abort
        return "unknown"


def _redact_url(url: str) -> str:
    if not url:
        return ""
    try:
        parts = urlsplit(url)
        port = parts.port  # Validate even when no password was supplied.
        if "[" in parts.netloc or "]" in parts.netloc:
            # Older urllib.parse releases (e.g. 3.11.1) accept any bracketed
            # host and silently drop text around it. Only "[IPv6 literal]",
            # optionally followed by ":port", with no bracket in the userinfo,
            # is parseable on every interpreter.
            userinfo, _, hostport = parts.netloc.rpartition("@")
            literal = _BRACKETED_HOST.fullmatch(hostport)
            if literal is None or "[" in userinfo or "]" in userinfo:
                raise ValueError("bracketed authority is not an IPv6 literal")
            ipaddress.IPv6Address(literal[1])
        if parts.password is None:
            # Without "://" (u:pw@host, cubrid:u:pw@host) urllib finds no
            # userinfo; an "@" outside a parsed authority is never trusted.
            if "@" in url and "@" not in parts.netloc:
                return "<unparseable-url-redacted>"
            return sanitize(url)
        host = parts.hostname or ""
        if ":" in host:
            host = f"[{host}]"
        if port is not None:
            host = f"{host}:{port}"
        userinfo = f"{parts.username}:***@" if parts.username else "***@"
        return sanitize(
            urlunsplit((parts.scheme, userinfo + host, parts.path, parts.query, parts.fragment))
        )
    except ValueError:
        return "<unparseable-url-redacted>"


def _metadata() -> dict[str, str]:
    # Failure context, when the caller exports it (the pytest run can set these
    # from a failure hook); otherwise the CI log holds the failing test id.
    raw = {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "pycubrid_version": _driver_version(),
        "hypothesis_profile": os.environ.get("HYPOTHESIS_PROFILE", "pr"),
        "cubrid_test_host": os.environ.get("CUBRID_TEST_HOST", ""),
        "cubrid_test_port": os.environ.get("CUBRID_TEST_PORT", ""),
        "cubrid_test_db": os.environ.get("CUBRID_TEST_DB", ""),
        "cubrid_test_user": os.environ.get("CUBRID_TEST_USER", ""),
        "cubrid_test_url": _redact_url(os.environ.get("CUBRID_TEST_URL", "")),
        # VERSION=HOST:PORT list of the version differential lane (#351); no credentials.
        "cubrid_version_matrix": os.environ.get("CUBRID_VERSION_MATRIX", ""),
        "failing_test_id": os.environ.get("BUG_HUNT_FAILING_TEST", ""),
        "failure_traceback": os.environ.get("BUG_HUNT_FAILURE_TRACEBACK", ""),
        "github_run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "github_sha": os.environ.get("GITHUB_SHA", ""),
    }
    return {key: sanitize(value) for key, value in raw.items()}


def _safe_file(path: str) -> bool:
    return (
        path.startswith("tests/")
        and path.endswith(".py")
        and all(part not in ("", ".", "..") for part in path.split("/"))
        and not any(char in path for char in ("\\", ":", "\x00", "\n", "\r"))
        and sanitize(path) == path
    )


def _identity(case: ElementTree.Element, kind: str) -> tuple[str, str, str]:
    file = case.get("file", "")
    classname = case.get("classname", "")
    name = case.get("name", "")
    if not _safe_file(file):
        return "", "unresolved", "missing or unsafe file"
    if set(case.attrib) - {"file", "classname", "name", "line", "time"}:
        return "", "unresolved", "custom identity attributes"
    if any(
        sanitize(value) != value or any(c in value for c in "\x00\r\n")
        for value in (classname, name)
    ):
        return "", "unresolved", "redacted or invalid identity"
    module = file[:-3].replace("/", ".")
    if not classname and kind == "error" and name in (module, file):
        return file, "file", "collection error identifies only a file"
    if classname != module and not classname.startswith(module + "."):
        return "", "unresolved", "classname does not match file module"
    classes = classname[len(module) + 1 :].split(".") if classname != module else []
    if not name.split("[", 1)[0].isidentifier() or any(not item.isidentifier() for item in classes):
        return "", "unresolved", "nonstandard test identity"
    return "::".join((file, *classes, name)), "exact", "xunit1 file/module/name"


def _detail(text: str) -> tuple[str, bool]:
    data = sanitize(text).encode("utf-8")
    return data[:_DETAIL_LIMIT].decode("utf-8", errors="ignore"), len(data) > _DETAIL_LIMIT


def _read_report(path: Path) -> tuple[dict[str, object], list[dict[str, object]]]:
    report: dict[str, object] = {"path": sanitize(str(path)), "case_count": 0}
    try:
        with path.open("rb") as stream:
            data = stream.read(_XML_LIMIT + 1)
    except FileNotFoundError:
        report["status"] = "missing"
        return report, []
    except OSError as exc:
        report.update(status="read-error", reason=sanitize(str(exc)))
        return report, []
    if len(data) > _XML_LIMIT:
        report["status"] = "oversized"
        return report, []
    try:
        if b"\x00" in data or b"<!DOCTYPE" in data.upper() or b"<!ENTITY" in data.upper():
            raise ValueError("NUL/DTD/entity input rejected")
        root = ElementTree.fromstring(data.decode("utf-8-sig"))  # nosec B314 - declarations rejected above
        if root.tag not in ("testsuites", "testsuite"):
            raise ValueError("not a JUnit suite")
    except (ValueError, ElementTree.ParseError) as exc:
        report.update(status="malformed", reason=sanitize(str(exc)))
        return report, []
    cases = list(root.iter("testcase"))
    report.update(status="parsed" if cases else "zero-cases", case_count=len(cases))
    failures: list[dict[str, object]] = []
    for case in cases:
        for error in case:
            if error.tag not in ("failure", "error"):
                continue
            node, status, reason = _identity(case, error.tag)
            detail, truncated = _detail("".join(error.itertext()))
            message, _ = _detail(error.get("message", ""))
            failures.append(
                {
                    "report": sanitize(str(path)),
                    "kind": error.tag,
                    "node_id": node,
                    "identity_status": status,
                    "identity_reason": reason,
                    "file": sanitize(case.get("file", "")),
                    "classname": sanitize(case.get("classname", "")),
                    "name": sanitize(case.get("name", "")),
                    "message": message,
                    "detail": detail,
                    "detail_truncated": truncated,
                }
            )
    return report, failures


def _endpoint_fields() -> dict[str, str]:
    # Reuse the exact readiness resolver without importing a foreign tests package.
    spec = importlib.util.spec_from_file_location(
        "_pycubrid_repro_endpoint",
        Path(__file__).resolve().parents[1] / "tests/_cubrid_endpoint.py",
    )
    if spec is None or spec.loader is None:
        raise ValueError("endpoint resolver unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    endpoint = module.resolve_endpoint()
    return {
        "host": endpoint.host,
        "port": str(endpoint.port),
        "db": endpoint.database,
        "user": endpoint.user,
    }


def _server_identity(path: Path | None) -> dict[str, str]:
    unavailable = {"status": "unavailable", "reason": "readiness sidecar missing"}
    if path is None:
        return unavailable
    try:
        with path.open("rb") as stream:
            data = stream.read(_DETAIL_LIMIT + 1)
        if len(data) > _DETAIL_LIMIT:
            raise ValueError("oversized readiness sidecar")
        info = json.loads(data)
        if not isinstance(info, dict):
            raise ValueError("readiness sidecar is not an object")
        if any(
            sanitize(str(info.get(key, ""))) != str(info.get(key, ""))
            for key in ("version", "endpoint", "github_sha")
        ):
            raise ValueError("server identity requires redaction")
        fields = _endpoint_fields()
        endpoint = f"{fields['user']}@{fields['host']}:{fields['port']}/{fields['db']}"
        sha = os.environ.get("GITHUB_SHA", "")
        if not sha or info.get("github_sha") != sha:
            raise ValueError("readiness SHA missing or mismatched")
        if sanitize(endpoint) != endpoint or info.get("endpoint") != endpoint:
            raise ValueError("readiness endpoint mismatched or redacted")
        if info.get("status") != "observed":
            raise ValueError(str(info.get("reason") or "readiness identity unavailable"))
        if (
            not isinstance(info.get("version"), str)
            or not info["version"]
            or not info.get("observed_at")
        ):
            raise ValueError("readiness version/time missing")
        return {
            key: sanitize(str(info[key]))
            for key in ("status", "version", "endpoint", "github_sha", "observed_at")
        }
    except (OSError, ValueError, TypeError, ImportError) as exc:
        return {"status": "unavailable", "reason": sanitize(str(exc))}


def _replay(meta: dict[str, str], targets: list[str]) -> str:
    if not targets:
        return "# Bug-hunt reproduction\n\nPrecise replay unavailable; see metadata.json for diagnostic absence or unresolved identity.\n"
    try:
        fields = _endpoint_fields()
        if any(sanitize(value) != value for value in fields.values()):
            raise ValueError("endpoint fields require redaction")
    except (ValueError, ImportError) as exc:
        return "# Bug-hunt reproduction\n\nPrecise replay unavailable: " + sanitize(str(exc)) + "\n"
    env = [
        ("HYPOTHESIS_PROFILE", meta["hypothesis_profile"] or "pr"),
        ("CUBRID_TEST_HOST", fields["host"]),
        ("CUBRID_TEST_PORT", fields["port"]),
        ("CUBRID_TEST_DB", fields["db"]),
        ("CUBRID_TEST_USER", fields["user"]),
    ]
    if meta["cubrid_version_matrix"]:
        env.append(("CUBRID_VERSION_MATRIX", meta["cubrid_version_matrix"]))
    command = " ".join(f"{key}={shlex.quote(value)}" for key, value in env)
    command += " python -m pytest " + shlex.join([*targets, "-p", "no:cacheprovider"])
    return (
        "# Bug-hunt reproduction\n\n"
        "Restore the saved hypothesis/ database to .hypothesis if present. "
        "Supply CUBRID_TEST_PASSWORD separately; it is not stored here. "
        "JUnit targets are deduplicated failure/error identities. Without JUnit, "
        "a target is a caller-supplied legacy hint, not verified report evidence. "
        "Neither is proof of a passing lane.\n\n"
        f"```bash\n{command}\n```\n"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--junit", action="append", default=[], type=Path)
    parser.add_argument("--server-info", type=Path)
    args = parser.parse_args([] if argv is None else argv)
    legacy = _metadata()
    reports: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []
    for path in args.junit:
        report, records = _read_report(path)
        reports.append(report)
        failures.extend(records)
    targets = list(
        dict.fromkeys(
            str(record["node_id"])
            for record in failures
            if record["identity_status"] in ("exact", "file")
        )
    )
    if not args.junit and legacy["failing_test_id"]:
        hint = os.environ.get("BUG_HUNT_FAILING_TEST", "")
        parts = hint.split("::")
        if (
            sanitize(hint) == hint
            and _safe_file(parts[0])
            and all(part.split("[", 1)[0].isidentifier() for part in parts[1:])
        ):
            targets.append(hint)
    errors: list[str] = []
    meta = {
        **legacy,
        "reports": reports,
        "failures": failures,
        "server_identity": _server_identity(args.server_info),
        "collection_errors": errors,
    }
    try:
        REPRO_DIR.mkdir(exist_ok=True)
    except OSError as exc:
        print(sanitize(f"Bundle unavailable: {exc}"), file=sys.stderr)
        return 0
    if HYPOTHESIS_DB.is_dir():
        try:
            dest = REPRO_DIR / "hypothesis"
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(HYPOTHESIS_DB, dest)
        except OSError as exc:
            errors.append(sanitize(f"Hypothesis copy unavailable: {exc}"))
    for name, content in (
        ("metadata.json", json.dumps(meta, indent=2) + "\n"),
        ("reproduce.md", _replay(legacy, targets)),
    ):
        try:
            (REPRO_DIR / name).write_text(content, encoding="utf-8")
        except OSError as exc:
            print(sanitize(f"{name} unavailable: {exc}"), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
