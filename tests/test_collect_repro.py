"""Offline checks for the bug-hunt repro bundle (issues #359, #351)."""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from urllib.parse import SplitResult, quote
from xml.etree import ElementTree

import pytest

from scripts import collect_repro

pytestmark = pytest.mark.repo_tooling

MATRIX = "10.2=localhost:33102,11.4=localhost:33114"


@pytest.fixture
def bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(collect_repro, "REPRO_DIR", tmp_path / "bug-hunt-repro")
    monkeypatch.setattr(collect_repro, "HYPOTHESIS_DB", tmp_path / ".hypothesis")
    return tmp_path / "bug-hunt-repro"


def test_version_matrix_is_recorded_in_metadata(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CUBRID_VERSION_MATRIX", MATRIX)
    result = collect_repro.main()
    assert result == 0
    meta = json.loads((bundle / "metadata.json").read_text())
    assert meta["cubrid_version_matrix"] == MATRIX


def test_version_matrix_is_part_of_the_reproduce_command(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CUBRID_VERSION_MATRIX", MATRIX)
    monkeypatch.setenv("BUG_HUNT_FAILING_TEST", "tests/test_protocol.py::test_example")
    collect_repro.main()
    tokens = _replay_tokens(bundle)
    assert f"CUBRID_VERSION_MATRIX={MATRIX}" in tokens


def test_reproduce_command_omits_an_unset_version_matrix(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CUBRID_VERSION_MATRIX", raising=False)
    collect_repro.main()
    assert "CUBRID_VERSION_MATRIX" not in (bundle / "reproduce.md").read_text()


def _replay_tokens(bundle: Path) -> list[str]:
    content = (bundle / "reproduce.md").read_text().replace("\\\n", " ")
    command = next(line for line in content.splitlines() if "python -m pytest " in line)
    return shlex.split(command)


def _collect(bundle: Path, *args: str) -> dict:
    result = collect_repro.main(list(args))
    assert result == 0
    return json.loads((bundle / "metadata.json").read_text())


def test_junit_preserves_failure_error_parameter_and_file_identity(bundle: Path) -> None:
    report = bundle.parent / "normal.xml"
    report.write_text(
        '<testsuites><testsuite tests="4"><testcase file="tests/test_example.py" '
        'classname="tests.test_example.TestGroup" name="test_value[a.b-x]">'
        '<failure message="bad value">Falsifying example: value=7</failure></testcase>'
        '<testcase file="tests/test_example.py" classname="tests.test_example" '
        'name="test_setup"><error message="setup failed">trace</error></testcase>'
        '<testcase file="tests/test_broken.py" name="tests.test_broken">'
        '<error message="collection failed">import failure</error></testcase>'
        '<testcase name="skip"><skipped/></testcase></testsuite></testsuites>'
    )
    meta = _collect(bundle, "--junit", str(report), "--junit", str(report))
    assert meta["reports"][0]["status"] == "parsed"
    assert meta["reports"][0]["case_count"] == 4
    failures = meta["failures"]
    assert {record["kind"] for record in failures} == {"failure", "error"}
    assert {(record["node_id"], record["identity_status"]) for record in failures} == {
        ("tests/test_example.py::TestGroup::test_value[a.b-x]", "exact"),
        ("tests/test_example.py::test_setup", "exact"),
        ("tests/test_broken.py", "file"),
    }
    assert any("Falsifying example" in record["detail"] for record in failures)
    tokens = _replay_tokens(bundle)
    assert tokens.count("tests/test_example.py::TestGroup::test_value[a.b-x]") == 1
    assert "-m" not in tokens[tokens.index("pytest") + 1 :]


@pytest.mark.parametrize(
    ("attributes", "reason"),
    [
        ('classname="tests.test_example" name="test_x"', "missing file"),
        ('file="../tests/test_example.py" classname="tests.test_example" name="test_x"', "path"),
        ('file="tests/test_example.py" classname="tests.other" name="test_x"', "module"),
        ('file="tests/test_example.py" classname="tests.test_example..Bad" name="test_x"', "class"),
        (
            'file="tests/test_example.py" classname="tests.test_example" name="test_x" nodeid="custom"',
            "custom",
        ),
    ],
)
def test_unproved_junit_identity_is_not_guessed(bundle: Path, attributes: str, reason: str) -> None:
    report = bundle.parent / "unresolved.xml"
    report.write_text(
        f"<testsuite><testcase {attributes}><failure>bad</failure></testcase></testsuite>"
    )
    meta = _collect(bundle, "--junit", str(report))
    record = meta["failures"][0]
    assert record["identity_status"] == "unresolved"
    assert record["identity_reason"]
    replay = (bundle / "reproduce.md").read_text()
    assert "python -m pytest tests/" not in replay
    assert "unavailable" in replay.lower(), reason


@pytest.mark.parametrize(
    ("content", "status"),
    [
        (None, "missing"),
        ("<bad", "malformed"),
        ("<testsuite/>", "zero-cases"),
        ('<!DOCTYPE x [<!ENTITY x "secret">]><testsuite/>', "malformed"),
        ("<testsuite>\x00</testsuite>", "malformed"),
        (" " * (10 * 1024 * 1024 + 1), "oversized"),
    ],
    ids=["missing", "malformed", "zero-cases", "dtd", "nul", "over-cap"],
)
def test_absent_or_invalid_report_never_claims_positive_proof(
    bundle: Path, content: str | None, status: str
) -> None:
    report = bundle.parent / "invalid.xml"
    if content is not None:
        report.write_text(content)
    meta = _collect(bundle, "--junit", str(report))
    assert meta["reports"][0]["status"] == status
    assert meta["failures"] == []


def test_secrets_are_redacted_before_detail_truncation(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "private:p@ss-word"
    encoded = quote(secret, safe="")
    encoded_lower = encoded.replace("%3A", "%3a")
    monkeypatch.setenv("CUBRID_TEST_PASSWORD", secret)
    monkeypatch.setenv("CUBRID_TEST_URL", f"cubrid://app:{encoded}@h:1/db")
    monkeypatch.setenv("BUG_HUNT_FAILURE_TRACEBACK", f"password {secret} {encoded}")
    report = bundle.parent / "secret.xml"
    suite = ElementTree.Element("testsuite")
    case = ElementTree.SubElement(
        suite,
        "testcase",
        file="tests/test_example.py",
        classname="tests.test_example",
        name="test_x",
    )
    failure = ElementTree.SubElement(
        case, "failure", message=f"credential {encoded} {encoded_lower}"
    )
    failure.text = "x" * (64 * 1024 - 4) + secret + f" cubrid://u:other-password@h:1/db {encoded}"
    ElementTree.ElementTree(suite).write(report, encoding="utf-8")
    meta = _collect(bundle, "--junit", str(report))
    persisted = (bundle / "metadata.json").read_text() + (bundle / "reproduce.md").read_text()
    assert secret not in persisted
    assert encoded not in persisted
    assert encoded_lower not in persisted
    assert "other-password" not in persisted
    record = meta["failures"][0]
    assert record["detail_truncated"] is True
    assert len(record["detail"]) <= 64 * 1024
    assert not record["detail"].endswith(secret[:4])
    assert not (bundle / report.name).exists()


def test_replay_quotes_each_environment_assignment_and_parameterized_target(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    host = "broker'; touch /tmp/not-executed; #"
    node = "tests/test_example.py::test_value[a b;$(false)]"
    monkeypatch.setenv("CUBRID_TEST_HOST", host)
    monkeypatch.setenv("HYPOTHESIS_PROFILE", "wide profile")
    report = bundle.parent / "quote.xml"
    suite = ElementTree.Element("testsuite")
    case = ElementTree.SubElement(
        suite,
        "testcase",
        file="tests/test_example.py",
        classname="tests.test_example",
        name="test_value[a b;$(false)]",
    )
    ElementTree.SubElement(case, "failure").text = "failed"
    ElementTree.ElementTree(suite).write(report)
    _collect(bundle, "--junit", str(report))
    tokens = _replay_tokens(bundle)
    assert f"CUBRID_TEST_HOST={host}" in tokens
    assert "HYPOTHESIS_PROFILE=wide profile" in tokens
    assert node in tokens
    assert tokens[tokens.index("pytest") + 1] == node


def test_detail_limit_is_utf8_bytes_not_unicode_character_count(bundle: Path) -> None:
    report = bundle.parent / "unicode.xml"
    report.write_text(
        '<testsuite><testcase file="tests/test_example.py" classname="tests.test_example" '
        'name="test_x"><failure>' + "한" * 30000 + "</failure></testcase></testsuite>",
        encoding="utf-8",
    )
    meta = _collect(bundle, "--junit", str(report))
    record = meta["failures"][0]
    assert len(record["detail"].encode("utf-8")) <= 64 * 1024
    assert record["detail_truncated"] is True


@pytest.mark.parametrize("scheme", ["cubrid", "https"])
def test_unconfigured_url_password_with_at_sign_cannot_leak_its_suffix(
    bundle: Path, monkeypatch: pytest.MonkeyPatch, scheme: str
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    monkeypatch.delenv("CUBRID_TEST_URL", raising=False)
    url = f"{scheme}://u:p@ss@host/testdb?query=retained@value#fragment=kept@text"
    monkeypatch.setenv("BUG_HUNT_FAILURE_TRACEBACK", f"URL {url}")
    monkeypatch.setenv("CUBRID_VERSION_MATRIX", url)
    report = bundle.parent / "authority.xml"
    report.write_text(
        '<testsuite><testcase file="tests/test_example.py" classname="tests.test_example" '
        f'name="test_x"><failure message="{url}">URL {url}</failure></testcase></testsuite>'
    )
    meta = _collect(bundle, "--junit", str(report))
    persisted = (bundle / "metadata.json").read_text() + (bundle / "reproduce.md").read_text()
    assert "ss@host" not in persisted
    redacted = f"{scheme}://u:***@host/testdb?query=retained@value#fragment=kept@text"
    assert meta["failure_traceback"] == f"URL {redacted}"
    assert meta["failures"][0]["message"] == redacted
    assert meta["failures"][0]["detail"] == f"URL {redacted}"
    assert redacted in (bundle / "reproduce.md").read_text()


@pytest.mark.parametrize("encoded", ["%C3%a9", "%c3%A9"])
def test_mixed_percent_hex_credentials_are_redacted_without_folding_literal_case(
    bundle: Path, monkeypatch: pytest.MonkeyPatch, encoded: str
) -> None:
    monkeypatch.setenv("CUBRID_TEST_PASSWORD", "é")
    monkeypatch.delenv("CUBRID_TEST_URL", raising=False)
    monkeypatch.setenv("BUG_HUNT_FAILURE_TRACEBACK", f"encoded {encoded}; literal É")
    monkeypatch.setenv("CUBRID_VERSION_MATRIX", encoded)
    report = bundle.parent / "percent.xml"
    report.write_text(
        '<testsuite><testcase file="tests/test_example.py" classname="tests.test_example" '
        f'name="test_x"><failure message="{encoded}">{encoded}</failure></testcase></testsuite>',
        encoding="utf-8",
    )
    meta = _collect(bundle, "--junit", str(report))
    persisted = (bundle / "metadata.json").read_text() + (bundle / "reproduce.md").read_text()
    assert encoded not in persisted
    assert meta["failures"][0]["message"] == "***"
    assert meta["failures"][0]["detail"] == "***"
    assert meta["failure_traceback"].endswith("literal É")


@pytest.mark.parametrize("scheme", ["cubrid", "https"])
def test_password_only_authority_is_redacted(
    bundle: Path, monkeypatch: pytest.MonkeyPatch, scheme: str
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    monkeypatch.delenv("CUBRID_TEST_URL", raising=False)
    monkeypatch.setenv("BUG_HUNT_FAILURE_TRACEBACK", f"{scheme}://:private-password@host/db")
    result = collect_repro.main()
    assert result == 0
    assert "private-password" not in (bundle / "metadata.json").read_text()


@pytest.mark.parametrize(
    ("password", "text", "expected"),
    [
        (
            "%41Af",
            "raw=%41Af other=%41af encoded=%2541Af decoded=AAf",
            "raw=*** other=%41af encoded=*** decoded=AAf",
        ),
        (
            "%4A",
            "raw=%4A other=%4a encoded=%254A decoded=J",
            "raw=*** other=%4a encoded=*** decoded=J",
        ),
    ],
)
def test_literal_percent_password_keeps_case_and_is_not_url_decoded(
    monkeypatch: pytest.MonkeyPatch,
    password: str,
    text: str,
    expected: str,
) -> None:
    monkeypatch.setenv("CUBRID_TEST_PASSWORD", password)
    monkeypatch.delenv("CUBRID_TEST_URL", raising=False)
    result = collect_repro.sanitize(text)
    assert result == expected


def test_url_only_malformed_authority_still_redacts_standalone_credential_hints(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    monkeypatch.setenv("CUBRID_TEST_URL", "cubrid://user:syntheticsecret@[bad]/db")
    monkeypatch.setenv("BUG_HUNT_FAILING_TEST", "tests/test_example.py::test_x[syntheticsecret]")
    monkeypatch.setenv("BUG_HUNT_FAILURE_TRACEBACK", "standalone credential syntheticsecret")
    report = bundle.parent / "malformed-authority.xml"
    report.write_text(
        '<testsuite><testcase file="tests/test_example.py" classname="tests.test_example" '
        'name="test_x[syntheticsecret]"><failure message="syntheticsecret">'
        "standalone credential syntheticsecret</failure></testcase></testsuite>"
    )
    meta = _collect(bundle, "--junit", str(report))
    persisted = (bundle / "metadata.json").read_text() + (bundle / "reproduce.md").read_text()
    assert "syntheticsecret" not in persisted
    assert meta["failures"][0]["identity_status"] == "unresolved"
    assert meta["cubrid_test_url"] == "<unparseable-url-redacted>"
    assert "unavailable" in (bundle / "reproduce.md").read_text().lower()


@pytest.mark.parametrize(
    "url",
    [
        "cubrid://user:syntheticsecret@[bad]/db",
        "cubrid://user:syntheticsecret@[bad]:33000/db",
        "cubrid://user@[bad]/db",
        "cubrid://user:syntheticsecret@[127.0.0.1]/db",
        "cubrid://user:syntheticsecret@[]/db",
        # Text around the brackets, or brackets in the userinfo: 3.11.1 drops
        # or tolerates them while later releases reject the URL.
        "cubrid://user:syntheticsecret@x[::1]/db",
        "cubrid://user:syntheticsecret@[::1]x/db",
        "cubrid://user[x]:syntheticsecret@host/db",
        "cubrid://user:synthetic[s]ecret@host/db",
    ],
)
def test_bracketed_non_ipv6_host_is_unparseable_on_every_interpreter(
    monkeypatch: pytest.MonkeyPatch, url: str
) -> None:
    # Early 3.11 patch releases accept any bracketed host; the script decides itself.
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    monkeypatch.setenv("CUBRID_TEST_URL", url)
    assert collect_repro._redact_url(url) == "<unparseable-url-redacted>"


@pytest.mark.parametrize(
    "netloc", ["user:syntheticsecret@[bad]", "user:syntheticsecret@x[::1]", "user:pw@[]"]
)
def test_script_check_does_not_depend_on_the_interpreters_urllib(
    monkeypatch: pytest.MonkeyPatch, netloc: str
) -> None:
    # Stand in for a lenient urllib.parse (CPython 3.11.1 accepts these
    # authorities) so the script's own check is exercised on every interpreter.
    lenient = SplitResult("cubrid", netloc, "/db", "", "")
    monkeypatch.setattr(collect_repro, "urlsplit", lambda _url: lenient)
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    assert collect_repro._redact_url(f"cubrid://{netloc}/db") == "<unparseable-url-redacted>"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "cubrid://user:syntheticsecret@[::1]:33000/db",
            "cubrid://user:***@[::1]:33000/db",
        ),
        ("cubrid://user:syntheticsecret@[FE80::1]/db", "cubrid://user:***@[fe80::1]/db"),
        ("cubrid://user@[::1]:33000/db", "cubrid://user@[::1]:33000/db"),
    ],
)
def test_bracketed_ipv6_literal_still_parses_and_redacts(
    monkeypatch: pytest.MonkeyPatch, url: str, expected: str
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    monkeypatch.setenv("CUBRID_TEST_URL", url)
    redacted = collect_repro._redact_url(url)
    assert redacted == expected
    assert "syntheticsecret" not in redacted


@pytest.mark.parametrize(
    "url",
    ["cubrid://user:syntheticsecret@[bad]/db", "cubrid://user:syntheticsecret@[::1]:33000/db"],
)
def test_bracketed_host_secret_never_reaches_the_bundle(
    bundle: Path, monkeypatch: pytest.MonkeyPatch, url: str
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    monkeypatch.setenv("CUBRID_TEST_URL", url)
    monkeypatch.setenv("BUG_HUNT_FAILURE_TRACEBACK", f"connect {url} syntheticsecret")
    collect_repro.main()
    persisted = (bundle / "metadata.json").read_text() + (bundle / "reproduce.md").read_text()
    assert "syntheticsecret" not in persisted


@pytest.mark.parametrize(
    "state", ["missing", "malformed", "sha", "endpoint", "unavailable", "redacted", "observed"]
)
def test_sidecar_identity_is_verified_not_inferred(
    bundle: Path, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    monkeypatch.setenv("GITHUB_SHA", "current-sha")
    monkeypatch.setenv("CUBRID_TEST_HOST", "h")
    monkeypatch.setenv("CUBRID_TEST_PORT", "1")
    monkeypatch.setenv("CUBRID_TEST_DB", "db")
    monkeypatch.setenv("CUBRID_TEST_USER", "u")
    sidecar = bundle.parent / "server.json"
    info = {
        "status": "observed",
        "version": "11.4.6.1963",
        "endpoint": "u@h:1/db",
        "github_sha": "current-sha",
        "observed_at": "2026-10-03T00:00:00Z",
    }
    if state == "sha":
        info["github_sha"] = "stale-sha"
    elif state == "endpoint":
        info["endpoint"] = "u@other:1/db"
    elif state == "unavailable":
        info.update(status="unavailable", version="", reason="lookup failed")
    elif state == "redacted":
        monkeypatch.setenv("CUBRID_TEST_PASSWORD", "private-password")
        info["version"] = "11.4-private-password"
    if state != "missing":
        sidecar.write_text("{bad" if state == "malformed" else json.dumps(info))
    meta = _collect(bundle, "--server-info", str(sidecar))
    identity = meta["server_identity"]
    if state == "observed":
        assert identity["status"] == "observed"
        assert identity["version"] == "11.4.6.1963"
    else:
        assert identity["status"] == "unavailable"
        assert identity["reason"]
        assert not identity.get("version")


def test_readable_sidecar_with_unavailable_driver_is_diagnostic_only(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sidecar = bundle.parent / "server.json"
    sidecar.write_text(json.dumps({"status": "observed", "version": "11.4.6.1963"}))
    original = collect_repro.importlib.util.spec_from_file_location

    def missing_driver(*args: object, **kwargs: object):
        spec = original(*args, **kwargs)
        if args[0] == "_pycubrid_repro_endpoint":

            def fail(module: object) -> None:
                raise ImportError("driver unavailable")

            spec.loader.exec_module = fail
        return spec

    monkeypatch.setattr(collect_repro.importlib.util, "spec_from_file_location", missing_driver)
    meta = _collect(bundle, "--server-info", str(sidecar))
    assert meta["server_identity"]["status"] == "unavailable"
    assert "driver unavailable" in meta["server_identity"]["reason"]


def test_malformed_url_and_legacy_hints_cannot_leak_credentials(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CUBRID_TEST_URL", "cubrid://app:s3cret@h:bad/db")
    monkeypatch.setenv("CUBRID_TEST_PASSWORD", "s3cret")
    monkeypatch.setenv("BUG_HUNT_FAILING_TEST", "tests/test_x.py::test_x[s3cret]")
    monkeypatch.setenv("BUG_HUNT_FAILURE_TRACEBACK", "failed with s3cret")
    result = collect_repro.main()
    assert result == 0
    persisted = (bundle / "metadata.json").read_text() + (bundle / "reproduce.md").read_text()
    assert "s3cret" not in persisted
    assert "python -m pytest tests/" not in persisted


def test_copy_failure_records_diagnostic_error_without_failing_collection(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    collect_repro.HYPOTHESIS_DB.mkdir()
    monkeypatch.setattr(collect_repro.shutil, "copytree", lambda *a, **k: _copy_failure())
    result = collect_repro.main()
    assert result == 0
    meta = json.loads((bundle / "metadata.json").read_text())
    assert meta["collection_errors"]


def _copy_failure() -> None:
    raise OSError("copy unavailable")


def test_unreadable_report_is_explicit_diagnostic_absence(bundle: Path) -> None:
    report = bundle.parent / "report-directory"
    report.mkdir()
    meta = _collect(bundle, "--junit", str(report))
    assert meta["reports"][0]["status"] == "read-error"
    assert meta["failures"] == []


def test_bundle_write_failure_is_best_effort_and_sanitized(
    bundle: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("CUBRID_TEST_PASSWORD", "private-password")
    original = Path.write_text

    def write(path: Path, *args: object, **kwargs: object) -> int:
        if path.parent == bundle:
            raise OSError("private-password write unavailable")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", write)
    result = collect_repro.main()
    assert result == 0
    output = capsys.readouterr()
    assert "unavailable" in output.err
    assert "private-password" not in output.err


def test_redacted_node_is_unresolved_not_a_changed_replay_target(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CUBRID_TEST_PASSWORD", "private-password")
    report = bundle.parent / "node.xml"
    report.write_text(
        '<testsuite><testcase file="tests/test_example.py" classname="tests.test_example" '
        'name="test_value[private-password]"><failure>bad</failure></testcase></testsuite>'
    )
    meta = _collect(bundle, "--junit", str(report))
    assert meta["failures"][0]["identity_status"] == "unresolved"
    assert "private-password" not in (bundle / "metadata.json").read_text()
    replay = (bundle / "reproduce.md").read_text()
    assert "test_value[" not in replay
    assert "unavailable" in replay.lower()


def test_replay_uses_the_effective_url_endpoint_not_default_fields(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for field in ("HOST", "PORT", "DB", "USER"):
        monkeypatch.delenv(f"CUBRID_TEST_{field}", raising=False)
    monkeypatch.setenv("CUBRID_TEST_URL", "cubrid://app:private-password@url-broker:33123/shop")
    monkeypatch.setenv("BUG_HUNT_FAILING_TEST", "tests/test_example.py::test_value")
    result = collect_repro.main()
    assert result == 0
    tokens = _replay_tokens(bundle)
    assert "CUBRID_TEST_HOST=url-broker" in tokens
    assert "CUBRID_TEST_PORT=33123" in tokens
    assert "CUBRID_TEST_DB=shop" in tokens
    assert "CUBRID_TEST_USER=app" in tokens
    assert "private-password" not in (bundle / "reproduce.md").read_text()


def test_real_pytest_xunit1_identity_round_trip(bundle: Path) -> None:
    case_dir = bundle.parent / "tests"
    case_dir.mkdir()
    source = case_dir / "test_real.py"
    source.write_text(
        "import pytest\nclass TestActual:\n"
        "    @pytest.mark.parametrize('value', [1], ids=['a.b-x'])\n"
        "    def test_value(self, value):\n        assert value == 2\n"
    )
    report = bundle.parent / "real.xml"
    completed = subprocess.run(  # noqa: S603 - fixed offline pytest, test-owned source
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/test_real.py",
            "-o",
            "addopts=",
            "-o",
            "junit_family=xunit1",
            f"--junitxml={report}",
            "-q",
        ],
        cwd=bundle.parent,
        env={**os.environ, "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"},
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 1, completed.stdout + completed.stderr
    actual = ElementTree.parse(report).find(".//testcase")
    assert actual is not None
    assert actual.get("file") == "tests/test_real.py"
    assert actual.get("classname") == "tests.test_real.TestActual"
    meta = _collect(bundle, "--junit", str(report))
    assert meta["failures"][0]["node_id"] == "tests/test_real.py::TestActual::test_value[a.b-x]"


# Issue #777: a CUBRID_TEST_URL without "://" must not leak its password.
_SCHEMELESS_URLS = [
    ("u:Syn7hS3cret@host/db", "Syn7hS3cret"),
    ("cubrid:u:Syn7hS3cret@host/db", "Syn7hS3cret"),
    ("//u:Syn7hS3cret@host/db", "Syn7hS3cret"),
    ("u:Syn7hS3cret@[::1]/db", "Syn7hS3cret"),
    ("cubrid:/u:Syn7hS3cret@host/db", "Syn7hS3cret"),
    ("u:Syn7h@S3cret@host/db", "Syn7h@S3cret"),
    ("cubrid:u:Syn7h:S3cret@host/db", "Syn7h:S3cret"),
    ("u:Syn7h:S3cret@host/db", "Syn7h:S3cret"),
    ("cubrid://u:Syn7h:S3cret@host:33000/db", "Syn7h:S3cret"),
    ("cubrid://u:Syn7h/S3cret@host/db", "Syn7h/S3cret"),
    ("cubrid://u:Syn7h#S3cret@host/db", "Syn7h#S3cret"),
    ("cubrid://u:Syn7h?S3cret@host/db", "Syn7h?S3cret"),
    ("cubrid://u:Pa@ssW/ord9@host/db", "Pa@ssW/ord9"),
    ("cubrid://u:PassW/ord9@host/db?opt=a@b", "PassW/ord9"),
    ("cubrid:a:b:u:Syn7hPw@host/db", "Syn7hPw"),
    ("cubrid://u:P\u00e4@Gr\u00f6\u00dfe/W\u00f6rt9@host/db", "P\u00e4@Gr\u00f6\u00dfe/W\u00f6rt9"),
    ('cubrid://u:Qu@"ot\\e/Bs\\9x@host/db', 'Qu@"ot\\e/Bs\\9x'),
]


def _bundle_texts(bundle: Path) -> list[str]:
    """Every bundle file's raw text plus each JSON-decoded metadata string.

    JSON escapes non-ASCII, quotes and backslashes, so a raw-text scan alone
    could miss a leaked password inside metadata.json.
    """
    texts = [
        path.read_text(encoding="utf-8") for path in sorted(bundle.rglob("*")) if path.is_file()
    ]
    pending: list[object] = [json.loads((bundle / "metadata.json").read_text(encoding="utf-8"))]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            pending.extend(value.keys())
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)
        elif isinstance(value, str):
            texts.append(value)
    return texts


@pytest.mark.parametrize(("url", "secret"), _SCHEMELESS_URLS)
def test_url_shape_never_leaks_its_password_into_any_bundle_output(
    bundle: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    url: str,
    secret: str,
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    monkeypatch.setenv("CUBRID_TEST_URL", url)
    monkeypatch.setenv("BUG_HUNT_FAILURE_TRACEBACK", f"connect failed for {url}: {secret}")
    report = bundle.parent / "shape.xml"
    suite = ElementTree.Element("testsuite")
    case = ElementTree.SubElement(
        suite,
        "testcase",
        file="tests/test_example.py",
        classname="tests.test_example",
        name="test_x",
    )
    failure = ElementTree.SubElement(case, "failure", message=f"auth {secret}")
    failure.text = f"OperationalError: login {url} rejected password {secret}"
    ElementTree.ElementTree(suite).write(report, encoding="utf-8")
    sidecar = bundle.parent / "server.json"
    sidecar.write_text("{}")
    meta = _collect(bundle, "--junit", str(report), "--server-info", str(sidecar))
    output = capsys.readouterr()
    texts = [*_bundle_texts(bundle), output.out, output.err]
    for leaked in {secret, *re.split(r"[:/?#@]", secret)}:
        if len(leaked) >= 4:
            assert not [text for text in texts if leaked in text], leaked
    assert meta["failures"][0]["detail"].startswith("OperationalError: login ")


@pytest.mark.parametrize(
    "url",
    [
        "u:Syn7hS3cret@host/db",
        "cubrid:u:Syn7hS3cret@host/db",
        "u:Syn7hS3cret@[::1]/db",
        "cubrid:/u:Syn7hS3cret@host/db",
        "u:@host/db",
        "cubrid:u:@host/db",
        "cubrid://u:Pa@ssW/ord9@host/db",
        "cubrid://u:PassW/ord9@host/db?opt=a@b",
        "cubrid://user:pw@host/db?opt=a@b",
    ],
)
def test_url_with_userinfo_but_no_parsed_password_is_unparseable(
    monkeypatch: pytest.MonkeyPatch, url: str
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    monkeypatch.setenv("CUBRID_TEST_URL", url)
    assert collect_repro._redact_url(url) == "<unparseable-url-redacted>"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("cubrid://user:pw@host:33000/db", "cubrid://user:***@host:33000/db"),
        ("//u:Syn7hS3cret@host/db", "//u:***@host/db"),
        ("cubrid://u:@host/db", "cubrid://u:***@host/db"),
        ("cubrid://user@host:33000/db", "cubrid://user@host:33000/db"),
        ("cubrid://host:33000/db", "cubrid://host:33000/db"),
    ],
)
def test_parseable_url_keeps_its_redacted_shape(
    monkeypatch: pytest.MonkeyPatch, url: str, expected: str
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    monkeypatch.setenv("CUBRID_TEST_URL", url)
    assert collect_repro._redact_url(url) == expected


@pytest.mark.parametrize(("url", "secret"), _SCHEMELESS_URLS)
def test_sanitize_redacts_configured_url_password_in_free_text(
    monkeypatch: pytest.MonkeyPatch, url: str, secret: str
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    monkeypatch.setenv("CUBRID_TEST_URL", url)
    encoded = quote(secret, safe="")
    text = f"detail: password={secret} encoded={encoded} url={url} tail"
    result = collect_repro.sanitize(text)
    assert secret not in result
    assert encoded not in result
    assert result.startswith("detail: password=")
    assert result.endswith(" tail")


def test_empty_url_password_does_not_redact_ordinary_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    for url in ("u:@host/db", "cubrid://u:@host/db", "cubrid:u:@host/db"):
        monkeypatch.setenv("CUBRID_TEST_URL", url)
        assert collect_repro.sanitize("ordinary text") == "ordinary text"


def test_url_decoded_password_is_redacted_in_free_text(
    bundle: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    monkeypatch.setenv("CUBRID_TEST_URL", "cubrid://u:P%40ssWord9@host/db")
    report = bundle.parent / "decoded.xml"
    report.write_text(
        '<testsuite><testcase file="tests/test_example.py" classname="tests.test_example" '
        'name="test_x"><failure message="auth P@ssWord9">login P@ssWord9 rejected</failure>'
        "</testcase></testsuite>"
    )
    meta = _collect(bundle, "--junit", str(report))
    assert meta["cubrid_test_url"] == "cubrid://u:***@host/db"
    assert meta["failures"][0]["detail"] == "login *** rejected"
    assert not [text for text in _bundle_texts(bundle) if "ssWord9" in text]


@pytest.mark.parametrize(
    "url",
    [
        "cubrid://u:" + ":@" * 1500 + "Syn7hS3cret@host/db",
        "u:" + "a:@" * 300 + "Syn7hS3cret@host/db",
    ],
    ids=["over-length", "over-candidates"],
)
def test_overlong_url_is_unparseable_and_redacted_whole(
    monkeypatch: pytest.MonkeyPatch, url: str
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    monkeypatch.setenv("CUBRID_TEST_URL", url)
    assert collect_repro._url_passwords(url) is None
    assert collect_repro._redact_url(url) == "<unparseable-url-redacted>"
    assert collect_repro.sanitize(f"before {url} after") == "before *** after"


def test_resolver_is_not_trusted_for_an_unparseable_url(
    bundle: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # urllib ends the password at "#" and lowercases "HostLike" into the host,
    # so the resolver would name a password fragment as the endpoint host.
    for field in ("HOST", "PORT", "DB", "USER", "PASSWORD"):
        monkeypatch.delenv(f"CUBRID_TEST_{field}", raising=False)
    url = "cubrid://u:Pa@HostLike#ord9@host/db"
    monkeypatch.setenv("CUBRID_TEST_URL", url)
    monkeypatch.setenv("GITHUB_SHA", "current-sha")
    monkeypatch.setenv("BUG_HUNT_FAILING_TEST", "tests/test_example.py::test_value")
    # Mirror scripts/wait_for_cubrid.py: the sidecar endpoint is the resolver's describe().
    sidecar = bundle.parent / "server.json"
    sidecar.write_text(
        json.dumps(
            {
                "status": "observed",
                "version": "11.4.6.1963",
                "endpoint": "u@hostlike:33000/testdb",
                "github_sha": "current-sha",
                "observed_at": "2026-10-03T00:00:00Z",
            }
        )
    )
    meta = _collect(bundle, "--server-info", str(sidecar))
    output = capsys.readouterr()
    texts = [*_bundle_texts(bundle), output.out, output.err]
    assert not [text for text in texts if "hostlike" in text.casefold()]
    assert meta["cubrid_test_url"] == "<unparseable-url-redacted>"
    assert meta["server_identity"]["status"] == "unavailable"
    assert "unavailable" in (bundle / "reproduce.md").read_text()


def test_overlong_well_formed_url_still_redacts_its_parsed_password(
    bundle: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    url = "cubrid://u:LongPw9Secret@host/db?opt=" + "x" * 2100
    assert collect_repro._url_passwords(url) is None
    monkeypatch.setenv("CUBRID_TEST_URL", url)
    monkeypatch.setenv("BUG_HUNT_FAILURE_TRACEBACK", "login LongPw9Secret rejected")
    report = bundle.parent / "long.xml"
    report.write_text(
        '<testsuite><testcase file="tests/test_example.py" classname="tests.test_example" '
        'name="test_x"><failure message="auth LongPw9Secret">login LongPw9Secret rejected'
        "</failure></testcase></testsuite>"
    )
    meta = _collect(bundle, "--junit", str(report))
    output = capsys.readouterr()
    texts = [*_bundle_texts(bundle), output.out, output.err]
    assert not [text for text in texts if "LongPw9Secret" in text]
    assert meta["failures"][0]["detail"] == "login *** rejected"
    assert meta["failures"][0]["message"] == "auth ***"
    assert meta["failure_traceback"] == "login *** rejected"


@pytest.mark.parametrize("char", ["\t", "\r", "\n"])
def test_url_password_with_tab_or_newline_is_redacted(
    monkeypatch: pytest.MonkeyPatch, char: str
) -> None:
    # urllib strips tab/CR/LF before parsing, so its password lacks them.
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    secret = f"Tab{char}Sec9"
    monkeypatch.setenv("CUBRID_TEST_URL", f"cubrid://u:{secret}@host/db")
    encoded = quote(secret, safe="")
    result = collect_repro.sanitize(f"raw={secret} encoded={encoded} tail")
    assert secret not in result
    assert encoded not in result
    assert result == "raw=*** encoded=*** tail"


def test_short_derived_fragments_are_not_redaction_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CUBRID_TEST_PASSWORD", raising=False)
    url = "cubrid://u:x/Secret9@host/db"
    monkeypatch.setenv("CUBRID_TEST_URL", url)
    candidates = collect_repro._url_passwords(url)
    assert candidates is not None
    assert "x" not in candidates
    assert {"Secret9", "x/Secret9"} <= candidates
    assert collect_repro.sanitize("example x/Secret9 Secret9") == "example *** ***"


def test_endpoint_leak_checks_ignore_case(bundle: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # A hostname is case-insensitive, so a lowercased password is still a leak.
    for field in ("PORT", "DB", "USER", "PASSWORD"):
        monkeypatch.delenv(f"CUBRID_TEST_{field}", raising=False)
    monkeypatch.setenv("CUBRID_TEST_URL", "cubrid://u:HostLike@host/db")
    monkeypatch.setenv("CUBRID_TEST_HOST", "hostlike")
    monkeypatch.setenv("GITHUB_SHA", "current-sha")
    monkeypatch.setenv("BUG_HUNT_FAILING_TEST", "tests/test_example.py::test_value")
    sidecar = bundle.parent / "server.json"
    sidecar.write_text(
        json.dumps(
            {
                "status": "observed",
                "version": "11.4.6.1963",
                "endpoint": "u@hostlike:33000/db",
                "github_sha": "current-sha",
                "observed_at": "2026-10-03T00:00:00Z",
            }
        )
    )
    meta = _collect(bundle, "--server-info", str(sidecar))
    # Only the endpoint checks are under test: cubrid_test_host echoes the
    # caller's own CUBRID_TEST_HOST verbatim.
    replay = (bundle / "reproduce.md").read_text()
    assert "hostlike" not in replay.casefold()
    assert "hostlike" not in json.dumps(meta["server_identity"]).casefold()
    assert meta["server_identity"]["status"] == "unavailable"
    assert "unavailable" in replay
