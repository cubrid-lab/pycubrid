"""Offline checks for the shared CUBRID test endpoint and fail-closed gate (#522, #432).

No live server: resolution is pure, the probe runs against a mocked
``pycubrid.connect``, and the end-to-end regressions point a real pytest run
at a local port nothing listens on.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from xml.etree import ElementTree

import pytest

import pycubrid

from . import conftest
from ._cubrid_endpoint import CubridEndpoint, is_configured, probe, resolve_endpoint

ROOT = Path(__file__).resolve().parents[1]
CUBRID_ENV = (
    "CUBRID_TEST_URL",
    "CUBRID_TEST_HOST",
    "CUBRID_TEST_PORT",
    "CUBRID_TEST_DB",
    "CUBRID_TEST_USER",
    "CUBRID_TEST_PASSWORD",
)


# ---------------------------------------------------------------------------
# Resolution: per-field variables > CUBRID_TEST_URL > defaults
# ---------------------------------------------------------------------------


def test_defaults_are_explicit_when_nothing_is_set() -> None:
    assert resolve_endpoint({}) == CubridEndpoint("localhost", 33000, "testdb", "dba", "")


def test_url_supplies_every_field() -> None:
    env = {"CUBRID_TEST_URL": "cubrid://app%40x:p%3Aw@db.example:33522/shop"}

    assert resolve_endpoint(env) == CubridEndpoint("db.example", 33522, "shop", "app@x", "p:w")


def test_url_without_port_or_credentials_falls_back_per_field() -> None:
    env = {"CUBRID_TEST_URL": "cubrid://db.example/"}

    assert resolve_endpoint(env) == CubridEndpoint("db.example", 33000, "testdb", "dba", "")


def test_per_field_variables_override_the_url() -> None:
    env = {
        "CUBRID_TEST_URL": "cubrid://u:secret@url-host:31000/urldb",
        "CUBRID_TEST_HOST": "field-host",
        "CUBRID_TEST_PORT": "32000",
        "CUBRID_TEST_DB": "fielddb",
        "CUBRID_TEST_USER": "field-user",
        "CUBRID_TEST_PASSWORD": "",
    }

    # An explicitly empty password is a real value and wins over the URL's.
    assert resolve_endpoint(env) == CubridEndpoint("field-host", 32000, "fielddb", "field-user", "")


def test_empty_non_password_fields_count_as_unset() -> None:
    env = {
        "CUBRID_TEST_URL": "cubrid://u@url-host:31000/urldb",
        "CUBRID_TEST_HOST": "",
        "CUBRID_TEST_PORT": "",
        "CUBRID_TEST_DB": "",
        "CUBRID_TEST_USER": "",
    }

    assert resolve_endpoint(env) == CubridEndpoint("url-host", 31000, "urldb", "u", "")


def test_backward_compatible_ci_configuration() -> None:
    # CI exports the URL and every per-field variable with the same values.
    env = {
        "CUBRID_TEST_URL": "cubrid://dba@localhost:33114/testdb",
        "CUBRID_TEST_HOST": "localhost",
        "CUBRID_TEST_PORT": "33114",
        "CUBRID_TEST_DB": "testdb",
        "CUBRID_TEST_USER": "dba",
        "CUBRID_TEST_PASSWORD": "",
    }

    assert resolve_endpoint(env) == CubridEndpoint("localhost", 33114, "testdb", "dba", "")


def test_explicit_url_port_zero_is_kept_not_defaulted() -> None:
    assert resolve_endpoint({"CUBRID_TEST_URL": "cubrid://dba@h:0/testdb"}).port == 0


def test_trailing_slash_is_not_part_of_the_database() -> None:
    env = {"CUBRID_TEST_URL": "cubrid://dba@h:1/testdb/"}

    assert resolve_endpoint(env).database == "testdb"


@pytest.mark.parametrize("flag", ["1", "true", "yes"])
def test_scheme_less_url_is_only_the_enable_switch(flag: str) -> None:
    env = {"CUBRID_TEST_URL": flag, "CUBRID_TEST_PORT": "33522"}

    assert is_configured(env)
    assert resolve_endpoint(env) == CubridEndpoint("localhost", 33522, "testdb", "dba", "")


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("postgresql://dba@localhost:5432/testdb", "cubrid:// scheme"),
        ("cubrid+pycubrid://dba@localhost:33000/testdb", "cubrid:// scheme"),
        ("cubrid://dba@localhost:notaport/testdb", "invalid port"),
        ("cubrid:///testdb", "must name a host"),
        ("cubrid://dba@h:1/a/b", "invalid database name"),
    ],
)
def test_malformed_url_is_rejected(url: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        resolve_endpoint({"CUBRID_TEST_URL": url})


@pytest.mark.parametrize(
    ("env", "configured"),
    [
        ({}, False),
        ({"CUBRID_TEST_URL": "", "CUBRID_TEST_HOST": ""}, False),
        ({"CUBRID_TEST_PORT": "33522"}, False),
        ({"CUBRID_TEST_URL": "cubrid://dba@localhost:33000/testdb"}, True),
        ({"CUBRID_TEST_HOST": "127.0.0.1"}, True),
    ],
)
def test_is_configured(env: dict[str, str], configured: bool) -> None:
    assert is_configured(env) is configured


def test_describe_omits_the_password() -> None:
    endpoint = CubridEndpoint("h", 1, "d", "u", "s3cret")

    assert endpoint.describe() == "u@h:1/d"
    assert "s3cret" not in endpoint.describe()


# ---------------------------------------------------------------------------
# Probe
# ---------------------------------------------------------------------------


def test_probe_selects_one_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = MagicMock()
    connect = MagicMock(return_value=conn)
    monkeypatch.setattr(pycubrid, "connect", connect)
    monkeypatch.delenv("CUBRID_TEST_CONNECT_TIMEOUT", raising=False)
    monkeypatch.delenv("CUBRID_TEST_READ_TIMEOUT", raising=False)

    probe(CubridEndpoint("h", 1, "d", "u", "p"))

    connect.assert_called_once_with(
        host="h",
        port=1,
        database="d",
        user="u",
        password="p",
        connect_timeout=5.0,
        read_timeout=5.0,
    )
    conn.cursor.return_value.execute.assert_called_once_with("SELECT 1")
    conn.cursor.return_value.close.assert_called_once_with()
    conn.close.assert_called_once_with()


def test_probe_propagates_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pycubrid, "connect", MagicMock(side_effect=OSError("refused")))

    with pytest.raises(OSError, match="refused"):
        probe(CubridEndpoint("h", 1, "d", "u", "p"))


# ---------------------------------------------------------------------------
# conftest gate (unit): skip when unconfigured, error when unreachable
# ---------------------------------------------------------------------------


def _item(*markers: str) -> Any:
    return SimpleNamespace(
        get_closest_marker=lambda name: object() if name in markers else None,
    )


@pytest.fixture
def gate_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for key in CUBRID_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(conftest, "_probe_error", None)
    return monkeypatch


def test_gate_ignores_offline_tests(gate_env: pytest.MonkeyPatch) -> None:
    gate_env.setattr(conftest, "probe", MagicMock(side_effect=AssertionError("probed")))

    conftest.pytest_runtest_setup(_item())


def test_gate_skips_when_unconfigured(gate_env: pytest.MonkeyPatch) -> None:
    gate_env.setattr(conftest, "probe", MagicMock(side_effect=AssertionError("probed")))

    with pytest.raises(pytest.skip.Exception, match="requires a live CUBRID server"):
        conftest.pytest_runtest_setup(_item("integration"))


def test_gate_fails_once_probed_when_configured_but_unreachable(
    gate_env: pytest.MonkeyPatch,
) -> None:
    gate_env.setenv("CUBRID_TEST_HOST", "127.0.0.1")
    gate_env.setenv("CUBRID_TEST_PORT", "1")
    failing = MagicMock(side_effect=ConnectionRefusedError("refused"))
    gate_env.setattr(conftest, "probe", failing)

    for _ in range(3):
        with pytest.raises(pytest.fail.Exception) as excinfo:
            conftest.pytest_runtest_setup(_item("integration"))
        message = str(excinfo.value)
        assert "dba@127.0.0.1:1/testdb is configured but unreachable" in message
        assert "ConnectionRefusedError: refused" in message

    failing.assert_called_once()


def test_gate_errors_on_a_malformed_url_without_probing(gate_env: pytest.MonkeyPatch) -> None:
    gate_env.setenv("CUBRID_TEST_URL", "postgresql://dba@localhost:5432/testdb")
    gate_env.setattr(conftest, "probe", MagicMock(side_effect=AssertionError("probed")))

    with pytest.raises(pytest.fail.Exception, match="misconfigured: .*cubrid:// scheme"):
        conftest.pytest_runtest_setup(_item("integration"))


def test_gate_passes_when_reachable(gate_env: pytest.MonkeyPatch) -> None:
    gate_env.setenv("CUBRID_TEST_URL", "cubrid://dba@localhost:33000/testdb")
    ok = MagicMock(return_value=None)
    gate_env.setattr(conftest, "probe", ok)

    conftest.pytest_runtest_setup(_item("integration"))
    conftest.pytest_runtest_setup(_item("integration", "slow"))

    ok.assert_called_once()


def test_gate_does_not_plain_probe_tls_tests(gate_env: pytest.MonkeyPatch) -> None:
    gate_env.setenv("CUBRID_TEST_HOST", "127.0.0.1")
    gate_env.setattr(conftest, "probe", MagicMock(side_effect=AssertionError("probed")))

    conftest.pytest_runtest_setup(_item("integration", "tls"))


# ---------------------------------------------------------------------------
# End to end: a real pytest run of formerly import-time-probed modules
# ---------------------------------------------------------------------------

# Two representative modules: one with its own import-time probe and one that
# used the shared ``_parity_helpers.can_connect`` probe before #522.
MODULES = ("tests/test_integration.py", "tests/test_batch_semantics.py")


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _run_suite(tmp_path: Path, extra_env: dict[str, str]) -> tuple[int, dict[str, int], str]:
    env = {key: value for key, value in os.environ.items() if key not in CUBRID_ENV}
    env.update(extra_env)
    env["CUBRID_TEST_CONNECT_TIMEOUT"] = "2"
    env["CUBRID_TEST_READ_TIMEOUT"] = "2"
    report = tmp_path / "results.xml"
    proc = subprocess.run(  # noqa: S603 - fixed argv, test-owned inputs
        [
            sys.executable,
            "-m",
            "pytest",
            *MODULES,
            "-m",
            "integration",
            "-o",
            "addopts=",
            "-p",
            "no:cacheprovider",
            "-q",
            f"--junitxml={report}",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    suite = ElementTree.parse(report).getroot()
    suite = suite if suite.tag == "testsuite" else next(suite.iter("testsuite"))
    counts = {key: int(suite.get(key, "0")) for key in ("tests", "errors", "failures", "skipped")}
    return proc.returncode, counts, proc.stdout + proc.stderr


def test_configured_but_unreachable_endpoint_errors_instead_of_skipping(tmp_path: Path) -> None:
    port = _unused_port()
    code, counts, output = _run_suite(
        tmp_path, {"CUBRID_TEST_HOST": "127.0.0.1", "CUBRID_TEST_PORT": str(port)}
    )

    assert code != 0, output
    assert counts["skipped"] == 0, output
    assert counts["errors"] == counts["tests"] > 0, output
    assert f"dba@127.0.0.1:{port}/testdb is configured but unreachable" in output


def test_malformed_url_does_not_break_the_offline_suite(tmp_path: Path) -> None:
    env = {key: value for key, value in os.environ.items() if key not in CUBRID_ENV}
    env["CUBRID_TEST_URL"] = "postgresql://dba@localhost:5432/testdb"
    proc = subprocess.run(  # noqa: S603 - fixed argv, test-owned inputs
        [sys.executable, "-m", "pytest", *MODULES, "-m", "not integration", "-o", "addopts=", "-q"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )

    # Collection succeeds; every test is merely deselected (exit code 5).
    assert proc.returncode == 5, proc.stdout + proc.stderr


def test_unconfigured_endpoint_still_skips(tmp_path: Path) -> None:
    code, counts, output = _run_suite(tmp_path, {})

    assert code == 0, output
    assert counts["skipped"] == counts["tests"] > 0, output
    assert counts["errors"] == counts["failures"] == 0, output
