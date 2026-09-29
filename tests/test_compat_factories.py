"""Construction-only compatibility surfaces do not change the 1.x driver."""

from __future__ import annotations

import inspect
import json
from typing import Any

import pytest

import pycubrid
from pycubrid.compat import cubriddb, native
from pycubrid.exceptions import InterfaceError, NotSupportedError


DSN = "CUBRID:127.0.0.1:33000:testdb:::"


@pytest.fixture
def fake_driver(monkeypatch: pytest.MonkeyPatch) -> type:
    class Driver:
        created: list[Driver] = []

        def __init__(self, **kwargs: Any) -> None:
            self.options = kwargs
            self.close_calls = 0
            self.drop_calls = 0
            self.created.append(self)

        def close(self) -> None:
            self.close_calls += 1

        def _drop_connection(self) -> None:
            self.drop_calls += 1
            self.close()

    monkeypatch.setattr(native, "_DriverConnection", Driver)
    return Driver


def test_native_defaults_own_one_autocommitting_transport(fake_driver: type) -> None:
    connection = native.connect(DSN)
    assert isinstance(connection, native.connection)
    assert len(fake_driver.created) == 1
    assert connection._driver is fake_driver.created[0]
    assert connection._driver.options == {
        "host": "127.0.0.1",
        "port": 33000,
        "database": "testdb",
        "user": "public",
        "password": "",
        "autocommit": True,
        "charset": "utf-8",
    }
    connection.close()
    connection.close()
    assert connection._driver.close_calls == 1


@pytest.mark.parametrize("factory_name", ["Connect", "connect", "connection"])
def test_wrapper_factories_retain_exact_native_owner(fake_driver: type, factory_name: str) -> None:
    wrapper = getattr(cubriddb, factory_name)(DSN, "dba", "secret")
    assert isinstance(wrapper, cubriddb.Connection)
    assert isinstance(wrapper.connection, native.connection)
    assert wrapper.connection._driver is fake_driver.created[0]
    assert wrapper.connection._driver.options["user"] == "dba"
    assert wrapper.connection._driver.options["password"] == "secret"
    wrapper.close()
    wrapper.close()
    assert fake_driver.created[0].close_calls == 1


def test_factory_positionals_override_keywords(fake_driver: type) -> None:
    wrapper = cubriddb.Connect(
        DSN, "dba", "from-position", dsn="ignored", user="ignored", password="ignored"
    )
    assert wrapper.connection._driver.options["user"] == "dba"
    assert wrapper.connection._driver.options["password"] == "from-position"
    wrapper.close()


def test_dsn_credentials_never_override_python_defaults(fake_driver: type) -> None:
    wrapper = cubriddb.Connect("CUBRID:127.0.0.1:33000:testdb:embedded:secret:")
    assert wrapper.connection._driver.options["user"] == "public"
    assert wrapper.connection._driver.options["password"] == ""
    wrapper.close()


def test_direct_constructors_and_case_insensitive_backend(fake_driver: type) -> None:
    native_obj = native.connection(DSN.lower(), user="dba", passwd="")
    wrapper = cubriddb.Connection(DSN, "dba", "", "utf8")
    assert native_obj._driver.options["user"] == "dba"
    assert wrapper.connection._driver is fake_driver.created[1]
    native_obj.close()
    wrapper.close()


@pytest.mark.parametrize(
    ("url", "error"),
    [
        ("", InterfaceError),
        ("CUBRID:::testdb:::", InterfaceError),
        ("CUBRID:localhost::testdb:::", InterfaceError),
        ("CUBRID:localhost:0:testdb:::", InterfaceError),
        ("CUBRID:localhost:65536:testdb:::", InterfaceError),
        ("CUBRID:bad host:33000:testdb:::", InterfaceError),
        ("MYSQL:localhost:33000:testdb:::", InterfaceError),
        ("CUBRID:localhost:33000:testdb::", InterfaceError),
        ("CUBRID-MYSQL:localhost:33000:testdb:::", NotSupportedError),
        ("CUBRID:localhost:33000:testdb:::?althosts=elsewhere", NotSupportedError),
        ("CUBRID:localhost:33000:testdb:::?altHosts=standby:33000", NotSupportedError),
    ],
)
def test_invalid_or_unsupported_dsn_fails_before_transport(
    fake_driver: type, url: str, error: type[Exception]
) -> None:
    with pytest.raises(error):
        native.connect(url)
    assert not fake_driver.created


@pytest.mark.parametrize(
    ("args", "kwargs"),
    [
        ((DSN, None), {}),
        ((DSN, b"dba"), {}),
        ((DSN,), {"passwd": None}),
        ((DSN,), {"unknown": "value"}),
        ((DSN, "dba", ""), {"user": "duplicate"}),
        ((DSN, "dba", "", "extra"), {}),
    ],
)
def test_native_argument_errors_precede_transport(
    fake_driver: type, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> None:
    with pytest.raises(TypeError):
        native.connect(*args, **kwargs)
    assert not fake_driver.created


def test_nul_and_sensitive_dsn_are_rejected_without_echo(
    fake_driver: type, caplog: pytest.LogCaptureFixture
) -> None:
    with pytest.raises(ValueError):
        native.connect(DSN + "\x00")
    with pytest.raises(NotSupportedError) as exc:
        native.connect("CUBRID:localhost:33000:testdb:::?password=private-marker")
    assert "private-marker" not in str(exc.value)
    assert "private-marker" not in caplog.text
    assert not fake_driver.created


@pytest.mark.parametrize("keyword", ["user", "passwd"])
def test_native_credential_nul_fails_before_transport(fake_driver: type, keyword: str) -> None:
    with pytest.raises(ValueError, match="NUL"):
        native.connect(DSN, **{keyword: "bad\x00value"})
    assert not fake_driver.created


def test_wrapper_charset_passes_through_to_the_driver(fake_driver: type) -> None:
    # The driver validates and normalizes it before socket work (#86).
    wrapper = cubriddb.Connection(DSN, charset="euckr")
    assert wrapper.connection._driver.options["charset"] == "euckr"
    assert cubriddb.Connection(DSN).connection._driver.options["charset"] == "utf8"


def test_invalid_charset_type_and_extra_wrapper_args_fail_early(fake_driver: type) -> None:
    with pytest.raises(TypeError):
        cubriddb.Connection(DSN, charset=None)
    with pytest.raises(TypeError):
        cubriddb.Connect(DSN, "dba", "", "extra")
    with pytest.raises(TypeError):
        cubriddb.Connect(DSN, unknown=True)
    assert not fake_driver.created


def test_constructor_failure_closes_acquired_transport_and_keeps_original(
    fake_driver: type, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FailingDriver(fake_driver):
        instance: FailingDriver | None = None

        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            type(self).instance = self
            self._socket = object()  # Model a transport acquired before setup failed.
            raise RuntimeError("initialization failed")

    monkeypatch.setattr(native, "_DriverConnection", FailingDriver)
    with pytest.raises(RuntimeError, match="initialization failed"):
        native.connect(DSN)
    assert FailingDriver.instance is not None
    assert FailingDriver.instance.drop_calls == 1
    assert FailingDriver.instance.close_calls == 1


def test_close_can_retry_after_underlying_close_fails(
    fake_driver: type, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FailingOnceDriver(fake_driver):
        def close(self) -> None:
            super().close()
            if self.close_calls == 1:
                raise RuntimeError("close failed")

    monkeypatch.setattr(native, "_DriverConnection", FailingOnceDriver)
    session = native.connect(DSN)
    with pytest.raises(RuntimeError, match="close failed"):
        session.close()
    session.close()
    assert session._driver.close_calls == 2


def test_secondary_drop_failure_cannot_mask_setup_error_or_log_dsn(
    fake_driver: type, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    class FailingDriver(fake_driver):
        instance: FailingDriver | None = None

        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            type(self).instance = self
            self._socket = object()
            raise ValueError("primary setup failure")

        def _drop_connection(self) -> None:
            super()._drop_connection()
            raise RuntimeError("secondary cleanup failure")

    monkeypatch.setattr(native, "_DriverConnection", FailingDriver)
    with pytest.raises(ValueError, match="primary setup failure"):
        native.connect("CUBRID:localhost:33000:testdb:dba:private-marker:")
    assert FailingDriver.instance is not None
    assert FailingDriver.instance.drop_calls == 1
    assert FailingDriver.instance.close_calls == 1
    assert "private-marker" not in caplog.text


def test_namespace_is_partial_and_ordinary_contract_is_unchanged() -> None:
    assert cubriddb.__all__ == ["Connection", "Connect", "connect", "connection"]
    assert native.__all__ == ["connection", "connect", "cursor"]
    for name in ("apilevel", "paramstyle", "threadsafety"):
        assert not hasattr(cubriddb, name)
    assert hasattr(native.connection, "cursor")
    assert pycubrid.threadsafety == 1
    assert inspect.signature(pycubrid.connect).parameters["user"].default == "dba"


def test_new_namespace_is_covered_by_the_existing_public_api_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import check_public_api

    assert "pycubrid.compat.cubriddb" in check_public_api.TRACKED_MODULES
    assert "pycubrid.compat.native" in check_public_api.TRACKED_MODULES
    assert ("pycubrid.compat.cubriddb", "Connection") in check_public_api.TRACKED_CLASSES
    assert ("pycubrid.compat.native", "connection") in check_public_api.TRACKED_CLASSES
    baseline = json.loads(check_public_api.BASELINE_PATH.read_text(encoding="utf-8"))
    assert check_public_api._diff_dicts(baseline, check_public_api.extract_full_surface()) == []

    monkeypatch.setattr(cubriddb, "temporary_export", lambda: None, raising=False)
    monkeypatch.setattr(cubriddb, "__all__", [*cubriddb.__all__, "temporary_export"])
    assert check_public_api._diff_dicts(baseline, check_public_api.extract_full_surface())
