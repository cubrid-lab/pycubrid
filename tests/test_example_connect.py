"""Offline unit tests for examples/01_connect.py."""

from __future__ import annotations

import importlib
from typing import Any

import pytest

import pycubrid


class FakeCursor:
    """Minimal in-memory cursor so 01_connect.py can run without a CUBRID server."""

    def __init__(
        self,
        connection: FakeConnection,
        *,
        execute_error: BaseException | None = None,
    ) -> None:
        self.connection = connection
        self.closed = False
        self.executed: list[str] = []
        self._execute_error = execute_error

    def execute(self, operation: str) -> FakeCursor:
        if self.closed:
            raise pycubrid.ProgrammingError("Cursor is closed")
        self.connection.events.append("execute")
        self.executed.append(operation)
        if self._execute_error is not None:
            raise self._execute_error
        return self

    def fetchone(self) -> tuple[int]:
        if self.closed:
            raise pycubrid.ProgrammingError("Cursor is closed")
        self.connection.events.append("fetchone")
        return (2,)

    def close(self) -> None:
        self.closed = True
        self.connection.events.append("cursor.close")


class FakeConnection:
    """In-memory connection that records call order and close() cleanup."""

    def __init__(
        self,
        *,
        server_version: str = "11.2.0.0000",
        execute_error: BaseException | None = None,
        version_error: BaseException | None = None,
    ) -> None:
        self.closed = False
        self.events: list[str] = []
        self.created_cursors: list[FakeCursor] = []
        self._server_version = server_version
        self._execute_error = execute_error
        self._version_error = version_error

    def cursor(self) -> FakeCursor:
        if self.closed:
            raise pycubrid.ProgrammingError("Connection is closed")
        self.events.append("cursor")
        cur = FakeCursor(self, execute_error=self._execute_error)
        self.created_cursors.append(cur)
        return cur

    def get_server_version(self) -> str:
        if self.closed:
            raise pycubrid.ProgrammingError("Connection is closed")
        self.events.append("get_server_version")
        if self._version_error is not None:
            raise self._version_error
        return self._server_version

    def close(self) -> None:
        self.closed = True
        self.events.append("connection.close")


def test_connect_example_env_override_query_output_and_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Custom CUBRID_* env vars should reach connect() and print the expected lines."""
    monkeypatch.setenv("CUBRID_HOST", "db.example.internal")
    monkeypatch.setenv("CUBRID_PORT", "33500")
    monkeypatch.setenv("CUBRID_DATABASE", "demodb")
    monkeypatch.setenv("CUBRID_USER", "appuser")
    monkeypatch.setenv("CUBRID_PASSWORD", "secret")

    connect_calls: list[dict[str, Any]] = []
    fake_conn = FakeConnection(server_version="11.2.0.0000")

    def fake_connect(**kwargs: Any) -> FakeConnection:
        connect_calls.append(kwargs)
        return fake_conn

    monkeypatch.setattr(pycubrid, "connect", fake_connect)

    mod = importlib.import_module("examples.01_connect")
    mod.main()

    assert connect_calls == [
        {
            "host": "db.example.internal",
            "port": 33500,
            "database": "demodb",
            "user": "appuser",
            "password": "secret",
        }
    ]
    # CUBRID_PORT is read as a string from os.environ; main() casts it to int for connect().
    assert isinstance(connect_calls[0]["port"], int)
    assert len(fake_conn.created_cursors) == 1
    cur = fake_conn.created_cursors[0]
    assert cur.executed == ["SELECT 1 + 1"]
    assert cur.closed is True
    assert fake_conn.closed is True
    assert fake_conn.events == [
        "cursor",
        "execute",
        "fetchone",
        "get_server_version",
        "cursor.close",
        "connection.close",
    ]

    captured = capsys.readouterr()
    assert captured.out == "Query result: (2,)\nServer version: 11.2.0.0000\n"


def test_connect_example_uses_default_env_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unset CUBRID_* env vars should fall back to the documented localhost defaults."""
    for var in ("CUBRID_HOST", "CUBRID_PORT", "CUBRID_DATABASE", "CUBRID_USER", "CUBRID_PASSWORD"):
        monkeypatch.delenv(var, raising=False)

    connect_calls: list[dict[str, Any]] = []
    fake_conn = FakeConnection()

    def fake_connect(**kwargs: Any) -> FakeConnection:
        connect_calls.append(kwargs)
        return fake_conn

    monkeypatch.setattr(pycubrid, "connect", fake_connect)

    mod = importlib.import_module("examples.01_connect")
    mod.main()

    assert connect_calls == [
        {
            "host": "localhost",
            "port": 33000,
            "database": "testdb",
            "user": "dba",
            "password": "",
        }
    ]
    assert fake_conn.created_cursors[0].closed is True
    assert fake_conn.closed is True


def test_connect_example_closes_resources_when_execute_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the query fails, the finally block must still close both cursor and connection."""
    fake_conn = FakeConnection(execute_error=pycubrid.DatabaseError("query failed"))
    monkeypatch.setattr(pycubrid, "connect", lambda **kwargs: fake_conn)

    mod = importlib.import_module("examples.01_connect")
    with pytest.raises(pycubrid.DatabaseError, match="query failed"):
        mod.main()

    assert len(fake_conn.created_cursors) == 1
    assert fake_conn.created_cursors[0].closed is True
    assert fake_conn.closed is True
    # Even if an error happens mid-run, finally should close the cursor before the connection.
    assert fake_conn.events == [
        "cursor",
        "execute",
        "cursor.close",
        "connection.close",
    ]


def test_connect_example_closes_resources_when_get_server_version_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If reading the server version fails, both cursor and connection must still close."""
    fake_conn = FakeConnection(
        version_error=pycubrid.OperationalError("could not read server version")
    )
    monkeypatch.setattr(pycubrid, "connect", lambda **kwargs: fake_conn)

    mod = importlib.import_module("examples.01_connect")
    with pytest.raises(pycubrid.OperationalError, match="could not read server version"):
        mod.main()

    assert len(fake_conn.created_cursors) == 1
    assert fake_conn.created_cursors[0].closed is True
    assert fake_conn.closed is True
    assert fake_conn.events == [
        "cursor",
        "execute",
        "fetchone",
        "get_server_version",
        "cursor.close",
        "connection.close",
    ]
