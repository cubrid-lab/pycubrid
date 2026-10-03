"""Offline script verification; these fakes never certify a live recording."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from demos import record_demo


class Database:
    """Small recording state for only the script's fixed SQL operations."""

    def __init__(self) -> None:
        self.exists = False
        self.rows: dict[int, str] = {}
        self.version = "11.4.6.1963"
        self.select_value = 2
        self.fail = ""
        self.connections: list[Connection] = []
        self.cursors: list[Cursor] = []
        self.calls: list[tuple[Connection, str, tuple[object, ...] | None]] = []
        self.events: list[str] = []

    def connect(self, **options: Any) -> Connection:
        conn = Connection(self, bool(options.get("autocommit", False)))
        self.connections.append(conn)
        return conn

    async def connect_async(self, **options: Any) -> AsyncConnection:
        conn = AsyncConnection(self, bool(options.get("autocommit", False)))
        self.connections.append(conn)
        return conn


class Cursor:
    def __init__(self, conn: Connection) -> None:
        self.conn = conn
        self.rowcount = -1
        self.result: list[tuple[object, ...]] = []
        self.closed = False
        conn.db.cursors.append(self)

    def execute(self, sql: str, parameters: tuple[object, ...] | None = None) -> None:
        db = self.conn.db
        db.calls.append((self.conn, sql, parameters))
        operation = sql.split()[0]
        if db.fail == operation or (db.fail == "async" and isinstance(self.conn, AsyncConnection)):
            raise RuntimeError("scripted operation failure")
        self.result = []
        rows = db.rows if self.conn.autocommit else self.conn.rows
        if sql == "SELECT 1 + 1":
            self.result = [(db.select_value,)]
        elif sql == "SELECT COUNT(*) FROM db_class WHERE class_name = ?":
            assert parameters == ("codex_demo_320",)
            self.result = [(int(db.exists),)]
        elif sql == "CREATE TABLE codex_demo_320 (id INTEGER PRIMARY KEY, txt VARCHAR(40))":
            assert parameters is None
            assert not db.exists
            db.exists = True
        elif sql == "DROP TABLE codex_demo_320":
            assert parameters is None
            db.exists = False
            db.rows.clear()
            db.events.append("drop")
        elif sql == "INSERT INTO codex_demo_320 VALUES (?, ?)":
            assert parameters is not None
            key, value = parameters
            rows[int(key)] = str(value)
            self.rowcount = 1
        elif sql == "UPDATE codex_demo_320 SET txt = ? WHERE id = ?":
            assert parameters is not None
            value, key = parameters
            self.rowcount = int(int(key) in rows)
            rows[int(key)] = str(value)
        elif sql == "DELETE FROM codex_demo_320 WHERE id = ?":
            assert parameters is not None
            self.rowcount = int(rows.pop(int(parameters[0]), None) is not None)
        elif sql == "SELECT txt FROM codex_demo_320 WHERE id = ?":
            assert parameters is not None
            value = rows.get(int(parameters[0]))
            self.result = [] if value is None else [(value,)]
            if self.conn.autocommit:
                db.events.append("observer-read")
        else:
            raise AssertionError("unexpected demo SQL: " + sql)
        if db.fail == "rowcount" and operation in ("INSERT", "UPDATE", "DELETE"):
            self.rowcount = 0

    def fetchone(self) -> tuple[object, ...] | None:
        return self.result.pop(0) if self.result else None

    def close(self) -> None:
        self.closed = True
        if self.conn.db.fail == "cursor-close":
            raise RuntimeError("scripted cursor close failure")

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class Connection:
    def __init__(self, db: Database, autocommit: bool) -> None:
        self.db = db
        self.autocommit = autocommit
        self.rows = dict(db.rows)
        self.closed = False

    def get_server_version(self) -> str:
        return self.db.version

    def cursor(self) -> Cursor:
        return Cursor(self)

    def close(self) -> None:
        self.closed = True
        self.db.events.append("close")
        if self.db.fail == "connection-close":
            raise RuntimeError("scripted connection close failure")

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, kind: object, *args: object) -> None:
        try:
            if kind is None:
                if self.db.fail == "commit":
                    raise RuntimeError("scripted commit failure")
                self.db.rows = dict(self.rows)
                self.db.events.append("context-commit")
        finally:
            self.close()


class AsyncCursor(Cursor):
    async def execute(self, sql: str, parameters: tuple[object, ...] | None = None) -> None:
        super().execute(sql, parameters)

    async def fetchone(self) -> tuple[object, ...] | None:
        return super().fetchone()

    async def __aenter__(self) -> AsyncCursor:
        return self

    async def __aexit__(self, *args: object) -> None:
        self.close()


class AsyncConnection(Connection):
    async def get_server_version(self) -> str:
        return super().get_server_version()

    def cursor(self) -> AsyncCursor:
        return AsyncCursor(self)

    async def __aenter__(self) -> AsyncConnection:
        return self

    async def __aexit__(self, *args: object) -> None:
        super().__exit__(*args)


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch) -> Iterator[Database]:
    db = Database()
    for key, value in {
        "DEMO_HOST": "127.0.0.1",
        "DEMO_PORT": "33595",
        "DEMO_EXPECTED_SERVER_VERSION": "11.4.6.1963",
        "DEMO_TABLE": "codex_demo_320",
        "DEMO_PASSWORD": "private-password",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(record_demo.pycubrid, "connect", db.connect)
    monkeypatch.setattr(record_demo.aio, "connect", db.connect_async)
    yield db


@pytest.mark.parametrize("mode", ["short", "extended"])
def test_success_marker_follows_all_closes_and_owned_table_cleanup(
    database: Database,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    mode: str,
) -> None:
    messages: list[str] = []

    def record(message: str) -> None:
        if message.startswith("DEMO_"):
            assert all(conn.closed for conn in database.connections)
            assert all(cur.closed for cur in database.cursors)
            assert not database.exists
        messages.append(message)

    monkeypatch.setattr(record_demo, "print", record, raising=False)
    result = record_demo.main([mode])
    assert result == 0
    assert messages[-1] == "DEMO_" + mode.upper() + "_OK"
    assert "private-password" not in "\n".join(messages)
    assert capsys.readouterr().out == ""
    if mode == "extended":
        assert database.events.index("context-commit") < database.events.index("observer-read")
        assert database.events.index("observer-read") < database.events.index("drop")
        changes = [
            (sql.split()[0], values)
            for _, sql, values in database.calls
            if sql.split()[0] in ("INSERT", "UPDATE", "DELETE")
        ]
        assert changes == [
            ("INSERT", (1, "hello")),
            ("UPDATE", ("updated", 1)),
            ("DELETE", (1,)),
            ("INSERT", (2, "committed")),
        ]
    else:
        assert len(database.calls) == 2


@pytest.mark.parametrize("mode", ["short", "extended"])
def test_server_version_mismatch_fails_before_changes_and_closes(
    database: Database, capsys: pytest.CaptureFixture[str], mode: str
) -> None:
    database.version = "unexpected-version"
    with pytest.raises(RuntimeError, match="server version"):
        record_demo.main([mode])
    assert database.calls == []
    assert all(conn.closed for conn in database.connections)
    assert "DEMO_" not in capsys.readouterr().out


def test_existing_table_is_refused_and_never_dropped(
    database: Database, capsys: pytest.CaptureFixture[str]
) -> None:
    database.exists = True
    database.rows = {42: "foreign"}
    with pytest.raises(RuntimeError, match="preexisting"):
        record_demo.main(["extended"])
    assert database.exists
    assert database.rows == {42: "foreign"}
    assert len(database.calls) == 1
    assert all(conn.closed for conn in database.connections)
    assert "DEMO_" not in capsys.readouterr().out


@pytest.mark.parametrize(
    "failure",
    [
        "INSERT",
        "UPDATE",
        "DELETE",
        "rowcount",
        "commit",
        "async",
        "DROP",
        "cursor-close",
        "connection-close",
    ],
)
def test_extended_failure_never_emits_success_and_always_closes_observer(
    database: Database, capsys: pytest.CaptureFixture[str], failure: str
) -> None:
    database.fail = failure
    with pytest.raises(RuntimeError):
        record_demo.main(["extended"])
    assert all(conn.closed for conn in database.connections)
    assert all(cur.closed for cur in database.cursors)
    assert "DEMO_" not in capsys.readouterr().out
    if failure != "DROP":
        assert not database.exists


def test_wrong_sync_value_does_not_reach_async_or_emit_success(
    database: Database, capsys: pytest.CaptureFixture[str]
) -> None:
    database.select_value = 3
    with pytest.raises(RuntimeError, match="sync SELECT"):
        record_demo.main(["short"])
    assert len(database.connections) == 1
    assert database.connections[0].closed
    assert "DEMO_" not in capsys.readouterr().out


@pytest.mark.parametrize("field", ["DEMO_PORT", "DEMO_EXPECTED_SERVER_VERSION", "DEMO_TABLE"])
def test_missing_recording_inputs_fail_without_connecting(
    database: Database, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    monkeypatch.delenv(field)
    with pytest.raises(ValueError):
        record_demo.main(["extended"])
    assert database.connections == []


def test_table_identifier_is_rejected_not_normalized_or_interpolated(
    database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DEMO_TABLE", "codex_demo_320; DROP TABLE foreign")
    with pytest.raises(ValueError, match="exact lowercase ASCII"):
        record_demo.main(["extended"])
    assert database.connections == []
