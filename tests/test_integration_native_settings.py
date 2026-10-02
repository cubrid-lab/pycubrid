"""Live cached-versus-effective native settings on an owned table (#467)."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import closing
from typing import Any

import pytest

import pycubrid
from pycubrid.compat import native
from pycubrid.connection import Connection
from pycubrid.constants import CCIDbParam
from pycubrid.exceptions import InterfaceError
from pycubrid.protocol import CommitPacket, GetDbParameterPacket, SetDbParameterPacket

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER
from ._parity_helpers import connect_kwargs, table_name

pytestmark = pytest.mark.integration


def _native() -> native.connection:
    return native.connect(f"CUBRID:{TEST_HOST}:{TEST_PORT}:{TEST_DB}:::", TEST_USER, TEST_PASSWORD)


@pytest.fixture
def observer() -> Generator[Connection, None, None]:
    with closing(pycubrid.connect(**connect_kwargs(), autocommit=True)) as conn:
        yield conn


def _sql(conn: Connection, sql: str, params: Any = None) -> Any:
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        if sql.startswith("SELECT"):
            return cur.fetchone()
        return None
    finally:
        cur.close()


@pytest.fixture
def table(observer: Connection) -> Generator[str, None, None]:
    name = table_name("p467")
    _sql(observer, f"CREATE TABLE {name} (id INT)")
    try:
        yield name
    finally:
        _sql(observer, f"DROP TABLE IF EXISTS {name}")


@pytest.fixture
def lob_table(observer: Connection) -> Generator[str, None, None]:
    name = table_name("p467l")
    _sql(observer, f"CREATE TABLE {name} (id INT, b BLOB)")
    try:
        yield name
    finally:
        _sql(observer, f"DROP TABLE IF EXISTS {name}")


def _count(observer: Connection, table: str, row_id: int) -> int:
    row = _sql(observer, f"SELECT COUNT(*) FROM {table} WHERE id = {row_id}")
    assert row is not None
    return int(row[0])


def _actual_isolation(conn: native.connection) -> int:
    driver = conn._driver
    packet = GetDbParameterPacket(CCIDbParam.ISOLATION_LEVEL)
    driver._send_and_receive(packet, expected_generation=driver._physical_generation)
    return packet.value


def test_initial_snapshots_and_raw_assignments_leave_effective_state_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _native()
    try:
        driver = conn._driver
        assert conn.autocommit is True
        assert driver._autocommit is True
        assert conn.lock_timeout == -1
        assert conn.max_string_len == 1_073_741_823
        assert conn.isolation_level == "CUBRID_TRAN_UNKNOWN_ISOLATION"
        actual = _actual_isolation(conn)
        assert actual == 4

        sent: list[object] = []
        original = driver._send_and_receive

        def capture(packet: Any, **kwargs: Any) -> Any:
            sent.append(packet)
            return original(packet, **kwargs)

        monkeypatch.setattr(driver, "_send_and_receive", capture)
        markers = [object() for _ in range(4)]
        for name, marker in zip(
            ("autocommit", "isolation_level", "lock_timeout", "max_string_len"), markers
        ):
            setattr(conn, name, marker)
            assert getattr(conn, name) is marker
        assert sent == []
        assert driver._autocommit is True
        actual = _actual_isolation(conn)
        assert actual == 4
    finally:
        conn.close()


def test_autocommit_effect_is_distinct_from_raw_cache_and_conditional_commit(
    observer: Connection, table: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _native()
    try:
        sent: list[object] = []
        driver = conn._driver
        original = driver._send_and_receive

        def capture(packet: Any, **kwargs: Any) -> Any:
            sent.append(packet)
            return original(packet, **kwargs)

        monkeypatch.setattr(driver, "_send_and_receive", capture)
        conn.autocommit = False  # raw cached snapshot only
        cur = conn.cursor()
        try:
            cur.prepare(f"INSERT INTO {table} VALUES (?)")
            cur.bind_param(1, 1)
            cur.execute()
            visible = _count(observer, table, 1)
            assert visible == 1  # actual mode remained True

            sent.clear()
            conn.set_autocommit(False)  # OUT_TRAN: local mode change, no COMMIT
            assert not any(isinstance(packet, CommitPacket) for packet in sent)
            cur.bind_param(1, 2)
            cur.execute()
            visible = _count(observer, table, 2)
            assert visible == 0

            conn.autocommit = True
            sent.clear()
            conn.set_autocommit(False)  # same effective mode, IN_TRAN
            assert not any(isinstance(packet, CommitPacket) for packet in sent)
            visible = _count(observer, table, 2)
            assert visible == 0

            conn.autocommit = False
            sent.clear()
            conn.set_autocommit(True)  # mode transition commits exactly once
            assert sum(isinstance(packet, CommitPacket) for packet in sent) == 1
            visible = _count(observer, table, 2)
            assert visible == 1
        finally:
            cur.close()
    finally:
        conn.close()


def test_isolation_setter_updates_actual_level_without_committing_manual_dml(
    observer: Connection, table: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _native()
    try:
        driver = conn._driver
        sent: list[object] = []
        original = driver._send_and_receive

        def capture(packet: Any, **kwargs: Any) -> Any:
            sent.append(packet)
            return original(packet, **kwargs)

        monkeypatch.setattr(driver, "_send_and_receive", capture)
        marker = object()
        conn.isolation_level = marker
        conn.set_isolation_level(4)
        assert conn.isolation_level == "CUBRID_REP_CLASS_COMMIT_INSTANCE"
        assert not any(isinstance(packet, SetDbParameterPacket) for packet in sent)

        conn.set_autocommit(False)
        cur = conn.cursor()
        try:
            cur.prepare(f"INSERT INTO {table} VALUES (?)")
            cur.bind_param(1, 3)
            cur.execute()
            visible = _count(observer, table, 3)
            assert visible == 0
            sent.clear()
            conn.set_isolation_level(5)
            assert conn.isolation_level == "CUBRID_REP_CLASS_REP_INSTANCE"
            actual = _actual_isolation(conn)
            assert actual == 5
            assert not any(isinstance(packet, CommitPacket) for packet in sent)
            visible = _count(observer, table, 3)
            assert visible == 0
            conn.rollback()
            visible = _count(observer, table, 3)
            assert visible == 0
        finally:
            cur.close()
    finally:
        conn.close()


def test_manual_fetch_stays_nontransferable_after_later_commit(
    observer: Connection, lob_table: str
) -> None:
    _sql(observer, f"INSERT INTO {lob_table} VALUES (?, ?)", (1, b"abc"))
    source, destination = _native(), _native()
    try:
        source.set_autocommit(False)
        query = source.cursor()
        try:
            query.prepare(f"SELECT b FROM {lob_table} WHERE id = 1")
            query.execute()
            lob = source.lob()
            query.fetch_lob(1, lob)
            assert lob._state[4] is False  # provenance records fetch-time mode
            source.commit()
        finally:
            query.close()

        insert = destination.cursor()
        try:
            insert.prepare(f"INSERT INTO {lob_table} VALUES (?, ?)")
            insert.bind_param(1, 2)
            with pytest.raises(InterfaceError, match="committed row"):
                insert.bind_lob(2, lob)
        finally:
            insert.close()
    finally:
        destination.close()
        source.close()
