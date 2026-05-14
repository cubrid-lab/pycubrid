"""Shared sync/async parity helpers for integration tests.

The shared scenarios are async-first so pytest can drive one implementation
against both adapters. The sync adapter uses thin async wrappers around direct
calls because pycubrid connections/cursors are not thread-safe enough for
``asyncio.to_thread()`` hopping between worker threads.
"""

# pyright: reportUnusedParameter=false, reportPrivateUsage=false, reportImplicitOverride=false, reportUnusedCallResult=false, reportAny=false

from __future__ import annotations

import os
import socket
import uuid
from collections.abc import Callable, Sequence
from typing import TypedDict

import pycubrid
import pycubrid.aio
from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.connection import Connection
from pycubrid.cursor import Cursor

JsonDeserializer = Callable[[str], object]
Row = tuple[object, ...]


class ConnectKwargs(TypedDict, total=False):
    host: str
    port: int
    database: str
    user: str
    password: str
    fetch_size: int
    json_deserializer: JsonDeserializer


TEST_HOST = os.environ.get("CUBRID_TEST_HOST", "localhost")
TEST_PORT = int(os.environ.get("CUBRID_TEST_PORT", "33000"))
TEST_DB = os.environ.get("CUBRID_TEST_DB", "testdb")
TEST_USER = os.environ.get("CUBRID_TEST_USER", "dba")
TEST_PASSWORD = os.environ.get("CUBRID_TEST_PASSWORD", "")


def connect_kwargs(
    *,
    fetch_size: int | None = None,
    json_deserializer: JsonDeserializer | None = None,
) -> ConnectKwargs:
    kwargs: ConnectKwargs = {
        "host": TEST_HOST,
        "port": TEST_PORT,
        "database": TEST_DB,
        "user": TEST_USER,
        "password": TEST_PASSWORD,
    }
    if fetch_size is not None:
        kwargs["fetch_size"] = fetch_size
    if json_deserializer is not None:
        kwargs["json_deserializer"] = json_deserializer
    return kwargs


def can_connect() -> bool:
    try:
        conn = pycubrid.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
        )
    except Exception:
        return False
    conn.close()
    return True


def table_name(prefix: str = "parity") -> str:
    return "%s_%s" % (prefix, uuid.uuid4().hex[:8])


class ParityAdapter:
    kind: str = "base"

    async def connect(
        self,
        *,
        fetch_size: int | None = None,
        json_deserializer: JsonDeserializer | None = None,
    ) -> Connection | AsyncConnection:
        raise NotImplementedError

    async def close_connection(self, _conn: Connection | AsyncConnection) -> None:
        raise NotImplementedError

    def cursor(self, _conn: Connection | AsyncConnection) -> Cursor | AsyncCursor:
        raise NotImplementedError

    async def close_cursor(self, _cur: Cursor | AsyncCursor) -> None:
        raise NotImplementedError

    async def set_autocommit(self, _conn: Connection | AsyncConnection, _value: bool) -> None:
        raise NotImplementedError

    async def commit(self, _conn: Connection | AsyncConnection) -> None:
        raise NotImplementedError

    async def rollback(self, _conn: Connection | AsyncConnection) -> None:
        raise NotImplementedError

    async def ping(self, _conn: Connection | AsyncConnection, reconnect: bool) -> bool:
        raise NotImplementedError

    async def get_server_version(self, _conn: Connection | AsyncConnection) -> str:
        raise NotImplementedError

    async def get_last_insert_id(self, _conn: Connection | AsyncConnection) -> str:
        raise NotImplementedError

    async def execute(
        self,
        _cur: Cursor | AsyncCursor,
        _operation: str,
        _parameters: Sequence[object] | None = None,
    ) -> None:
        raise NotImplementedError

    async def executemany(
        self,
        _cur: Cursor | AsyncCursor,
        _operation: str,
        _seq_of_parameters: Sequence[Sequence[object]],
    ) -> None:
        raise NotImplementedError

    async def executemany_batch(
        self,
        _cur: Cursor | AsyncCursor,
        _sql_list: list[str],
    ) -> list[tuple[int, int]]:
        raise NotImplementedError

    async def fetchone(self, _cur: Cursor | AsyncCursor) -> Row | None:
        raise NotImplementedError

    async def fetchmany(self, _cur: Cursor | AsyncCursor, _size: int) -> list[Row]:
        raise NotImplementedError

    async def fetchall(self, _cur: Cursor | AsyncCursor) -> list[Row]:
        raise NotImplementedError

    async def drop_transport(self, _conn: Connection | AsyncConnection) -> None:
        raise NotImplementedError

    def autocommit_state(self, conn: Connection | AsyncConnection) -> bool:
        return conn.autocommit

    def lastrowid(self, cur: Cursor | AsyncCursor) -> int | None:
        return cur.lastrowid

    def mark_cas_inactive(self, conn: Connection | AsyncConnection) -> None:
        conn._ensure_connected()
        conn._cas_info = bytes([0]) + bytes(conn._cas_info[1:])

    def transport_token(self, _conn: Connection | AsyncConnection) -> object | None:
        raise NotImplementedError


class SyncParityAdapter(ParityAdapter):
    kind: str = "sync"

    async def connect(
        self,
        *,
        fetch_size: int | None = None,
        json_deserializer: JsonDeserializer | None = None,
    ) -> Connection:
        return pycubrid.connect(
            **connect_kwargs(fetch_size=fetch_size, json_deserializer=json_deserializer)
        )

    async def close_connection(self, conn: Connection | AsyncConnection) -> None:
        assert isinstance(conn, Connection)
        conn.close()

    def cursor(self, conn: Connection | AsyncConnection) -> Cursor:
        assert isinstance(conn, Connection)
        return conn.cursor()

    async def close_cursor(self, cur: Cursor | AsyncCursor) -> None:
        assert isinstance(cur, Cursor)
        cur.close()

    async def set_autocommit(self, conn: Connection | AsyncConnection, value: bool) -> None:
        assert isinstance(conn, Connection)
        conn.autocommit = value

    async def commit(self, conn: Connection | AsyncConnection) -> None:
        assert isinstance(conn, Connection)
        conn.commit()

    async def rollback(self, conn: Connection | AsyncConnection) -> None:
        assert isinstance(conn, Connection)
        conn.rollback()

    async def ping(self, conn: Connection | AsyncConnection, reconnect: bool) -> bool:
        assert isinstance(conn, Connection)
        return conn.ping(reconnect=reconnect)

    async def get_server_version(self, conn: Connection | AsyncConnection) -> str:
        assert isinstance(conn, Connection)
        return conn.get_server_version()

    async def get_last_insert_id(self, conn: Connection | AsyncConnection) -> str:
        assert isinstance(conn, Connection)
        return conn.get_last_insert_id()

    async def execute(
        self,
        cur: Cursor | AsyncCursor,
        operation: str,
        parameters: Sequence[object] | None = None,
    ) -> None:
        assert isinstance(cur, Cursor)
        if parameters is None:
            cur.execute(operation)
            return
        cur.execute(operation, parameters)

    async def executemany(
        self,
        cur: Cursor | AsyncCursor,
        operation: str,
        seq_of_parameters: Sequence[Sequence[object]],
    ) -> None:
        assert isinstance(cur, Cursor)
        cur.executemany(operation, seq_of_parameters)

    async def executemany_batch(
        self,
        cur: Cursor | AsyncCursor,
        sql_list: list[str],
    ) -> list[tuple[int, int]]:
        assert isinstance(cur, Cursor)
        return cur.executemany_batch(sql_list)

    async def fetchone(self, cur: Cursor | AsyncCursor) -> Row | None:
        assert isinstance(cur, Cursor)
        return cur.fetchone()

    async def fetchmany(self, cur: Cursor | AsyncCursor, size: int) -> list[Row]:
        assert isinstance(cur, Cursor)
        return cur.fetchmany(size)

    async def fetchall(self, cur: Cursor | AsyncCursor) -> list[Row]:
        assert isinstance(cur, Cursor)
        return cur.fetchall()

    async def drop_transport(self, conn: Connection | AsyncConnection) -> None:
        assert isinstance(conn, Connection)
        if conn._socket is None:
            return
        try:
            conn._socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        conn._socket.close()
        conn._socket = None

    def transport_token(self, conn: Connection | AsyncConnection) -> object | None:
        assert isinstance(conn, Connection)
        return conn._socket


class AsyncParityAdapter(ParityAdapter):
    kind: str = "async"

    async def connect(
        self,
        *,
        fetch_size: int | None = None,
        json_deserializer: JsonDeserializer | None = None,
    ) -> AsyncConnection:
        return await pycubrid.aio.connect(
            **connect_kwargs(fetch_size=fetch_size, json_deserializer=json_deserializer)
        )

    async def close_connection(self, conn: Connection | AsyncConnection) -> None:
        assert isinstance(conn, AsyncConnection)
        await conn.close()

    def cursor(self, conn: Connection | AsyncConnection) -> AsyncCursor:
        assert isinstance(conn, AsyncConnection)
        return conn.cursor()

    async def close_cursor(self, cur: Cursor | AsyncCursor) -> None:
        assert isinstance(cur, AsyncCursor)
        await cur.close()

    async def set_autocommit(self, conn: Connection | AsyncConnection, value: bool) -> None:
        assert isinstance(conn, AsyncConnection)
        await conn.set_autocommit(value)

    async def commit(self, conn: Connection | AsyncConnection) -> None:
        assert isinstance(conn, AsyncConnection)
        await conn.commit()

    async def rollback(self, conn: Connection | AsyncConnection) -> None:
        assert isinstance(conn, AsyncConnection)
        await conn.rollback()

    async def ping(self, conn: Connection | AsyncConnection, reconnect: bool) -> bool:
        assert isinstance(conn, AsyncConnection)
        return await conn.ping(reconnect=reconnect)

    async def get_server_version(self, conn: Connection | AsyncConnection) -> str:
        assert isinstance(conn, AsyncConnection)
        return await conn.get_server_version()

    async def get_last_insert_id(self, conn: Connection | AsyncConnection) -> str:
        assert isinstance(conn, AsyncConnection)
        return await conn.get_last_insert_id()

    async def execute(
        self,
        cur: Cursor | AsyncCursor,
        operation: str,
        parameters: Sequence[object] | None = None,
    ) -> None:
        assert isinstance(cur, AsyncCursor)
        if parameters is None:
            await cur.execute(operation)
            return
        await cur.execute(operation, parameters)

    async def executemany(
        self,
        cur: Cursor | AsyncCursor,
        operation: str,
        seq_of_parameters: Sequence[Sequence[object]],
    ) -> None:
        assert isinstance(cur, AsyncCursor)
        await cur.executemany(operation, seq_of_parameters)

    async def executemany_batch(
        self,
        cur: Cursor | AsyncCursor,
        sql_list: list[str],
    ) -> list[tuple[int, int]]:
        assert isinstance(cur, AsyncCursor)
        return await cur.executemany_batch(sql_list)

    async def fetchone(self, cur: Cursor | AsyncCursor) -> Row | None:
        assert isinstance(cur, AsyncCursor)
        return await cur.fetchone()

    async def fetchmany(self, cur: Cursor | AsyncCursor, size: int) -> list[Row]:
        assert isinstance(cur, AsyncCursor)
        return await cur.fetchmany(size)

    async def fetchall(self, cur: Cursor | AsyncCursor) -> list[Row]:
        assert isinstance(cur, AsyncCursor)
        return await cur.fetchall()

    async def drop_transport(self, conn: Connection | AsyncConnection) -> None:
        assert isinstance(conn, AsyncConnection)
        if conn._writer is None:
            return
        conn._writer.close()
        try:
            await conn._writer.wait_closed()
        except OSError:
            pass
        conn._writer = None
        conn._reader = None

    def transport_token(self, conn: Connection | AsyncConnection) -> object | None:
        assert isinstance(conn, AsyncConnection)
        return conn._writer


ADAPTERS: tuple[ParityAdapter, ParityAdapter] = (SyncParityAdapter(), AsyncParityAdapter())


async def cleanup_table(
    adapter: ParityAdapter, conn: Connection | AsyncConnection, table: str
) -> None:
    cur = adapter.cursor(conn)
    try:
        await adapter.execute(cur, "DROP TABLE IF EXISTS %s" % table)
        if not adapter.autocommit_state(conn):
            await adapter.commit(conn)
    finally:
        await adapter.close_cursor(cur)


async def select_round_trip(
    adapter: ParityAdapter,
    rows: Sequence[Sequence[object]],
    *,
    table_prefix: str,
    table_definition: str = "(id INT, name VARCHAR(200), val DOUBLE)",
    insert_sql: str | None = None,
    select_sql: str | None = None,
    autocommit: bool = False,
    fetch_size: int | None = None,
    json_deserializer: JsonDeserializer | None = None,
) -> list[Row]:
    table = table_name(table_prefix)
    rendered_insert_sql = None if insert_sql is None else insert_sql.format(table=table)
    rendered_select_sql = None if select_sql is None else select_sql.format(table=table)
    conn = await adapter.connect(fetch_size=fetch_size, json_deserializer=json_deserializer)
    await adapter.set_autocommit(conn, autocommit)
    cur = adapter.cursor(conn)
    try:
        await adapter.execute(cur, "DROP TABLE IF EXISTS %s" % table)
        await adapter.execute(cur, "CREATE TABLE %s %s" % (table, table_definition))
        await adapter.executemany(
            cur,
            rendered_insert_sql or "INSERT INTO %s (id, name, val) VALUES (?, ?, ?)" % table,
            rows,
        )
        if not autocommit:
            await adapter.commit(conn)
        await adapter.execute(
            cur,
            rendered_select_sql or "SELECT id, name, val FROM %s ORDER BY id" % table,
        )
        return await adapter.fetchall(cur)
    finally:
        try:
            await cleanup_table(adapter, conn, table)
        finally:
            await adapter.close_connection(conn)


async def rollback_rows(adapter: ParityAdapter) -> list[Row]:
    table = table_name("rollback")
    conn = await adapter.connect()
    await adapter.set_autocommit(conn, False)
    cur = adapter.cursor(conn)
    try:
        await adapter.execute(cur, "DROP TABLE IF EXISTS %s" % table)
        await adapter.execute(cur, "CREATE TABLE %s (id INT)" % table)
        await adapter.commit(conn)
        await adapter.execute(cur, "INSERT INTO %s (id) VALUES (?)" % table, (999,))
        await adapter.rollback(conn)
        await adapter.execute(cur, "SELECT id FROM %s" % table)
        return await adapter.fetchall(cur)
    finally:
        try:
            await cleanup_table(adapter, conn, table)
        finally:
            await adapter.close_connection(conn)


async def fetchmany_round_trip(adapter: ParityAdapter) -> tuple[list[Row], list[Row], list[Row]]:
    table = table_name("fetchmany")
    rows = [(index, "item_%d" % index, float(index)) for index in range(10)]
    conn = await adapter.connect()
    await adapter.set_autocommit(conn, True)
    cur = adapter.cursor(conn)
    try:
        await adapter.execute(cur, "DROP TABLE IF EXISTS %s" % table)
        await adapter.execute(
            cur, "CREATE TABLE %s (id INT, name VARCHAR(100), val DOUBLE)" % table
        )
        await adapter.executemany(
            cur, "INSERT INTO %s (id, name, val) VALUES (?, ?, ?)" % table, rows
        )
        await adapter.execute(cur, "SELECT id, name, val FROM %s ORDER BY id" % table)
        batch_one = await adapter.fetchmany(cur, 3)
        batch_two = await adapter.fetchmany(cur, 3)
        remaining = await adapter.fetchall(cur)
        return batch_one, batch_two, remaining
    finally:
        try:
            await cleanup_table(adapter, conn, table)
        finally:
            await adapter.close_connection(conn)


async def ping_after_drop(adapter: ParityAdapter, reconnect: bool) -> tuple[bool, Row | None]:
    conn = await adapter.connect()
    try:
        await adapter.drop_transport(conn)
        ping_result = await adapter.ping(conn, reconnect=reconnect)
        if not reconnect:
            return ping_result, None
        cur = adapter.cursor(conn)
        try:
            await adapter.execute(cur, "SELECT 1")
            return ping_result, await adapter.fetchone(cur)
        finally:
            await adapter.close_cursor(cur)
    finally:
        await adapter.close_connection(conn)


async def reconnect_after_inactive_cas(adapter: ParityAdapter) -> tuple[bool, Row, str]:
    conn = await adapter.connect()
    before = adapter.transport_token(conn)
    try:
        adapter.mark_cas_inactive(conn)
        version = await adapter.get_server_version(conn)
        cur = adapter.cursor(conn)
        try:
            await adapter.execute(cur, "SELECT 1")
            row = await adapter.fetchone(cur)
        finally:
            await adapter.close_cursor(cur)
        if row is None:
            raise AssertionError("SELECT 1 must return a row")
        return before is not adapter.transport_token(conn), row, version
    finally:
        await adapter.close_connection(conn)


async def autocommit_transitions(adapter: ParityAdapter) -> tuple[bool, bool, bool]:
    conn = await adapter.connect()
    try:
        initial = adapter.autocommit_state(conn)
        await adapter.set_autocommit(conn, True)
        enabled = adapter.autocommit_state(conn)
        await adapter.set_autocommit(conn, False)
        disabled = adapter.autocommit_state(conn)
        return initial, enabled, disabled
    finally:
        await adapter.close_connection(conn)


async def insert_identity_values(adapter: ParityAdapter) -> tuple[int | None, str]:
    table = table_name("identity")
    conn = await adapter.connect()
    await adapter.set_autocommit(conn, False)
    cur = adapter.cursor(conn)
    try:
        await adapter.execute(cur, "DROP TABLE IF EXISTS %s" % table)
        await adapter.execute(
            cur,
            "CREATE TABLE %s (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), val INT)"
            % table,
        )
        await adapter.commit(conn)
        await adapter.execute(cur, "INSERT INTO %s (name, val) VALUES (?, ?)" % table, ("alpha", 1))
        return adapter.lastrowid(cur), await adapter.get_last_insert_id(conn)
    finally:
        try:
            await cleanup_table(adapter, conn, table)
        finally:
            await adapter.close_connection(conn)


async def executemany_batch_semantics(
    adapter: ParityAdapter,
) -> tuple[list[tuple[int, int]], int, Row | None]:
    table = table_name("batch")
    conn = await adapter.connect()
    await adapter.set_autocommit(conn, True)
    cur = adapter.cursor(conn)
    try:
        await adapter.execute(cur, "DROP TABLE IF EXISTS %s" % table)
        await adapter.execute(cur, "CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(30))" % table)
        results = await adapter.executemany_batch(
            cur,
            [
                "INSERT INTO %s (id, name) VALUES (1, 'first')" % table,
                "INSERT INTO %s (id, name) VALUES (2, 'second')" % table,
                "UPDATE %s SET name = 'updated' WHERE id = 2" % table,
            ],
        )
        batch_rowcount = cur.rowcount
        await adapter.execute(cur, "SELECT COUNT(*) FROM %s" % table)
        return results, batch_rowcount, await adapter.fetchone(cur)
    finally:
        try:
            await cleanup_table(adapter, conn, table)
        finally:
            await adapter.close_connection(conn)


async def close_cursor_then_connection(adapter: ParityAdapter) -> tuple[bool, bool]:
    conn = await adapter.connect()
    cur = adapter.cursor(conn)
    try:
        await adapter.execute(cur, "SELECT 1")
        await adapter.close_cursor(cur)
        await adapter.close_connection(conn)
        return cur._closed, not conn._connected
    finally:
        if conn._connected:
            await adapter.close_connection(conn)
