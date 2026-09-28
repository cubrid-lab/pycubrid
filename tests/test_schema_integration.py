"""Initial live CLASS/ATTRIBUTE ownership contract; wider type matrix is #457."""

from __future__ import annotations

import inspect
import os
import uuid
from typing import Any

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.constants import CCISchemaType
from pycubrid.exceptions import InterfaceError


pytestmark = pytest.mark.integration


async def call(target: Any, name: str, *args: Any, **kwargs: Any) -> Any:
    value = getattr(target, name)(*args, **kwargs)
    return await value if inspect.isawaitable(value) else value


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "boundary", ["fetch", "abandon", "commit", "rollback", "autocommit_false", "autocommit_true"]
)
async def test_owned_schema_rows_and_noncommitting_cleanup(
    asynchronous: bool, boundary: str
) -> None:
    config = dict(
        host=os.environ.get("CUBRID_TEST_HOST", "127.0.0.1"),
        port=int(os.environ.get("CUBRID_TEST_PORT", "33000")),
        database=os.environ.get("CUBRID_TEST_DB", "testdb"),
        user=os.environ.get("CUBRID_TEST_USER", "dba"),
        password=os.environ.get("CUBRID_TEST_PASSWORD", ""),
        no_backslash_escapes=True,
        read_timeout=5,
        connect_timeout=5,
        fetch_size=1,
    )
    name = "s456_" + uuid.uuid4().hex[:16]
    conn = await pycubrid.aio.connect(**config) if asynchronous else pycubrid.connect(**config)
    cursor = await call(conn, "cursor")
    try:
        await call(cursor, "execute", f"CREATE TABLE {name} (id INTEGER, note VARCHAR(40))")
        await call(conn, "commit")
        packet = await call(conn, "get_schema_info", CCISchemaType.CLASS, name, 0)
        rows = await call(conn, "fetch_schema_info", packet)
        assert len(rows) == packet.tuple_count == 1
        assert rows[0][0].split(".")[-1] == name
        assert [column.name for column in packet.columns][:2] == ["NAME", "TYPE"]

        attributes = await call(conn, "get_schema_info", CCISchemaType.ATTRIBUTE, name, 2, arg2="%")
        rows = await call(conn, "fetch_schema_info", attributes)
        assert [row[0] for row in rows] == ["id", "note"]
        filtered = await call(conn, "get_schema_info", CCISchemaType.ATTRIBUTE, name, 2, arg2="n%")
        assert [row[0] for row in await call(conn, "fetch_schema_info", filtered)] == ["note"]
        null_filter = await call(conn, "get_schema_info", CCISchemaType.ATTRIBUTE, name, 0)
        assert await call(conn, "fetch_schema_info", null_filter) == []
        pattern = await call(conn, "get_schema_info", CCISchemaType.CLASS, name[:-2] + "%", 1)
        assert any(
            row[0].split(".")[-1] == name for row in await call(conn, "fetch_schema_info", pattern)
        )

        # FC9/FETCH/handle-only FC6 must not commit unrelated caller work.
        await call(cursor, "execute", f"INSERT INTO {name} (id) VALUES (1)")
        owned = await call(conn, "get_schema_info", CCISchemaType.ATTRIBUTE, name, 2, arg2="%")
        if boundary == "fetch":
            assert len(await call(conn, "fetch_schema_info", owned)) == 2
        elif boundary == "abandon":
            await call(conn, "close_schema_info", owned)
        elif boundary.startswith("autocommit_"):
            enabled = boundary == "autocommit_true"
            if asynchronous:
                await conn.set_autocommit(enabled)
            else:
                conn.autocommit = enabled
        else:
            await call(conn, boundary)
        await call(conn, "close_schema_info", owned)
        with pytest.raises(InterfaceError, match="retired"):
            await call(conn, "fetch_schema_info", owned)
        await call(conn, "rollback")
        await call(cursor, "execute", f"SELECT COUNT(*) FROM {name}")
        assert await call(cursor, "fetchone") == (
            1 if boundary == "commit" or boundary.startswith("autocommit_") else 0,
        )
        await call(cursor, "execute", "SELECT 1")
        assert await call(cursor, "fetchone") == (1,)
    finally:
        try:
            await call(conn, "rollback")
            await call(cursor, "execute", f"DROP TABLE IF EXISTS {name}")
            await call(conn, "commit")
        finally:
            await call(conn, "close")


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["execute", "batch_default", "batch_override"])
async def test_implicit_autocommit_retires_schema_before_cursor_work(
    asynchronous: bool, operation: str
) -> None:
    config = dict(
        host=os.environ.get("CUBRID_TEST_HOST", "127.0.0.1"),
        port=int(os.environ.get("CUBRID_TEST_PORT", "33000")),
        database=os.environ.get("CUBRID_TEST_DB", "testdb"),
        user=os.environ.get("CUBRID_TEST_USER", "dba"),
        password=os.environ.get("CUBRID_TEST_PASSWORD", ""),
        no_backslash_escapes=True,
        read_timeout=5,
        connect_timeout=5,
    )
    name = "s456_auto_" + uuid.uuid4().hex[:16]
    automatic_connection = operation != "batch_override"
    conn = (
        await pycubrid.aio.connect(**config, autocommit=automatic_connection)
        if asynchronous
        else pycubrid.connect(**config, autocommit=automatic_connection)
    )
    try:
        cursor = await call(conn, "cursor")
        await call(cursor, "execute", f"CREATE TABLE {name} (id INTEGER)")
        if not automatic_connection:
            await call(conn, "commit")
        packet = await call(conn, "get_schema_info", CCISchemaType.CLASS, name, 0)
        assert packet.tuple_count == 1
        if operation == "execute":
            await call(cursor, "execute", f"UPDATE {name} SET id=id")
        elif operation == "batch_default":
            await call(cursor, "executemany_batch", [f"UPDATE {name} SET id=id"])
        else:
            await call(
                cursor,
                "executemany_batch",
                [f"UPDATE {name} SET id=id"],
                auto_commit=True,
            )
        with pytest.raises(InterfaceError, match="retired"):
            await call(conn, "fetch_schema_info", packet)
        other_cursor = await call(conn, "cursor")
        await call(other_cursor, "execute", "SELECT 1")
        assert await call(other_cursor, "fetchone") == (1,)
        await call(other_cursor, "close")
    finally:
        await call(conn, "close")
        cleanup = pycubrid.connect(**config)
        try:
            cur = cleanup.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {name}")
            cleanup.commit()
            cur.close()
        finally:
            cleanup.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_autocommit_version_lookup_retires_schema(asynchronous: bool) -> None:
    config = dict(
        host=os.environ.get("CUBRID_TEST_HOST", "127.0.0.1"),
        port=int(os.environ.get("CUBRID_TEST_PORT", "33000")),
        database=os.environ.get("CUBRID_TEST_DB", "testdb"),
        user=os.environ.get("CUBRID_TEST_USER", "dba"),
        password=os.environ.get("CUBRID_TEST_PASSWORD", ""),
        no_backslash_escapes=True,
        read_timeout=5,
        connect_timeout=5,
    )
    name = "s456_version_" + uuid.uuid4().hex[:16]
    conn = (
        await pycubrid.aio.connect(**config, autocommit=True)
        if asynchronous
        else pycubrid.connect(**config, autocommit=True)
    )
    try:
        cursor = await call(conn, "cursor")
        await call(cursor, "execute", f"CREATE TABLE {name} (id INTEGER)")
        packet = await call(conn, "get_schema_info", CCISchemaType.CLASS, name, 0)
        assert packet.tuple_count == 1
        version = await call(conn, "get_server_version")
        assert version and isinstance(version, str)
        with pytest.raises(InterfaceError, match="retired"):
            await call(conn, "fetch_schema_info", packet)
        other = await call(conn, "cursor")
        await call(other, "execute", "SELECT 1")
        assert await call(other, "fetchone") == (1,)
        await call(other, "close")
        await call(cursor, "close")
    finally:
        await call(conn, "close")
        cleanup = pycubrid.connect(**config)
        try:
            cur = cleanup.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {name}")
            cleanup.commit()
            cur.close()
        finally:
            cleanup.close()
