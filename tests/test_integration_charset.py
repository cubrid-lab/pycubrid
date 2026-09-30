"""Live ``charset="euckr"`` round trips against an EUC-KR database (#86).

These tests need a database created with ``CUBRID_LOCALE=ko_KR.euckr`` (the
``integration-charset`` CI lane). Against any other database charset they skip
with a reason ``scripts/check_integration_lanes.py`` classifies.
"""

from __future__ import annotations

import inspect
import json
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio

import pycubrid
import pycubrid.aio
from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import DataError, ProgrammingError
from pycubrid.lob import Lob

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER

pytestmark = pytest.mark.integration

EUCKR_LANE_SKIP = "requires an EUC-KR database (integration-charset lane)"
HANGUL = "한글"


def _database_charset() -> str:
    # charset() of an ASCII literal answers in ASCII whatever the codec.
    with pycubrid.connect(
        host=TEST_HOST, port=TEST_PORT, database=TEST_DB, user=TEST_USER, password=TEST_PASSWORD
    ) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT charset('a')")
        row = cursor.fetchone()
        return str(row[0]) if row else ""


@pytest.fixture(scope="module")
def euckr_database() -> None:
    if _database_charset() != "euckr":
        pytest.skip(EUCKR_LANE_SKIP)


async def _call(value: Any) -> Any:
    """Await async driver results; pass sync ones through."""
    return await value if inspect.isawaitable(value) else value


async def _connect(kind: str, **options: Any) -> Any:
    options = {
        "host": TEST_HOST,
        "port": TEST_PORT,
        "database": TEST_DB,
        "user": TEST_USER,
        "password": TEST_PASSWORD,
        "autocommit": True,
        **options,
    }
    if kind == "sync":
        return pycubrid.connect(**options)
    return await pycubrid.aio.connect(**options)


@pytest_asyncio.fixture(params=["sync", "async"])
async def conn(request: pytest.FixtureRequest, euckr_database: None) -> AsyncIterator[Any]:
    connection = await _connect(
        request.param, charset="euckr", decode_collections=True, json_deserializer=json.loads
    )
    try:
        yield connection
    finally:
        await _call(connection.close())


async def _query(connection: Any, sql: str, params: Any = None) -> list[Any]:
    cursor = connection.cursor()
    try:
        await _call(cursor.execute(sql, params))
        if cursor.description is None:
            return []
        return list(await _call(cursor.fetchall()))
    finally:
        await _call(cursor.close())


def _table(prefix: str = "pycubrid_cs") -> str:
    return "%s_%s" % (prefix, uuid.uuid4().hex[:8])


@pytest.mark.asyncio
async def test_hangul_round_trips_in_text_columns(conn: Any) -> None:
    table = _table()
    await _query(
        conn,
        f"CREATE TABLE {table} (c CHAR(4), v VARCHAR(10), s STRING, "
        "e ENUM('가', '나다'), st SET(VARCHAR(10)))",
    )
    try:
        await _query(
            conn,
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, {{'가나', '다'}})",
            (HANGUL, HANGUL, HANGUL + "ABC", "나다"),
        )
        rows = await _query(
            conn,
            f"SELECT c, v, s, e, st, length(v), length(s), octet_length(v), hex(v) FROM {table}",
        )
        c, v, s, e, st, v_chars, s_chars, v_bytes, v_hex = rows[0]
        # CHAR(4) pads with the EUC-KR full-width space (U+3000).
        assert c.rstrip("　 ") == HANGUL
        assert (v, s, e, st) == (HANGUL, HANGUL + "ABC", "나다", frozenset({"가나", "다"}))
        # Characters, not bytes: the server stored real EUC-KR, not mojibake.
        assert (v_chars, s_chars, v_bytes, v_hex) == (2, 5, 4, "C7D1B1DB")
        assert await _query(conn, f"SELECT v FROM {table} WHERE v = ?", (HANGUL,)) == [(HANGUL,)]
    finally:
        await _query(conn, f"DROP TABLE {table}")


@pytest.mark.asyncio
async def test_varchar_overflow_error_is_readable(conn: Any) -> None:
    table = _table()
    await _query(conn, f"CREATE TABLE {table} (v VARCHAR(4))")
    try:
        with pytest.raises(ProgrammingError) as raised:
            await _query(conn, f"INSERT INTO {table} VALUES (?)", ("가나다라마",))
        assert raised.value.errno == -494
        assert "가나다라마" in raised.value.msg
        assert await _query(conn, "SELECT 1") == [(1,)]
    finally:
        await _query(conn, f"DROP TABLE {table}")


@pytest.mark.asyncio
async def test_korean_identifiers_in_metadata_and_error_echo(conn: Any) -> None:
    # CUBRID's lexer takes non-ASCII identifiers only when quoted.
    table = "표_" + uuid.uuid4().hex[:8]
    with pytest.raises(ProgrammingError) as raised:
        await _query(conn, f"SELECT * FROM [{table}]")
    assert f"{table}" in raised.value.msg
    await _query(conn, f"CREATE TABLE [{table}] ([열] VARCHAR(10) DEFAULT '기본')")
    try:
        await _query(conn, f"INSERT INTO [{table}] DEFAULT VALUES")
        cursor = conn.cursor()
        try:
            await _call(cursor.execute(f"SELECT [열], [열] AS [별칭] FROM [{table}]"))
            assert [column[0] for column in cursor.description] == ["열", "별칭"]
            assert list(await _call(cursor.fetchall())) == [("기본", "기본")]
        finally:
            await _call(cursor.close())
    finally:
        await _query(conn, f"DROP TABLE [{table}]")


@pytest.mark.asyncio
async def test_json_with_hangul_stays_utf8_on_the_wire(conn: Any) -> None:
    table = _table()
    await _query(conn, f"CREATE TABLE {table} (j JSON)")
    try:
        await _query(conn, f"INSERT INTO {table} VALUES (?)", ('{"k": "한글"}',))
        assert await _query(conn, f"SELECT j FROM {table}") == [({"k": HANGUL},)]
    finally:
        await _query(conn, f"DROP TABLE {table}")


@pytest.mark.asyncio
async def test_utf8_column_raises_documented_data_error(conn: Any) -> None:
    table = _table()
    await _query(conn, f"CREATE TABLE {table} (u VARCHAR(10) CHARSET utf8)")
    try:
        await _query(conn, f"INSERT INTO {table} VALUES (?)", (HANGUL,))
        # The CAS sends the value in its column charset (UTF-8 here).
        with pytest.raises(DataError, match="column value is not valid euc_kr"):
            await _query(conn, f"SELECT u FROM {table}")
        assert await _query(conn, f"SELECT hex(u) FROM {table}") == [("ED959CEAB880",)]
        # Documented workaround: convert in SQL to the connection charset.
        converted = f"SELECT CAST(u AS VARCHAR(10) CHARSET euckr) FROM {table}"
        assert await _query(conn, converted) == [(HANGUL,)]
    finally:
        await _query(conn, f"DROP TABLE {table}")


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["\U0001f600", "똠", '{"k": "\U0001f600"}'])
async def test_unencodable_parameter_raises_data_error_and_keeps_the_session(
    conn: Any, value: str
) -> None:
    # Hangul outside KS X 1001 and JSON text are sent through the SQL codec too.
    with pytest.raises(DataError, match="cannot be encoded as euc_kr"):
        await _query(conn, "SELECT ?", (value,))
    assert await _query(conn, "SELECT ?", (HANGUL,)) == [(HANGUL,)]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sync", "async"])
async def test_default_utf8_client_reading_euckr_data_raises_data_error(
    kind: str, euckr_database: None
) -> None:
    writer = await _connect(kind, charset="euckr")
    reader = await _connect(kind)
    table = _table()
    try:
        await _query(writer, f"CREATE TABLE {table} (v VARCHAR(10))")
        await _query(writer, f"INSERT INTO {table} VALUES (?)", (HANGUL,))
        with pytest.raises(DataError, match="column value is not valid UTF-8"):
            await _query(reader, f"SELECT v FROM {table}")
        assert await _query(reader, "SELECT 1") == [(1,)]
    finally:
        await _query(writer, f"DROP TABLE IF EXISTS {table}")
        await _call(reader.close())
        await _call(writer.close())


@pytest.mark.asyncio
async def test_reconnect_keeps_the_connection_codec(conn: Any) -> None:
    conn._drop_connection()
    assert await _call(conn.ping(reconnect=True)) is True
    assert conn._encoding == "euc_kr"
    assert await _query(conn, "SELECT ?, length(?)", (HANGUL, HANGUL)) == [(HANGUL, 2)]


def test_clob_bytes_are_in_the_column_charset(euckr_database: None) -> None:
    # A Korean table name lands in the LOB file locator (EUC-KR bytes).
    table = "[표_" + uuid.uuid4().hex[:8] + "]"
    with pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
        autocommit=True,
        charset="euckr",
    ) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"CREATE TABLE {table} (cl CLOB)")
            cursor.execute(f"INSERT INTO {table} VALUES (CHAR_TO_CLOB(?))", (HANGUL,))
            cursor.execute(f"SELECT cl FROM {table}")
            handle = cursor.fetchone()[0]
            assert "표_" in handle["file_locator"]
            with Lob(conn, CUBRIDDataType.CLOB, handle["packed_lob_handle"]) as lob:
                content = lob.read(handle["lob_length"])
            # LOB content is raw bytes: CLOB text is in the column charset.
            assert content == HANGUL.encode("euc-kr")
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            cursor.close()


def test_prepared_compat_path_and_cubriddb_wrapper_use_the_charset(
    euckr_database: None,
) -> None:
    from pycubrid.compat import cubriddb

    wrapper = cubriddb.connect(
        f"CUBRID:{TEST_HOST}:{TEST_PORT}:{TEST_DB}:::", TEST_USER, TEST_PASSWORD, charset="euckr"
    )
    try:
        cur = wrapper.connection.cursor()
        try:
            cur.prepare("SELECT CAST(? AS VARCHAR(10)), length(?), '가'")
            cur.bind_param(1, HANGUL)
            cur.bind_param(2, HANGUL)
            assert cur.execute() == 1
            assert cur.fetch_row() == (HANGUL, 2, "가")
            with pytest.raises(DataError, match="cannot be encoded as euc_kr"):
                cur.bind_param(1, "\U0001f600")
        finally:
            cur.close()
    finally:
        wrapper.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["\u3164", "\u3164\u3138\u3157\u3141", "한\u3164"])
async def test_hangul_filler_round_trips(conn: Any, value: str) -> None:
    table = _table()
    await _query(conn, f"CREATE TABLE {table} (v VARCHAR(10))")
    try:
        await _query(conn, f"INSERT INTO {table} VALUES (?)", (value,))
        rows = await _query(conn, f"SELECT v, length(v) FROM {table}")
        assert rows == [(value, len(value))]
    finally:
        await _query(conn, f"DROP TABLE {table}")
