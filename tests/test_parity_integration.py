from __future__ import annotations

import datetime
import enum
import json
import os
from collections.abc import Callable
from decimal import Decimal
from typing import cast

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.aio.connection import AsyncConnection
from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import DataError, NotSupportedError
from tests._parity_helpers import (
    ADAPTERS,
    ParityAdapter,
    autocommit_transitions,
    can_connect,
    cleanup_table,
    close_cursor_then_connection,
    connect_kwargs,
    executemany_batch_semantics,
    fetchmany_round_trip,
    insert_identity_values,
    ping_after_drop,
    reuse_session_after_out_tran,
    rollback_rows,
    select_round_trip,
    table_name,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available"),
]

approx = cast(Callable[..., object], getattr(pytest, "approx"))


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


class TestParityBasicTypes:
    @pytest.mark.asyncio
    async def test_integers_and_strings(self, adapter: ParityAdapter) -> None:
        rows = [(1, "hello", 1.5), (2, "world", 2.5), (3, None, None)]
        result = await select_round_trip(adapter, rows, table_prefix="basic")
        assert result == [(1, "hello", 1.5), (2, "world", 2.5), (3, None, None)]

    @pytest.mark.asyncio
    async def test_null_handling(self, adapter: ParityAdapter) -> None:
        rows = [(1, None, None), (2, "", 0.0)]
        result = await select_round_trip(adapter, rows, table_prefix="nulls")
        assert result == [(1, None, None), (2, "", 0.0)]

    @pytest.mark.asyncio
    async def test_large_string(self, adapter: ParityAdapter) -> None:
        big = "x" * 1000
        result = await select_round_trip(
            adapter,
            [(1, big, 3.14)],
            table_prefix="large",
            table_definition="(id INT, name VARCHAR(4096), val DOUBLE)",
        )
        assert result == [(1, big, 3.14)]


class TestParityExecutemany:
    @pytest.mark.asyncio
    async def test_batch_insert(self, adapter: ParityAdapter) -> None:
        rows = [(index, "row_%d" % index, float(index) * 1.1) for index in range(50)]
        result = await select_round_trip(adapter, rows, table_prefix="batch_insert")
        assert len(result) == len(rows)
        for actual, expected in zip(result, rows, strict=True):
            assert actual[:2] == expected[:2]
            # DOUBLE round-trips can normalize the final binary float bits.
            assert actual[2] == approx(expected[2], abs=1e-12)


class TestParityTransactions:
    @pytest.mark.asyncio
    async def test_rollback_discards_inserts(self, adapter: ParityAdapter) -> None:
        assert await rollback_rows(adapter) == []


class TestParityFetchMethods:
    @pytest.mark.asyncio
    async def test_fetchmany_parity(self, adapter: ParityAdapter) -> None:
        batch_one, batch_two, remaining = await fetchmany_round_trip(adapter)
        assert batch_one == [(0, "item_0", 0.0), (1, "item_1", 1.0), (2, "item_2", 2.0)]
        assert batch_two == [(3, "item_3", 3.0), (4, "item_4", 4.0), (5, "item_5", 5.0)]
        assert remaining == [
            (6, "item_6", 6.0),
            (7, "item_7", 7.0),
            (8, "item_8", 8.0),
            (9, "item_9", 9.0),
        ]


class TestParityBytes:
    @pytest.mark.asyncio
    async def test_blob_round_trip(self, adapter: ParityAdapter) -> None:
        payload = bytes(range(256)) * 4
        result = await select_round_trip(
            adapter,
            [(1, payload)],
            table_prefix="blob",
            table_definition="(id INT, payload BIT VARYING(8192))",
            insert_sql="INSERT INTO {table} (id, payload) VALUES (?, ?)",
            select_sql="SELECT payload FROM {table} WHERE id = 1",
            autocommit=True,
        )
        assert result == [(payload,)]

    @pytest.mark.asyncio
    async def test_datetime_round_trip(self, adapter: ParityAdapter) -> None:
        value = datetime.datetime(2025, 6, 15, 10, 30, 45)
        result = await select_round_trip(
            adapter,
            [(1, value, datetime.date(2025, 6, 15), datetime.time(10, 30, 45))],
            table_prefix="datetime",
            table_definition="(id INT, dt DATETIME, d DATE, t TIME)",
            insert_sql="INSERT INTO {table} VALUES (?, ?, ?, ?)",
            select_sql="SELECT dt, d, t FROM {table} WHERE id = 1",
            autocommit=True,
        )
        assert result == [(value, datetime.date(2025, 6, 15), datetime.time(10, 30, 45))]

    @pytest.mark.asyncio
    async def test_large_fetch_size(self, adapter: ParityAdapter) -> None:
        rows = [(index, "row_%d" % index, float(index)) for index in range(200)]
        result = await select_round_trip(
            adapter,
            rows,
            table_prefix="fetch_size",
            autocommit=True,
            fetch_size=500,
        )
        assert result == rows

    @pytest.mark.asyncio
    async def test_json_column(self, adapter: ParityAdapter) -> None:
        payload = {"key": "value", "number": 42, "nested": [1, 2, 3]}
        result = await select_round_trip(
            adapter,
            [(1, json.dumps(payload))],
            table_prefix="json",
            table_definition="(id INT, payload JSON)",
            insert_sql="INSERT INTO {table} VALUES (?, ?)",
            select_sql="SELECT payload FROM {table} WHERE id = 1",
            autocommit=True,
            json_deserializer=json.loads,
        )
        assert result == [(payload,)]


class TestParityDecimalLiterals:
    """Decimal parameters stay NUMERIC on the server (#517)."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "value, precision, scale",
        [
            (Decimal("0.0000001"), 7, 7),
            (Decimal("-1.5E-3"), 4, 4),
            (Decimal("1.10"), 3, 2),
            (Decimal("-0.00"), 2, 2),
            (Decimal("1.23456789012345678901234E-7"), 30, 30),
            (Decimal("1E-38"), 38, 38),
            (Decimal("9" * 38), 38, 0),
        ],
        ids=["1E-7", "negative", "trailing-zero", "negative-zero", "scale-30", "scale-38", "p38"],
    )
    async def test_select_parameter_is_numeric(
        self,
        adapter: ParityAdapter,
        value: Decimal,
        precision: int,
        scale: int,
    ) -> None:
        conn = await adapter.connect()
        cur = adapter.cursor(conn)
        try:
            await adapter.execute(cur, "SELECT ?, TYPEOF(?)", [value, value])
            row = await adapter.fetchone(cur)
            description = cur.description
        finally:
            await adapter.close_cursor(cur)
            await adapter.close_connection(conn)
        assert row is not None
        assert description is not None
        assert description[0][1] == CUBRIDDataType.NUMERIC
        assert row[1] == "numeric (%d, %d)" % (precision, scale)
        fetched = row[0]
        assert isinstance(fetched, Decimal)
        assert fetched == value
        assert fetched.as_tuple().exponent == min(int(value.as_tuple().exponent), 0)

    @pytest.mark.asyncio
    async def test_numeric_38_30_round_trip_and_overflow(self, adapter: ParityAdapter) -> None:
        exact = Decimal("1.23456789012345678901234E-7")
        table = table_name("dec517")
        conn = await adapter.connect()
        cur = adapter.cursor(conn)
        try:
            await adapter.execute(cur, "DROP TABLE IF EXISTS %s" % table)
            await adapter.execute(cur, "CREATE TABLE %s (id INT, v NUMERIC(38,30))" % table)
            await adapter.execute(cur, "INSERT INTO %s VALUES (?, ?)" % table, [1, exact])
            with pytest.raises(DataError, match="at most 38 digits"):
                await adapter.execute(
                    cur,
                    "INSERT INTO %s VALUES (?, ?)" % table,
                    [2, Decimal("1E-39")],
                )
            await adapter.execute(cur, "SELECT id, v FROM %s ORDER BY id" % table)
            rows = await adapter.fetchall(cur)
            await adapter.commit(conn)
        finally:
            await adapter.close_cursor(cur)
            await cleanup_table(adapter, conn, table)
            await adapter.close_connection(conn)
        assert rows == [(1, exact)]
        assert str(rows[0][1]) == "1.23456789012345678901234E-7"


class _Color(enum.IntEnum):
    RED = 1


class _Perm(enum.IntFlag):
    R = 4
    W = 2


class _HostileInt(int):
    def __str__(self) -> str:
        return "1; DROP TABLE t"

    __repr__ = __str__


class _HostileFloat(float):
    def __str__(self) -> str:
        return "1; DROP TABLE t"

    __repr__ = __str__


class _HostileDecimal(Decimal):
    def __str__(self) -> str:
        return "1; DROP TABLE t"

    def __format__(self, spec: str, *args: object) -> str:
        return "1; DROP TABLE t"


class TestParityNumericSubclassLiterals:
    """int/float/Decimal subclasses are bound by value, not str() (#518)."""

    @pytest.mark.asyncio
    async def test_subclasses_round_trip_by_value(self, adapter: ParityAdapter) -> None:
        values = [
            _Color.RED,
            _Perm.R | _Perm.W,
            _HostileInt(42),
            _HostileFloat(2.5),
            _HostileDecimal("0.0000001"),
        ]
        table = table_name("num518")
        conn = await adapter.connect()
        cur = adapter.cursor(conn)
        try:
            await adapter.execute(cur, "SELECT ?, ?, ?, ?, ?", values)
            selected = await adapter.fetchone(cur)
            await adapter.execute(cur, "DROP TABLE IF EXISTS %s" % table)
            await adapter.execute(
                cur,
                "CREATE TABLE %s (a INT, b INT, c BIGINT, d DOUBLE, e NUMERIC(38,30))" % table,
            )
            await adapter.execute(cur, "INSERT INTO %s VALUES (?, ?, ?, ?, ?)" % table, values)
            await adapter.execute(cur, "SELECT a, b, c, d, e FROM %s" % table)
            stored = await adapter.fetchall(cur)
            await adapter.commit(conn)
        finally:
            await adapter.close_cursor(cur)
            await cleanup_table(adapter, conn, table)
            await adapter.close_connection(conn)
        expected = (1, 6, 42, 2.5, Decimal("0.0000001"))
        assert selected == expected
        assert type(selected[0]) is int
        assert isinstance(selected[4], Decimal)
        assert stored == [expected]


class TestParityConnectionLifecycle:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("reconnect", [False, True], ids=["no-reconnect", "reconnect"])
    async def test_ping_healthy_connection(
        self,
        adapter: ParityAdapter,
        reconnect: bool,
    ) -> None:
        conn = await adapter.connect()
        try:
            assert await adapter.ping(conn, reconnect=reconnect) is True
        finally:
            await adapter.close_connection(conn)

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not os.getenv("CUBRID_TEST_URL"),
        reason="Set CUBRID_TEST_URL to run broker drop parity scenarios",
    )
    @pytest.mark.parametrize("reconnect", [False, True], ids=["no-reconnect", "reconnect"])
    async def test_ping_after_transport_drop(
        self,
        adapter: ParityAdapter,
        reconnect: bool,
    ) -> None:
        ping_result, row = await ping_after_drop(adapter, reconnect=reconnect)
        assert ping_result is reconnect
        expected_row = (1,) if reconnect else None
        assert row == expected_row

    @pytest.mark.asyncio
    async def test_out_tran_keeps_session(self, adapter: ParityAdapter) -> None:
        same_session, row, version = await reuse_session_after_out_tran(adapter)
        assert same_session is True
        assert row == (1,)
        assert version

    @pytest.mark.asyncio
    async def test_autocommit_transitions(self, adapter: ParityAdapter) -> None:
        assert await autocommit_transitions(adapter) == (False, True, False)

    @pytest.mark.asyncio
    async def test_lastrowid_and_last_insert_id(self, adapter: ParityAdapter) -> None:
        lastrowid, last_insert_id = await insert_identity_values(adapter)
        assert isinstance(lastrowid, int)
        assert lastrowid is not None and lastrowid > 0
        assert last_insert_id == str(lastrowid)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("rows", [1, 2])
    @pytest.mark.parametrize("boundary", ["commit", "rollback"])
    async def test_insert_identity_survives_transaction_boundary(
        self, adapter: ParityAdapter, rows: int, boundary: str
    ) -> None:
        table = table_name("identity_boundary")
        conn = await adapter.connect()
        cur = adapter.cursor(conn)
        observer = adapter.cursor(conn)
        try:
            assert await adapter.get_last_insert_id(conn) is None
            await adapter.execute(cur, "CREATE TABLE %s (id INT AUTO_INCREMENT, v INT)" % table)
            await adapter.commit(conn)
            sql = "INSERT INTO %s (v) VALUES (10)" % table
            if rows == 2:
                sql += ", (20)"
            await adapter.execute(cur, sql)
            captured = adapter.lastrowid(cur)
            assert captured == 1  # CUBRID reports the first identity for multi-row INSERT.
            assert await adapter.get_last_insert_id(conn) == "1"
            await adapter.execute(observer, "SELECT id FROM %s ORDER BY id" % table)
            assert await adapter.fetchall(observer) == [(number,) for number in range(1, rows + 1)]
            if boundary == "commit":
                await adapter.commit(conn)
            else:
                await adapter.rollback(conn)
            assert adapter.lastrowid(cur) == captured
            assert await adapter.get_last_insert_id(conn) == "1"
            transport = conn._writer if isinstance(conn, AsyncConnection) else conn._socket
            await adapter.execute(observer, "SELECT COUNT(*) FROM %s" % table)
            assert await adapter.fetchone(observer) == (rows if boundary == "commit" else 0,)
            current = conn._writer if isinstance(conn, AsyncConnection) else conn._socket
            # Normal END_TRAN does not replace the physical session or identity cache.
            assert current is transport
            assert await adapter.get_last_insert_id(conn) == "1"
            assert adapter.lastrowid(cur) == captured
        finally:
            try:
                await cleanup_table(adapter, conn, table)
            finally:
                await adapter.close_connection(conn)

    @pytest.mark.asyncio
    async def test_non_auto_insert_preserves_broker_identity_semantics(
        self, adapter: ParityAdapter
    ) -> None:
        auto_table = table_name("identity_auto")
        plain_table = table_name("identity_plain")
        conn = await adapter.connect()
        cur = adapter.cursor(conn)
        try:
            await adapter.execute(
                cur, "CREATE TABLE %s (id INT AUTO_INCREMENT, v INT)" % auto_table
            )
            await adapter.execute(cur, "CREATE TABLE %s (v INT)" % plain_table)
            await adapter.commit(conn)
            await adapter.execute(cur, "INSERT INTO %s VALUES (1)" % plain_table)
            assert adapter.lastrowid(cur) is None
            assert await adapter.get_last_insert_id(conn) is None
            await adapter.execute(cur, "INSERT INTO %s (v) VALUES (1)" % auto_table)
            assert await adapter.get_last_insert_id(conn) == "1"
            await adapter.execute(cur, "INSERT INTO %s VALUES (2)" % plain_table)
            # Existing broker behavior: no metadata distinguishes a retained id.
            assert adapter.lastrowid(cur) == 1
            assert await adapter.get_last_insert_id(conn) == "1"
            await adapter.commit(conn)
            await adapter.execute(cur, "INSERT INTO %s VALUES (3)" % plain_table)
            # The same physical session can retain the earlier broker ID even
            # after END_TRAN; a plain INSERT is not proof of a new identity.
            assert adapter.lastrowid(cur) == 1
            assert await adapter.get_last_insert_id(conn) == "1"
        finally:
            try:
                await cleanup_table(adapter, conn, auto_table)
                await cleanup_table(adapter, conn, plain_table)
            finally:
                await adapter.close_connection(conn)

    @pytest.mark.asyncio
    async def test_get_server_version(self, adapter: ParityAdapter) -> None:
        conn = await adapter.connect()
        try:
            version = await adapter.get_server_version(conn)
        finally:
            await adapter.close_connection(conn)
        assert isinstance(version, str)
        assert version
        assert version.split(".", 1)[0].isdigit()

    @pytest.mark.asyncio
    async def test_executemany_batch_rowcount_semantics(self, adapter: ParityAdapter) -> None:
        results, rowcount, count_row = await executemany_batch_semantics(adapter)
        assert len(results) == 3
        assert rowcount == sum(count for _, count in results)
        assert count_row == (2,)

    @pytest.mark.asyncio
    async def test_cursor_close_then_connection_close(self, adapter: ParityAdapter) -> None:
        assert await close_cursor_then_connection(adapter) == (True, True)

    @pytest.mark.asyncio
    async def test_async_connection_create_lob_raises_not_supported(self) -> None:
        sync_conn = pycubrid.connect(**connect_kwargs())
        async_conn = await pycubrid.aio.connect(**connect_kwargs())
        try:
            assert callable(sync_conn.create_lob)
            with pytest.raises(NotSupportedError, match="not supported on async connections"):
                async_conn.create_lob(CUBRIDDataType.BLOB)
        finally:
            sync_conn.close()
            await async_conn.close()
