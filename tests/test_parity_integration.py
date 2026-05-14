from __future__ import annotations

import datetime
import json
import os
from collections.abc import Callable
from typing import cast

import pytest

import pycubrid
import pycubrid.aio
from tests._parity_helpers import (
    ADAPTERS,
    ParityAdapter,
    autocommit_transitions,
    can_connect,
    close_cursor_then_connection,
    connect_kwargs,
    executemany_batch_semantics,
    fetchmany_round_trip,
    insert_identity_values,
    ping_after_drop,
    reconnect_after_inactive_cas,
    rollback_rows,
    select_round_trip,
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
    async def test_reconnect_after_inactive_cas(self, adapter: ParityAdapter) -> None:
        reconnected, row, version = await reconnect_after_inactive_cas(adapter)
        assert reconnected is True
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
    async def test_async_connection_has_no_create_lob_method(self) -> None:
        sync_conn = pycubrid.connect(**connect_kwargs())
        async_conn = await pycubrid.aio.connect(**connect_kwargs())
        try:
            assert callable(sync_conn.create_lob)
            with pytest.raises(AttributeError, match="create_lob"):
                getattr(async_conn, "create_lob")()
        finally:
            sync_conn.close()
            await async_conn.close()
