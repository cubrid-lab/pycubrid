"""Live native ``connection.schema_info()`` first-row contract on owned tables (#1011)."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import closing
from typing import Any

import pytest

import pycubrid
from pycubrid.compat import native
from pycubrid.exceptions import NotSupportedError
from pycubrid.protocol import GetSchemaPacket

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER
from ._parity_helpers import connect_kwargs, table_name

pytestmark = pytest.mark.integration


def _native() -> native.connection:
    return native.connect(f"CUBRID:{TEST_HOST}:{TEST_PORT}:{TEST_DB}:::", TEST_USER, TEST_PASSWORD)


@pytest.fixture
def tables() -> Generator[tuple[str, str], None, None]:
    table, view = table_name("p1011"), table_name("p1011v")
    with closing(pycubrid.connect(**connect_kwargs(), autocommit=True)) as conn:
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL DEFAULT 'x',"
            " s SET(INT))"
        )
        cur.execute(f"ALTER TABLE {table} ADD CLASS ATTRIBUTE ca INT")
        cur.execute(f"CREATE VIEW {view} AS SELECT id FROM {table}")
        cur.execute(f"INSERT INTO {table} (id) VALUES (1), (2), (3)")
        try:
            yield table, view
        finally:
            cur.execute(f"DROP VIEW IF EXISTS {view}")
            cur.execute(f"DROP TABLE IF EXISTS {table}")


def _bare(name: Any) -> Any:
    # CUBRID 11.x qualifies class names with their owner; 10.2 does not.
    return name.rsplit(".", 1)[-1] if isinstance(name, str) else name


@pytest.mark.parametrize("manual", [False, True])
def test_supported_types_return_the_first_row_or_none(
    tables: tuple[str, str], manual: bool
) -> None:
    table, view = tables
    with closing(_native()) as conn:
        if manual:
            conn.set_autocommit(False)
        row = conn.schema_info(1, table)
        assert type(row) is list and [_bare(row[0]), row[1:]] == [table, [2, None]]
        view_row = conn.schema_info(2, view)
        assert view_row is not None and [_bare(view_row[0]), view_row[1]] == [view, 1]
        assert conn.schema_info(2, table) is None
        assert conn.schema_info(1, f"{table}_missing") is None
        name = conn.schema_info(4, table, "name")
        assert name is not None and len(name) == 14
        assert name[:9] == ["name", 2, 0, 20, 0, 1, 0, 0, "'x'"]
        assert [_bare(name[10]), _bare(name[11]), name[12]] == [table, table, 0]
        assert [type(value) for value in name] == [
            str,
            int,
            int,
            int,
            int,
            int,
            int,
            int,
            str,
            int,
            str,
            str,
            int,
            str,
        ]
        collection = conn.schema_info(4, table, "s")
        assert collection is not None and collection[:2] == ["s", 8]  # element INT, as CCI
        pattern = conn.schema_info(4, table, "na%")
        assert pattern is not None and pattern[0] == "name"
        class_attribute = conn.schema_info(5, table)
        assert class_attribute is not None and class_attribute[:2] == ["ca", 8]
        assert conn.schema_info(5, table, "id") is None
        if manual:
            conn.rollback()


def test_each_call_closes_its_handle_on_the_same_physical_session(
    tables: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    table, _view = tables
    handles: list[int] = []
    original = GetSchemaPacket.parse

    def parse(packet: GetSchemaPacket, data: bytes) -> None:
        original(packet, data)
        handles.append(packet.query_handle)

    monkeypatch.setattr(GetSchemaPacket, "parse", parse)
    with closing(_native()) as conn:
        conn.set_autocommit(False)
        driver = conn._driver
        generation = driver._physical_generation
        for _ in range(3):
            assert conn.schema_info(1, table) is not None
            assert conn.schema_info(1, f"{table}_missing") is None
        # CAS reuses the lowest free server handle: a leaked handle would advance it.
        assert len(handles) == 6 and len(set(handles)) == 1
        assert driver._schema_results == {}
        assert driver._physical_generation == generation
        conn.rollback()


@pytest.mark.parametrize("manual", [False, True])
def test_schema_info_leaves_an_open_prepared_result_usable(
    tables: tuple[str, str], manual: bool
) -> None:
    table, _view = tables
    with closing(_native()) as conn:
        if manual:
            conn.set_autocommit(False)
        cur = conn.cursor()
        try:
            cur.prepare(f"SELECT id FROM {table} ORDER BY id")
            assert cur.execute() == 3
            assert cur.fetch_row() == (1,)
            assert conn.schema_info(4, table, "id") is not None
            assert cur.fetch_row() == (2,)
            assert conn.schema_info(1, f"{table}_missing") is None
            assert cur.fetch_row() == (3,)
            assert cur.fetch_row() is None
        finally:
            cur.close()
        if manual:
            conn.rollback()


def test_unverified_types_fail_without_traffic(tables: tuple[str, str]) -> None:
    table, _view = tables
    with closing(_native()) as conn:
        driver = conn._driver
        generation, cas_info = driver._physical_generation, bytes(driver._cas_info)
        for schema_type in (3, *range(6, 21)):
            with pytest.raises(NotSupportedError):
                conn.schema_info(schema_type, table)
        assert driver._physical_generation == generation
        assert bytes(driver._cas_info) == cas_info
        assert conn.schema_info(1, table) is not None
