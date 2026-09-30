"""Source/DDL-derived schema rows on real CUBRID, not native-driver parity.

Metadata: CUBRID/cubrid cas_schema_info.c at 6b2bc755 (11.4) and d56a158c
(10.2). Filter/order/DOMAIN encoding: cas_execute.c; FK actions: storage_common.h.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio

import pycubrid
import pycubrid.aio
from pycubrid.aio.connection import AsyncConnection
from pycubrid.constants import CCISchemaType, CUBRIDDataType
from pycubrid.protocol import CloseQueryPacket, FetchPacket
from tests.test_schema_integration import call

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]
STRING, SHORT, INT = CUBRIDDataType.STRING, CUBRIDDataType.SHORT, CUBRIDDataType.INT
CLASS_COLUMNS = [("NAME", STRING, 255), ("TYPE", SHORT, 0), ("REMARKS", STRING, 2048)]
ATTRIBUTE_COLUMNS = [
    ("ATTR_NAME", STRING, 255),
    ("DOMAIN", SHORT, 0),
    ("SCALE", SHORT, 0),
    ("PRECISION", INT, 0),
    ("INDEXED", SHORT, 0),
    ("NON_NULL", SHORT, 0),
    ("SHARED", SHORT, 0),
    ("UNIQUE", SHORT, 0),
    ("DEFAULT", STRING, 0),
    ("ATTR_ORDER", INT, 0),
    ("CLASS_NAME", STRING, 255),
    ("SOURCE_CLASS", STRING, 255),
    ("IS_KEY", SHORT, 0),
    ("REMARKS", STRING, 1024),
]
CONSTRAINT_COLUMNS = [
    ("TYPE", SHORT, 0),
    ("NAME", STRING, 255),
    ("ATTR_NAME", STRING, 255),
    ("NUM_PAGES", INT, 0),
    ("NUM_KEYS", INT, 0),
    ("PRIMARY_KEY", SHORT, 0),
    ("KEY_ORDER", SHORT, 0),
    ("ASC_DESC", STRING, 0),
]
PRIMARY_COLUMNS = [
    ("CLASS_NAME", STRING, 255),
    ("ATTR_NAME", STRING, 255),
    ("KEY_SEQ", INT, 0),
    ("KEY_NAME", STRING, 255),
]
FOREIGN_COLUMNS = [
    ("PKTABLE_NAME", STRING, 255),
    ("PKCOLUMN_NAME", STRING, 255),
    ("FKTABLE_NAME", STRING, 255),
    ("FKCOLUMN_NAME", STRING, 255),
    ("KEY_SEQ", SHORT, 0),
    ("UPDATE_RULE", SHORT, 0),
    ("DELETE_RULE", SHORT, 0),
    ("FK_NAME", STRING, 255),
    ("PK_NAME", STRING, 255),
]
EXTRA_NAMES = ["c%02d" % position for position in range(8)]


def assert_columns(packet: Any, expected: list[tuple[str, int, int]]) -> None:
    assert [(c.name, c.column_type, c.scale, c.precision) for c in packet.columns] == [
        (name, column_type, 0, precision) for name, column_type, precision in expected
    ]


def local_name(value: str) -> str:
    # Preserve the raw API; test-owned identifiers can be owner-qualified in 11.4.
    parts = value.split(".")
    assert len(parts) == 1 or parts[:-1] == [TEST_USER.lower()]
    return parts[-1]


async def schema_rows(conn: Any, kind: int, name: str, flags: int = 0, **kwargs: Any) -> Any:
    packet = await call(conn, "get_schema_info", kind, name, flags, **kwargs)
    try:
        rows = await call(conn, "fetch_schema_info", packet)
        assert len(rows) == packet.tuple_count
        assert all(len(row) == len(packet.columns) for row in rows)
        return packet, rows
    finally:
        await call(conn, "close_schema_info", packet)


@pytest_asyncio.fixture(params=["sync", "async"])
async def owned_schema(request: pytest.FixtureRequest) -> AsyncIterator[tuple[Any, Any, str]]:
    config = dict(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
        no_backslash_escapes=True,
        read_timeout=5,
        connect_timeout=5,
        fetch_size=2,
    )
    conn = (
        await pycubrid.aio.connect(**config)
        if request.param == "async"
        else pycubrid.connect(**config)
    )
    prefix = "s457_" + uuid.uuid4().hex[:16]
    parent, child, view = [prefix + suffix for suffix in ("_parent", "_child", "_view")]
    cursor = await call(conn, "cursor")
    try:
        extra_columns = ", ".join(name + " INTEGER" for name in EXTRA_NAMES)
        await call(
            cursor,
            "execute",
            (
                "CREATE TABLE %s (part_a INTEGER, part_b INTEGER, "
                "label VARCHAR(40) DEFAULT 'base' NOT NULL, %s, "
                "CONSTRAINT %s_pk PRIMARY KEY(part_b, part_a))"
            )
            % (parent, extra_columns, prefix),
        )
        await call(cursor, "execute", "CREATE INDEX %s_idx ON %s(label, c00)" % (prefix, parent))
        await call(
            cursor,
            "execute",
            (
                "CREATE TABLE %s (ref_b INTEGER, ref_a INTEGER, CONSTRAINT %s_fk "
                "FOREIGN KEY(ref_b, ref_a) REFERENCES %s(part_b, part_a) ON DELETE CASCADE)"
            )
            % (child, prefix, parent),
        )
        await call(
            cursor,
            "execute",
            "CREATE VIEW %s AS SELECT part_a, part_b, label FROM %s" % (view, parent),
        )
        await call(cursor, "execute", "INSERT INTO %s(part_a, part_b) VALUES(10, 20)" % parent)
        await call(cursor, "execute", "INSERT INTO %s VALUES(20, 10)" % child)
        await call(conn, "commit")
        request.node.user_properties.extend(
            [
                ("server_version", await call(conn, "get_server_version")),
                ("owned_prefix", prefix),
            ]
        )
        yield conn, cursor, prefix
    finally:
        try:
            await call(conn, "rollback")
            for kind, name in (("VIEW", view), ("TABLE", child), ("TABLE", parent)):
                await call(cursor, "execute", "DROP %s IF EXISTS %s" % (kind, name))
            await call(conn, "commit")
            _, remaining = await schema_rows(conn, CCISchemaType.CLASS, prefix + "%", 1)
            assert remaining == []
        finally:
            await call(conn, "close")


async def test_schema_tables_views_and_empty_filters(owned_schema: tuple[Any, Any, str]) -> None:
    conn, cursor, prefix = owned_schema
    parent, child, view = [prefix + suffix for suffix in ("_parent", "_child", "_view")]
    packet, rows = await schema_rows(conn, CCISchemaType.CLASS, parent)
    assert_columns(packet, CLASS_COLUMNS)
    assert [(local_name(row[0]), row[1]) for row in rows] == [(parent, 2)]
    packet, rows = await schema_rows(conn, CCISchemaType.CLASS, prefix + "%", 1)
    assert_columns(packet, CLASS_COLUMNS)
    assert sorted((local_name(row[0]), row[1]) for row in rows) == sorted(
        [(parent, 2), (child, 2), (view, 1)]
    )
    for flags, name in ((0, view), (1, prefix + "%")):
        packet, rows = await schema_rows(conn, CCISchemaType.VCLASS, name, flags)
        assert_columns(packet, CLASS_COLUMNS)
        assert [(local_name(row[0]), row[1]) for row in rows] == [(view, 1)]
    for kind in (CCISchemaType.CLASS, CCISchemaType.VCLASS):
        _, rows = await schema_rows(conn, kind, prefix + "_missing")
        assert rows == []
    await call(cursor, "execute", "SELECT part_a, part_b, label FROM %s" % view)
    assert await call(cursor, "fetchall") == [(10, 20, "base")]


async def test_schema_attribute_filters_and_real_multiple_fetches(
    owned_schema: tuple[Any, Any, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, _, prefix = owned_schema
    parent = prefix + "_parent"
    pages: list[tuple[int, int, int, int, bool]] = []
    closes: list[tuple[int, bool]] = []

    def observe(packet: Any, reconnect: bool) -> None:
        if isinstance(packet, FetchPacket):
            pages.append(
                (
                    packet.query_handle,
                    packet.current_tuple_count,
                    packet.fetch_size,
                    len(packet.rows),
                    reconnect,
                )
            )
        elif isinstance(packet, CloseQueryPacket):
            closes.append((packet.query_handle, reconnect))

    with monkeypatch.context() as spy:
        transport = (
            "_send_and_receive_locked" if isinstance(conn, AsyncConnection) else "_send_and_receive"
        )
        original = getattr(conn, transport)
        if isinstance(conn, AsyncConnection):

            async def forward(packet: Any, *, allow_reconnect: bool = True) -> Any:
                result = await original(packet, allow_reconnect=allow_reconnect)
                observe(packet, allow_reconnect)
                return result
        else:

            def forward(packet: Any, *, allow_reconnect: bool = True) -> Any:
                result = original(packet, allow_reconnect=allow_reconnect)
                observe(packet, allow_reconnect)
                return result

        spy.setattr(conn, transport, forward)
        packet, rows = await schema_rows(conn, CCISchemaType.ATTRIBUTE, parent, 2, arg2="%")
    assert_columns(packet, ATTRIBUTE_COLUMNS)
    expected_names = ["part_a", "part_b", "label", *EXTRA_NAMES]
    assert [row[0] for row in rows] == expected_names
    assert [row[9] for row in rows] == list(range(1, len(expected_names) + 1))
    assert len(pages) >= 2
    assert pages == [
        (packet.query_handle, offset, 2, min(2, len(expected_names) - offset), False)
        for offset in range(0, len(expected_names), 2)
    ]
    assert closes == [(packet.query_handle, False)]
    for row in rows:
        assert local_name(row[10]) == local_name(row[11]) == parent
        assert row[1] & 0xFF == (STRING if row[0] == "label" else INT)
        assert row[2] == 0
        assert row[3] == (40 if row[0] == "label" else 10)
        assert row[5] == int(row[0] in ("part_a", "part_b", "label"))
        assert row[8] == ("'base'" if row[0] == "label" else "NULL")
    for flags, name, arg2, expected in (
        (0, parent, "label", ["label"]),
        (2, parent, "c0%", EXTRA_NAMES),
        (3, prefix + "_par%", "c0%", EXTRA_NAMES),
        (0, parent, None, []),
        (0, parent, "", []),
        (2, parent, "missing%", []),
    ):
        _, filtered = await schema_rows(conn, CCISchemaType.ATTRIBUTE, name, flags, arg2=arg2)
        assert [row[0] for row in filtered] == expected


async def test_schema_named_index_columns(owned_schema: tuple[Any, Any, str]) -> None:
    conn, _, prefix = owned_schema
    packet, rows = await schema_rows(conn, CCISchemaType.CONSTRAINT, prefix + "_parent")
    assert_columns(packet, CONSTRAINT_COLUMNS)
    # CAS type11 reports index families, not every DDL constraint. Page/key values
    # are not meaningful statistics in this path and are intentionally not fixed.
    assert [(r[0], r[1], r[2], r[5], r[6], r[7]) for r in rows] == [
        (1, prefix + "_idx", "label", 0, 1, "A"),
        (1, prefix + "_idx", "c00", 0, 2, "A"),
    ]


async def test_schema_composite_primary_and_foreign_keys(
    owned_schema: tuple[Any, Any, str],
) -> None:
    conn, _, prefix = owned_schema
    parent, child, pk, fk = [prefix + suffix for suffix in ("_parent", "_child", "_pk", "_fk")]
    packet, rows = await schema_rows(conn, CCISchemaType.PRIMARY_KEY, parent)
    assert_columns(packet, PRIMARY_COLUMNS)
    # CAS sorts PK rows by attribute name; KEY_SEQ still describes declared order.
    assert [(local_name(r[0]), *r[1:]) for r in rows] == [
        (parent, "part_a", 2, pk),
        (parent, "part_b", 1, pk),
    ]
    expected = [
        (parent, "part_b", child, "ref_b", 1, 1, 0, fk, pk),
        (parent, "part_a", child, "ref_a", 2, 1, 0, fk, pk),
    ]
    for kind, name in ((CCISchemaType.IMPORTED_KEYS, child), (CCISchemaType.EXPORTED_KEYS, parent)):
        packet, rows = await schema_rows(conn, kind, name)
        assert_columns(packet, FOREIGN_COLUMNS)
        # storage_common.h: RESTRICT=1 (default update), CASCADE=0 (explicit delete).
        assert [(local_name(r[0]), r[1], local_name(r[2]), *r[3:]) for r in rows] == expected
    for kind, name in (
        (CCISchemaType.PRIMARY_KEY, child),
        (CCISchemaType.IMPORTED_KEYS, parent),
        (CCISchemaType.EXPORTED_KEYS, child),
    ):
        _, rows = await schema_rows(conn, kind, name)
        assert rows == []
