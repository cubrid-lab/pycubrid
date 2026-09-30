"""SET / MULTISET / SEQUENCE live CRUD + decode parity (#403).

Most tests insert collections as SQL literals. Plain Python containers stay
rejected as parameters; ``TestTypedCollectionParameters`` binds the typed
``pycubrid.types.Set``/``Multiset``/``Sequence`` parameters (#567).
"""

from __future__ import annotations

import datetime
import uuid
from collections.abc import Generator
from decimal import Decimal
from typing import Any

import pycubrid
import pycubrid.aio
import pytest
from pycubrid.connection import Connection
from pycubrid.cursor import Cursor
from pycubrid.types import Multiset, Sequence, Set

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER

pytestmark = pytest.mark.integration


def _connect(**kwargs: Any) -> Connection:
    return pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
        **kwargs,
    )


@pytest.fixture
def conn() -> Generator[Connection, None, None]:
    c = _connect(decode_collections=True)
    yield c
    c.close()


@pytest.fixture
def cursor(conn: Connection) -> Generator[Cursor, None, None]:
    cur = conn.cursor()
    yield cur
    cur.close()


def _tbl() -> str:
    return "pycubrid_coll_%s" % uuid.uuid4().hex[:8]


class TestCollectionCRUD:
    def test_set_of_int(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (a SET(INT))" % table)
            cursor.execute("INSERT INTO %s VALUES ({1,2,3})" % table)
            cursor.execute("SELECT * FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            # SET: no duplicates, order may vary
            assert isinstance(row[0], frozenset)
            assert row[0] == frozenset({1, 2, 3})
            assert cursor.description is not None
            assert cursor.description[0][1] == 16
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_multiset_of_int(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (a MULTISET(INT))" % table)
            cursor.execute("INSERT INTO %s VALUES ({1,1,2})" % table)
            cursor.execute("SELECT * FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            # MULTISET preserves duplicates
            assert isinstance(row[0], list)
            assert sorted(row[0]) == [1, 1, 2]
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_sequence_of_int(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (a SEQUENCE(INT))" % table)
            cursor.execute("INSERT INTO %s VALUES ({1,2,3})" % table)
            cursor.execute("SELECT * FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            # SEQUENCE (LIST) preserves order
            assert isinstance(row[0], list)
            assert row[0] == [1, 2, 3]
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_empty_collection(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (a SET(INT))" % table)
            cursor.execute("INSERT INTO %s VALUES ({})" % table)
            cursor.execute("SELECT * FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            # Empty set
            assert row[0] == frozenset()
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_null_collection(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (a SET(INT))" % table)
            cursor.execute("INSERT INTO %s VALUES (NULL)" % table)
            cursor.execute("SELECT * FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            assert row[0] is None
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_mixed_columns(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (id INT, tags SET(VARCHAR(20)))" % table)
            cursor.execute("INSERT INTO %s VALUES (1, {'alpha','beta','gamma'})" % table)
            cursor.execute("SELECT id, tags FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 1
            assert set(row[1]) == {"alpha", "beta", "gamma"}
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_collection_predicate(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (a SET(INT), b MULTISET(INT), c SEQUENCE(INT))" % table)
            cursor.execute(
                "INSERT INTO %s VALUES "
                "({},{},{}),(NULL,NULL,NULL),({1,1},{1,1},{1,1}),"
                "({1,2,3},{1,2,3},{1,2,3})" % table
            )
            cursor.execute("SELECT * FROM %s WHERE a SETEQ {'1'} ORDER BY 1" % table)
            rows = cursor.fetchall()
            assert rows == [(frozenset({1}), [1, 1], [1, 1])]
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)


class TestNullOnlyCollections:
    """Nonempty collections whose elements are all SQL NULL (#483)."""

    def test_null_only_and_mixed_rows(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute(
                "CREATE TABLE %s (id INT, s SET(INT), m MULTISET(INT), "
                "q SEQUENCE(INT), l LIST(VARCHAR(10)))" % table
            )
            cursor.execute(
                "INSERT INTO %s VALUES "
                "(1, {NULL}, {NULL}, {NULL}, {NULL}),"
                "(2, {NULL,NULL}, {NULL,NULL}, {NULL,NULL}, {NULL,NULL}),"
                "(3, {1,NULL}, {NULL,1,NULL}, {NULL,2,NULL}, {'a',NULL}),"
                "(4, {}, {}, {}, {})" % table
            )
            cursor.execute("SELECT * FROM %s ORDER BY id" % table)
            assert cursor.fetchall() == [
                (1, frozenset({None}), [None], [None], [None]),
                (2, frozenset({None}), [None, None], [None, None], [None, None]),
                (3, frozenset({1, None}), [1, None, None], [None, 2, None], ["a", None]),
                (4, frozenset(), [], [], []),
            ]
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_null_only_literals(self, cursor: Cursor) -> None:
        cursor.execute(
            "SELECT {NULL}, {NULL,NULL}, CAST({NULL} AS SET(INT)), "
            "CAST({NULL,NULL} AS MULTISET(INT)), "
            "CAST({NULL,NULL} AS SEQUENCE(VARCHAR(5))), {NULL,1}, {}"
        )
        assert cursor.fetchone() == (
            [None],
            [None, None],
            frozenset({None}),
            [None, None],
            [None, None],
            [None, 1],
            [],
        )

    def test_null_only_raw_bytes_unchanged(self) -> None:
        with _connect(decode_collections=False) as conn:
            cur = conn.cursor()
            cur.execute("SELECT {NULL,NULL}, CAST({NULL} AS SET(INT))")
            assert cur.fetchone() == (
                bytes.fromhex("0000000002ffffffffffffffff"),
                bytes.fromhex("0000000001ffffffff"),
            )
            cur.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("decode", [False, True])
    async def test_async_null_only_collections(self, decode: bool) -> None:
        conn = await pycubrid.aio.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
            decode_collections=decode,
        )
        async with conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT CAST({NULL,NULL} AS SET(INT)), "
                    "CAST({NULL,NULL} AS MULTISET(INT)), CAST({NULL} AS SEQUENCE(INT))"
                )
                row = await cur.fetchone()
                if decode:
                    assert row == (frozenset({None}), [None, None], [None])
                else:
                    assert row == (
                        bytes.fromhex("0000000002ffffffffffffffff"),
                        bytes.fromhex("0000000002ffffffffffffffff"),
                        bytes.fromhex("0000000001ffffffff"),
                    )


class TestCollectionDecodeFlag:
    def test_decode_collections_false(self) -> None:
        with _connect(decode_collections=False) as conn:
            cur = conn.cursor()
            table = _tbl()
            try:
                cur.execute("CREATE TABLE %s (a SET(INT))" % table)
                cur.execute("INSERT INTO %s VALUES ({1,2,3})" % table)
                cur.execute("SELECT * FROM %s" % table)
                row = cur.fetchone()
                assert row is not None
                # With decode_collections=False, raw bytes are returned
                assert isinstance(row[0], (bytes, bytearray))
            finally:
                cur.execute("DROP TABLE IF EXISTS %s" % table)
                cur.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("decode", [False, True])
    async def test_async_collection_decode(self, decode: bool) -> None:
        conn = await pycubrid.aio.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
            decode_collections=decode,
        )
        async with conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT CAST({1,2,1} AS SET(INT)), "
                    "CAST({1,1,2} AS MULTISET(INT)), CAST({2,1,2} AS SEQUENCE(INT)), 42"
                )
                row = await cur.fetchone()
                assert row is not None
                assert cur.description is not None
                assert [col[1] for col in cur.description] == [16, 17, 18, 8]
                if decode:
                    assert row == (frozenset({1, 2}), [1, 1, 2], [2, 1, 2], 42)
                else:
                    assert all(isinstance(value, bytes) for value in row[:3])
                    assert row[3] == 42


_INJECTED = "x'}); DROP TABLE t; --"


def _inject(*args: object, **kwargs: object) -> str:
    return _INJECTED


class _EvilStr(str):
    """A str whose overridable text methods lie; only its characters may bind."""

    replace = _inject
    __str__ = _inject
    __format__ = _inject


class _EvilInt(int):
    __str__ = _inject
    __repr__ = _inject
    __format__ = _inject


class TestTypedCollectionParameters:
    """Typed Set/Multiset/Sequence parameters round-trip through the server (#567)."""

    _DDL = (
        "CREATE TABLE %s (id INT, s SET(INT), m MULTISET(VARCHAR(20)), q SEQUENCE(INT), "
        "d SET(DATE), n SEQUENCE(NUMERIC(10,2)), b SET(BIT VARYING(64)), "
        "f SEQUENCE(DOUBLE), dt SEQUENCE(DATETIME), t SET(TIME))"
    )
    _INSERT = "INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    _PARAMS = (
        1,
        Set([3, 1, 1, 2]),
        Multiset(["b", "a", "a", "it's"]),
        Sequence([3, 1, 2, 1]),
        Set([datetime.date(2024, 1, 2), datetime.date(99, 12, 31)]),
        Sequence([Decimal("1.25"), Decimal("-2")]),
        Set([b"\x0a\xff"]),
        Sequence([1.5, 1e-07]),
        Sequence([datetime.datetime(2024, 1, 2, 3, 4, 5, 123000)]),
        Set([datetime.time(1, 2, 3)]),
    )

    @staticmethod
    def _check(row: Any) -> None:
        assert row[0] == 1
        assert row[1] == frozenset({1, 2, 3})
        # MULTISET keeps duplicates, not order; SEQUENCE keeps both.
        assert sorted(row[2]) == ["a", "a", "b", "it's"]
        assert row[3] == [3, 1, 2, 1]
        assert row[4] == frozenset({datetime.date(2024, 1, 2), datetime.date(99, 12, 31)})
        assert row[5] == [Decimal("1.25"), Decimal("-2.00")]
        assert row[6] == frozenset({b"\x0a\xff"})
        assert row[7] == [1.5, 1e-07]
        assert row[8] == [datetime.datetime(2024, 1, 2, 3, 4, 5, 123000)]
        assert row[9] == frozenset({datetime.time(1, 2, 3)})

    def test_round_trip(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute(self._DDL % table)
            cursor.execute(self._INSERT % table, self._PARAMS)
            assert cursor.rowcount == 1
            cursor.execute("SELECT * FROM %s" % table)
            self._check(cursor.fetchone())
            assert cursor.description is not None
            assert [c[1] for c in cursor.description[1:4]] == [16, 17, 18]
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_predicates_and_empty(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (id INT, s SET(INT), q SEQUENCE(INT))" % table)
            cursor.executemany(
                "INSERT INTO %s VALUES (?, ?, ?)" % table,
                [
                    (1, Set([1, 3]), Sequence([3, 1])),
                    (2, Set(), Sequence()),
                    (3, Set([None]), Sequence([None, 2])),
                ],
            )
            cursor.execute("SELECT id FROM %s WHERE s = ? ORDER BY id" % table, (Set([3, 1]),))
            assert cursor.fetchall() == [(1,)]
            cursor.execute("SELECT id FROM %s WHERE q = ? ORDER BY id" % table, (Sequence([3, 1]),))
            assert cursor.fetchall() == [(1,)]
            # Order matters for SEQUENCE equality.
            cursor.execute("SELECT id FROM %s WHERE q = ?" % table, (Sequence([1, 3]),))
            assert cursor.fetchall() == []
            cursor.execute(
                "SELECT id FROM %s WHERE s SUBSETEQ ? ORDER BY id" % table,
                (Set([1, 3, 5]),),
            )
            assert cursor.fetchall() == [(1,), (2,)]
            cursor.execute("SELECT s, q FROM %s WHERE id = 2" % table)
            assert cursor.fetchone() == (frozenset(), [])
            cursor.execute("SELECT s, q FROM %s WHERE id = 3" % table)
            assert cursor.fetchone() == (frozenset({None}), [None, 2])
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_hostile_elements_bind_their_values(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (m MULTISET(VARCHAR(40)), q SEQUENCE(INT))" % table)
            cursor.execute(
                "INSERT INTO %s VALUES (?, ?)" % table,
                (Multiset([_EvilStr("it's"), _EvilStr("}")]), Sequence([_EvilInt(7)])),
            )
            cursor.execute("SELECT m, q FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            assert sorted(row[0]) == ["it's", "}"]
            assert row[1] == [7]
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_plain_containers_stay_rejected(self, cursor: Cursor) -> None:
        with pytest.raises(pycubrid.ProgrammingError, match="cannot bind a collection"):
            cursor.execute("SELECT ?", ([1, 2],))
        with pytest.raises(pycubrid.ProgrammingError, match="nested collection"):
            cursor.execute("SELECT ?", (Set([Set([1])]),))

    @pytest.mark.asyncio
    async def test_async_round_trip(self) -> None:
        conn = await pycubrid.aio.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
            decode_collections=True,
        )
        table = _tbl()
        async with conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(self._DDL % table)
                    await cur.execute(self._INSERT % table, self._PARAMS)
                    await cur.execute("SELECT * FROM %s" % table)
                    self._check(await cur.fetchone())
                    await cur.execute(
                        "SELECT id FROM %s WHERE q = ?" % table, (Sequence([3, 1, 2, 1]),)
                    )
                    assert await cur.fetchall() == [(1,)]
                finally:
                    await cur.execute("DROP TABLE IF EXISTS %s" % table)
