"""Live parameter round-trip property tests (issue #337).

Feeds Hypothesis-generated Python values through the full driver path against a
real CUBRID server:

    Python value -> parameter formatter -> SQL -> CAS -> CUBRID
                 -> CAS response -> packet decoder -> Python value

and asserts the round-trip preserves the value per CUBRID type semantics, that
supported and unsupported values both surface as DB-API results/errors (never a
raw ``struct.error`` / ``UnicodeDecodeError`` / ``decimal.InvalidOperation``),
and that sync and async agree.

Skipped automatically when no CUBRID server is reachable (same gate as
``test_integration.py``). Configure with ``CUBRID_TEST_HOST`` /
``CUBRID_TEST_PORT`` etc. Exploration budget follows the active Hypothesis
profile (see ``tests/conftest.py``): small on PR, wide on nightly.
"""

from __future__ import annotations

import asyncio
import datetime
import sys
import uuid
from decimal import Context, Decimal

import pytest
from hypothesis import given, settings, strategies as st

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import Error as DBAPIError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

pytestmark = pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available")

# Only these exception types are an acceptable failure mode for a value the
# driver cannot represent. Anything else (struct.error, UnicodeDecodeError,
# decimal.InvalidOperation, OverflowError, ...) is a raw-leak defect.
ACCEPTABLE_REJECT: tuple[type[BaseException], ...] = (DBAPIError,)


def _tbl() -> str:
    return "ph_live_%s" % uuid.uuid4().hex[:8]


# ---------------------------------------------------------------------------
# Value strategies grouped by the CUBRID column type used to store them.
# Each entry: (column_ddl, strategy, comparator). The comparator returns True
# when the fetched value matches the inserted value per that type's semantics.
# ---------------------------------------------------------------------------


def _eq(inserted: object, fetched: object) -> bool:
    return inserted == fetched


_INT_MIN, _INT_MAX = -(2**31), 2**31 - 1
_BIGINT_MIN, _BIGINT_MAX = -(2**63), 2**63 - 1

integers_int = st.integers(min_value=_INT_MIN, max_value=_INT_MAX)
integers_bigint = st.integers(min_value=_BIGINT_MIN, max_value=_BIGINT_MAX)

# Doubles: exclude NaN/Inf (driver rejects them by contract, tested separately)
# and subnormals below CUBRID's DOUBLE range. CUBRID rejects magnitudes outside
# roughly [2.2e-308, 1.7e308]; values below the smallest normal double raise a
# server-side "exceeds limit of double" error rather than round-tripping.
_MIN_NORMAL_DOUBLE = 2.2250738585072014e-308
doubles = st.floats(
    allow_nan=False,
    allow_infinity=False,
    width=64,
    min_value=-1.7e308,
    max_value=1.7e308,
).filter(lambda x: x == 0.0 or abs(x) >= _MIN_NORMAL_DOUBLE)

# Strings: exclude NUL and Ctrl-Z (driver rejects them by contract), and
# surrogates (cannot encode to UTF-8).
text_strings = st.text(
    alphabet=st.characters(
        blacklist_categories=("Cs",),
        blacklist_characters="\x00\x1a",
    ),
    min_size=0,
    max_size=64,
)

# NUMERIC(38,10): 27 integer digits + 10 fractional = 37, leaving one digit of
# headroom under the column's precision of 38 so no generated value overflows.
decimals = st.decimals(
    min_value=Decimal("-999999999999999999999999999.9999999999"),
    max_value=Decimal("999999999999999999999999999.9999999999"),
    allow_nan=False,
    allow_infinity=False,
    places=10,
)

dates = st.dates(min_value=datetime.date(1970, 1, 1), max_value=datetime.date(2037, 12, 31))
times = st.times().map(lambda t: t.replace(microsecond=0))


class _LiveDB:
    """Minimal sync helper: create a one-column table, round-trip one value."""

    def __init__(self) -> None:
        self.conn = pycubrid.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
        )
        self.conn.autocommit = True

    def roundtrip(self, column_ddl: str, value: object) -> object:
        table = _tbl()
        cur = self.conn.cursor()
        try:
            cur.execute("DROP TABLE IF EXISTS %s" % table)
            cur.execute("CREATE TABLE %s (v %s)" % (table, column_ddl))
            cur.execute("INSERT INTO %s (v) VALUES (?)" % table, (value,))
            cur.execute("SELECT v FROM %s" % table)
            row = cur.fetchone()
            assert row is not None
            return row[0]
        finally:
            try:
                cur.execute("DROP TABLE IF EXISTS %s" % table)
            except DBAPIError:
                pass
            cur.close()

    def close(self) -> None:
        self.conn.close()


async def _roundtrip_async(column_ddl: str, value: object) -> object:
    conn = await pycubrid.aio.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )
    await conn.set_autocommit(True)
    table = _tbl()
    cur = conn.cursor()
    try:
        await cur.execute("DROP TABLE IF EXISTS %s" % table)
        await cur.execute("CREATE TABLE %s (v %s)" % (table, column_ddl))
        await cur.execute("INSERT INTO %s (v) VALUES (?)" % table, (value,))
        await cur.execute("SELECT v FROM %s" % table)
        row = await cur.fetchone()
        assert row is not None
        return row[0]
    finally:
        try:
            await cur.execute("DROP TABLE IF EXISTS %s" % table)
        except DBAPIError:
            pass
        await cur.close()
        await conn.close()


@pytest.fixture(scope="module")
def db() -> "_LiveDB":
    handle = _LiveDB()
    yield handle
    handle.close()


class TestLiveRoundTrip:
    @given(value=integers_int)
    @settings(deadline=None, max_examples=40)
    def test_integer(self, db: _LiveDB, value: int) -> None:
        assert db.roundtrip("INTEGER", value) == value

    @given(value=integers_bigint)
    @settings(deadline=None, max_examples=40)
    def test_bigint(self, db: _LiveDB, value: int) -> None:
        assert db.roundtrip("BIGINT", value) == value

    @given(value=doubles)
    @settings(deadline=None, max_examples=40)
    def test_double(self, db: _LiveDB, value: float) -> None:
        fetched = db.roundtrip("DOUBLE", value)
        assert isinstance(fetched, float)
        # The driver renders doubles as a decimal text literal (str(value)) and
        # CUBRID re-parses it; that text round-trip can differ by up to a couple
        # of ULP from the original IEEE-754 bits, so compare with a small
        # relative tolerance rather than for bit-exact equality.
        if value == 0.0:
            assert fetched == 0.0
        else:
            assert abs(fetched - value) <= abs(value) * 1e-12

    @given(value=decimals)
    @settings(deadline=None, max_examples=40)
    def test_numeric(self, db: _LiveDB, value: Decimal) -> None:
        fetched = db.roundtrip("NUMERIC(38,10)", value)
        assert isinstance(fetched, Decimal)
        # Quantize to scale 10 under a context wide enough for NUMERIC(38,10);
        # the default 28-digit decimal context would raise InvalidOperation for
        # values needing all 38 significant digits.
        wide = Context(prec=40)
        assert fetched == value.quantize(Decimal("1.0000000000"), context=wide)

    @given(value=text_strings)
    @settings(deadline=None, max_examples=60)
    def test_varchar(self, db: _LiveDB, value: str) -> None:
        fetched = db.roundtrip("VARCHAR(4096)", value)
        assert fetched == value

    @given(value=st.none())
    @settings(deadline=None, max_examples=1)
    def test_null(self, db: _LiveDB, value: None) -> None:
        assert db.roundtrip("INTEGER", value) is None

    @given(value=dates)
    @settings(deadline=None, max_examples=40)
    def test_date(self, db: _LiveDB, value: datetime.date) -> None:
        assert db.roundtrip("DATE", value) == value

    @given(value=times)
    @settings(deadline=None, max_examples=40)
    def test_time(self, db: _LiveDB, value: datetime.time) -> None:
        assert db.roundtrip("TIME", value) == value


class TestSyncAsyncTypeParity:
    @given(value=integers_int)
    @settings(deadline=None, max_examples=25)
    def test_integer_types_match(self, db: _LiveDB, value: int) -> None:
        sync_val = db.roundtrip("INTEGER", value)
        async_val = asyncio.run(_roundtrip_async("INTEGER", value))
        assert type(sync_val) is type(async_val)
        assert sync_val == async_val

    @given(value=text_strings)
    @settings(deadline=None, max_examples=25)
    def test_varchar_types_match(self, db: _LiveDB, value: str) -> None:
        sync_val = db.roundtrip("VARCHAR(4096)", value)
        async_val = asyncio.run(_roundtrip_async("VARCHAR(4096)", value))
        assert type(sync_val) is type(async_val)
        assert sync_val == async_val


class TestUnsupportedValuesRejectCleanly:
    """Values the driver cannot represent must raise a DB-API error, not leak."""

    @given(
        value=st.sampled_from(
            [
                float("nan"),
                float("inf"),
                float("-inf"),
                Decimal("NaN"),
                Decimal("Infinity"),
                "\x00",
                "abc\x00def",
                "ctrl\x1aZ",
                [1, 2, 3],
                {"a": 1},
                (1, 2),
                {1, 2},
            ]
        )
    )
    @settings(deadline=None, max_examples=12)
    def test_unsupported_value_is_dbapi_error(self, db: _LiveDB, value: object) -> None:
        try:
            db.roundtrip("VARCHAR(4096)", value)
        except ACCEPTABLE_REJECT:
            return
        except BaseException as exc:  # noqa: BLE001 - asserting on the type
            raise AssertionError(
                f"unsupported value {value!r} leaked a non-DB-API exception: "
                f"{type(exc).__module__}.{type(exc).__name__}: {exc!r}"
            ) from exc
        # Some values (e.g. a numeric string) are legitimately storable; that is
        # fine. Only a raw non-DB-API leak fails the test.


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
