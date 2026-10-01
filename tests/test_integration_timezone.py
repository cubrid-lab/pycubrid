"""Timezone decoding against a live broker (#413)."""

from __future__ import annotations

import contextlib
import datetime
import sys
import zoneinfo
from collections.abc import Iterator, Sequence
from typing import cast

import pytest

from pycubrid.exceptions import DataError
from tests._parity_helpers import ADAPTERS, ParityAdapter, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]

AMBIGUOUS = "2026-11-01 01:30:00.250 America/New_York"


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


@contextlib.contextmanager
def _hide_tz_database() -> Iterator[None]:
    """Hide both the system zoneinfo and the ``tzdata`` package."""
    saved_path = zoneinfo.TZPATH
    with pytest.MonkeyPatch.context() as patch:
        for name in tuple(sys.modules):
            if name == "tzdata" or name.startswith("tzdata."):
                patch.delitem(sys.modules, name)
        patch.setitem(sys.modules, "tzdata", None)
        zoneinfo.reset_tzpath(to=[])
        zoneinfo.ZoneInfo.clear_cache()
        try:
            yield
        finally:
            zoneinfo.reset_tzpath(to=saved_path)
            zoneinfo.ZoneInfo.clear_cache()


@pytest.fixture
def no_tz_database() -> Iterator[None]:
    with _hide_tz_database():
        yield


@pytest.mark.asyncio
async def test_ambiguous_hour_follows_server_abbreviation(adapter: ParityAdapter) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        await adapter.execute(
            cursor,
            f"SELECT DATETIMETZ'{AMBIGUOUS} EDT', DATETIMETZ'{AMBIGUOUS} EST',"
            " TIMESTAMPTZ'2026-11-01 01:30:00 America/New_York EST'",
        )
        row = await adapter.fetchone(cursor)
        assert row is not None
        edt, est, ts_est = cast(tuple[datetime.datetime, ...], row)
        assert edt.utcoffset() == datetime.timedelta(hours=-4)
        assert est.utcoffset() == datetime.timedelta(hours=-5)
        assert ts_est.utcoffset() == datetime.timedelta(hours=-5)
        utc = datetime.timezone.utc
        assert est.astimezone(utc) - edt.astimezone(utc) == datetime.timedelta(hours=1)
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_tz_database")
@pytest.mark.parametrize(
    ("literal", "token"),
    [
        ("TIMESTAMPTZ'2026-01-15 10:30:00 Asia/Seoul'", "Asia/Seoul"),
        ("DATETIMETZ'2026-01-15 10:30:00.500 Asia/Seoul'", "Asia/Seoul"),
        ("TIMESTAMPLTZ'2026-01-15 10:30:00'", "UTC"),
        ("DATETIMELTZ'2026-01-15 10:30:00.500'", "UTC"),
    ],
)
async def test_missing_tz_database_raises_data_error_and_keeps_session(
    adapter: ParityAdapter, literal: str, token: str
) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        # LTZ values carry the session zone; pin it so the token is known.
        await adapter.execute(cursor, "SET TIME ZONE 'UTC'")
        token_before = adapter.transport_token(connection)
        with pytest.raises(DataError, match=f"'{token}'.*install the 'tzdata' package"):
            await adapter.execute(cursor, f"SELECT {literal}")
        assert adapter.transport_token(connection) is token_before
        # Offsets need no database; the same session keeps working.
        await adapter.execute(cursor, "SELECT TIMESTAMPTZ'2026-01-15 10:30:00 +05:30'")
        row = await adapter.fetchone(cursor)
        assert row is not None
        value = cast(datetime.datetime, row[0])
        assert value.utcoffset() == datetime.timedelta(hours=5, minutes=30)
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)


@pytest.mark.asyncio
async def test_shared_abbreviation_keeps_first_occurrence(adapter: ParityAdapter) -> None:
    # Moscow went from UTC+4 to UTC+3 on 2014-10-26; both are "MSK".
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        await adapter.execute(cursor, "SELECT DATETIMETZ'2014-10-26 01:30:00 Europe/Moscow MSK'")
        row = await adapter.fetchone(cursor)
        assert row is not None
        value = cast(datetime.datetime, row[0])
        assert (value.fold, value.utcoffset()) == (0, datetime.timedelta(hours=4))
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_tz_database")
async def test_unresolved_zone_on_execute_then_same_connection(adapter: ParityAdapter) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        with pytest.raises(DataError, match="'Asia/Seoul'"):
            await adapter.execute(cursor, "SELECT TIMESTAMPTZ'2026-01-15 10:30:00 Asia/Seoul'")
        assert cursor.description is None
        await adapter.close_cursor(cursor)
        cursor = adapter.cursor(connection)
        await adapter.execute(cursor, "SELECT 1")
        assert await adapter.fetchone(cursor) == (1,)
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)


@pytest.mark.asyncio
async def test_unresolved_zone_on_later_fetch_page(adapter: ParityAdapter) -> None:
    connection = await adapter.connect(fetch_size=17)
    cursor = adapter.cursor(connection)
    table = table_name("tz413")
    created = False
    try:
        await adapter.execute(
            cursor,
            "CREATE TABLE %s (id INTEGER PRIMARY KEY, pad VARCHAR(1000), v TIMESTAMPTZ)" % table,
        )
        created = True
        # Wide rows push the last one past the first reply page.
        rows: list[Sequence[object]] = [
            (i, "x" * 1000, "2026-01-15 10:30:00 +05:30") for i in range(199)
        ]
        rows.append((199, "x" * 1000, "2026-01-15 10:30:00 Asia/Seoul"))
        await adapter.executemany(cursor, "INSERT INTO %s VALUES (?, ?, ?)" % table, rows)
        with _hide_tz_database():
            await adapter.execute(cursor, "SELECT id, pad, v FROM %s ORDER BY id" % table)
            first_page = cursor._fetched_count
            assert 0 < first_page < 200
            seen: list[int] = []
            with pytest.raises(DataError, match="'Asia/Seoul'"):
                while (row := await adapter.fetchone(cursor)) is not None:
                    seen.append(cast(int, row[0]))
            # The failure came from a FETCH after the first page was consumed.
            assert seen == list(range(len(seen)))
            assert first_page <= len(seen) < 199
            await adapter.close_cursor(cursor)
            cursor = adapter.cursor(connection)
            await adapter.execute(cursor, "SELECT COUNT(*) FROM %s" % table)
            assert await adapter.fetchone(cursor) == (200,)
    finally:
        if created:
            await adapter.execute(cursor, "DROP TABLE %s" % table)
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)
