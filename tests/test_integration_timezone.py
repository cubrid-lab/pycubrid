"""Timezone decoding against a live broker (#413)."""

from __future__ import annotations

import datetime
import sys
import zoneinfo
from collections.abc import Iterator
from typing import cast

import pytest

from pycubrid.exceptions import DataError
from tests._parity_helpers import ADAPTERS, ParityAdapter

pytestmark = pytest.mark.integration

AMBIGUOUS = "2026-11-01 01:30:00.250 America/New_York"


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


@pytest.fixture
def no_tz_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Hide both the system zoneinfo and the ``tzdata`` package."""
    monkeypatch.setitem(sys.modules, "tzdata", None)
    zoneinfo.reset_tzpath(to=[])
    zoneinfo.ZoneInfo.clear_cache()
    try:
        yield
    finally:
        zoneinfo.reset_tzpath()
        zoneinfo.ZoneInfo.clear_cache()


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
    token_before = adapter.transport_token(connection)
    try:
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
