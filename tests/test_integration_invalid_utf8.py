"""Invalid UTF-8 from a live broker keeps the session usable (#492)."""

from __future__ import annotations

from typing import cast

import pytest

from pycubrid.exceptions import DataError, ProgrammingError
from tests._parity_helpers import ADAPTERS, ParityAdapter

pytestmark = pytest.mark.integration

WIDE = "\U00010000" * 60


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


@pytest.mark.asyncio
async def test_error_message_cut_mid_character(adapter: ParityAdapter) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    token = adapter.transport_token(connection)
    try:
        replaced = False
        # CUBRID cuts the echoed identifier by bytes; which padding lands the
        # cut inside a 4-byte character differs by server version.
        for pad in range(4):
            with pytest.raises(ProgrammingError) as raised:
                await adapter.execute(cursor, "SELECT %s%sx FROM db_root" % ("a" * pad, WIDE))
            assert raised.value.errno == -494
            assert raised.value.sqlstate == "42000"
            replaced = replaced or "�" in raised.value.msg
        assert replaced
        assert adapter.transport_token(connection) is token
        await adapter.execute(cursor, "SELECT 1")
        assert await adapter.fetchone(cursor) == (1,)
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)


@pytest.mark.asyncio
async def test_invalid_value_raises_data_error(adapter: ParityAdapter) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    token = adapter.transport_token(connection)
    try:
        # SUBSTRB cuts the 4-byte character after its first two bytes.
        with pytest.raises(DataError, match="not valid UTF-8"):
            await adapter.execute(cursor, "SELECT SUBSTRB('a' || ?, 1, 3)", (WIDE,))
        assert adapter.transport_token(connection) is token
        await adapter.execute(cursor, "SELECT SUBSTRB('a' || ?, 1, 5)", (WIDE,))
        assert await adapter.fetchone(cursor) == ("a\U00010000",)
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)
