"""A ? inside a quoted identifier raises an explained ProgrammingError (#990).

The CAS counts every ? outside comments and single-quoted strings as a marker,
also inside quoted identifiers, and rejects the statement with CAS_ER_NUM_BIND
(-1007, no text) when fewer values were bound. Sync and async.
"""

from __future__ import annotations

from typing import cast

import pytest

from pycubrid.exceptions import ProgrammingError
from tests._parity_helpers import ADAPTERS, ParityAdapter

pytestmark = [pytest.mark.integration]


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "params"),
    [
        ('SELECT 1 AS "a?b"', None),
        ("SELECT 1 AS `a?b`", None),
        ('SELECT ? AS "q?"', (1,)),
    ],
    ids=["double-quoted", "backtick", "with-parameter"],
)
async def test_marker_in_quoted_identifier_is_explained(
    adapter: ParityAdapter, operation: str, params: tuple[int, ...] | None
) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        with pytest.raises(ProgrammingError, match="inside a quoted identifier") as raised:
            await adapter.execute(cursor, operation, params)
        assert (raised.value.errno, raised.value.error_source) == (-1007, "cas")
        # The connection stays usable.
        await adapter.execute(cursor, "SELECT 1")
        assert [tuple(row) for row in await adapter.fetchall(cursor)] == [(1,)]
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)
