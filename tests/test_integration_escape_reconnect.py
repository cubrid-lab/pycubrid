"""Live read-only proof that escape mode is valid after physical recovery (#471)."""

from __future__ import annotations

import pytest

from ._parity_helpers import ADAPTERS, ParityAdapter

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.mark.parametrize("adapter", ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
async def test_backslash_value_after_physical_recovery(adapter: ParityAdapter) -> None:
    conn = await adapter.connect()
    value = r"C:\temp\file"

    async def select_value() -> object:
        cur = adapter.cursor(conn)
        try:
            await adapter.execute(cur, "SELECT ?", (value,))
            row = await adapter.fetchone(cur)
            assert row is not None
            return row[0]
        finally:
            await adapter.close_cursor(cur)

    try:
        mode = conn._no_backslash_escapes
        assert type(mode) is bool
        generation = conn._physical_generation
        original_transport = adapter.transport_token(conn)
        assert await select_value() == value

        await adapter.drop_transport(conn)
        assert await adapter.ping(conn, reconnect=True) is True

        assert adapter.transport_token(conn) is not original_transport
        assert conn._physical_generation == generation + 1
        assert conn._no_backslash_escapes is mode
        assert await select_value() == value
    finally:
        await adapter.close_connection(conn)
