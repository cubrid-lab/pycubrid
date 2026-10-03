"""Real native constraint failures remain usable after rollback (#390)."""

from __future__ import annotations

from typing import cast

import pytest

from pycubrid.exceptions import IntegrityError
from tests._parity_helpers import ADAPTERS, ParityAdapter, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["execute", "executemany", "batch"])
@pytest.mark.parametrize("code", [-631, -922], ids=["not-null", "foreign-key"])
async def test_native_integrity_failure_and_rollback_reuse(
    adapter: ParityAdapter, operation: str, code: int
) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    table = table_name("er390")
    created = False
    try:
        await adapter.set_autocommit(connection, False)
        await adapter.execute(
            cursor,
            "CREATE TABLE %s (id INTEGER PRIMARY KEY, parent_id INTEGER, n INTEGER NOT NULL, "
            "FOREIGN KEY(parent_id) REFERENCES %s(id))" % (table, table),
        )
        created = True
        await adapter.commit(connection)
        values = (1, None, None) if code == -631 else (1, 999, 1)
        sql = "INSERT INTO %s VALUES (?, ?, ?)" % table
        with pytest.raises(IntegrityError) as raised:
            if operation == "execute":
                await adapter.execute(cursor, sql, values)
            elif operation == "executemany":
                await adapter.executemany(cursor, sql, [values])
            else:
                literal = "1, NULL, NULL" if code == -631 else "1, 999, 1"
                await adapter.executemany_batch(
                    cursor, ["INSERT INTO %s VALUES (%s)" % (table, literal)]
                )
        assert raised.value.code == code
        assert raised.value.errno == code
        assert raised.value.sqlstate == "23000"
        await adapter.rollback(connection)
        await adapter.execute(cursor, "SELECT 1")
        assert await adapter.fetchone(cursor) == (1,)
        await adapter.execute(cursor, sql, (2, None, 7))
        await adapter.execute(cursor, "SELECT COUNT(*) FROM %s" % table)
        assert await adapter.fetchone(cursor) == (1,)
    finally:
        await adapter.rollback(connection)
        if created:
            await adapter.execute(cursor, "DROP TABLE %s" % table)
            await adapter.commit(connection)
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)


_PARENT_CHANGES = [
    ("delete", "DELETE FROM {parent} WHERE id = 1", {-924}),
    ("update", "UPDATE {parent} SET id = 5 WHERE id = 1", {-924}),
    # CUBRID 11.4 reports ER_TRUNCATE_PK_REFERRED; 10.2 reports -924.
    ("truncate", "TRUNCATE TABLE {parent}", {-924, -1284}),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "statement", "codes"),
    [
        pytest.param(operation, statement, codes, id="%s-%s" % (kind, operation))
        for kind, statement, codes in _PARENT_CHANGES
        for operation in ("execute", "executemany", "batch")
        # TRUNCATE takes no parameters to bind through executemany().
        if not (kind == "truncate" and operation == "executemany")
    ],
)
async def test_referenced_parent_change_is_integrity_error(
    adapter: ParityAdapter, operation: str, statement: str, codes: set[int]
) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    parent = table_name("er493p")
    child = table_name("er493c")
    created: list[str] = []
    try:
        await adapter.set_autocommit(connection, False)
        await adapter.execute(cursor, "CREATE TABLE %s (id INTEGER PRIMARY KEY)" % parent)
        created.append(parent)
        await adapter.execute(
            cursor,
            "CREATE TABLE %s (id INTEGER PRIMARY KEY, parent_id INTEGER, "
            "FOREIGN KEY(parent_id) REFERENCES %s(id))" % (child, parent),
        )
        created.insert(0, child)
        await adapter.execute(cursor, "INSERT INTO %s VALUES (1)" % parent)
        await adapter.execute(cursor, "INSERT INTO %s VALUES (1, 1)" % child)
        await adapter.commit(connection)
        sql = statement.format(parent=parent)
        with pytest.raises(IntegrityError) as raised:
            if operation == "execute":
                await adapter.execute(cursor, sql)
            elif operation == "executemany":
                await adapter.executemany(cursor, sql.replace("id = 1", "id = ?"), [(1,)])
            else:
                await adapter.executemany_batch(cursor, [sql])
        assert raised.value.code in codes
        assert raised.value.errno == raised.value.code
        assert raised.value.sqlstate == "23000"
        await adapter.rollback(connection)
        await adapter.execute(cursor, "SELECT COUNT(*) FROM %s" % parent)
        assert await adapter.fetchone(cursor) == (1,)
    finally:
        await adapter.rollback(connection)
        for table in created:
            await adapter.execute(cursor, "DROP TABLE %s" % table)
        await adapter.commit(connection)
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)
