"""Live smoke test of the published example scripts (#1312).

``examples/01_connect.py``, ``02_crud.py``, ``04_lob.py`` and ``05_batch.py``
each run as a separate ``python examples/<script>`` process, the way a
first-time user runs them, with ``CUBRID_HOST/PORT/DATABASE/USER/PASSWORD``
pointing at the integration endpoint. A second connection then inspects the
database, so a passing test proves the script's effect and not only its exit
status or printed text. ``03_transactions.py`` is excluded: its rollback
behavior is covered by ``tests/test_example_transactions.py`` (#1282).

The CRUD, LOB and batch scripts create fixed-name tables with
``CREATE TABLE IF NOT EXISTS`` and never drop them. A table of that name that
already exists may belong to someone else, so the test fails before running
the script instead of modifying it, and drops only a table that did not exist
before the script ran, also after a failed assertion. ``make integration``
starts every run with an empty, run-owned database.
"""

from __future__ import annotations

import os
import re
import subprocess  # nosec B404 - fixed argv: this interpreter and a repository script
import sys
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

import pycubrid
from pycubrid.connection import Connection

from ._cubrid_endpoint import CubridEndpoint, resolve_endpoint

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
SCRIPT_TIMEOUT = 120
# Literal statements for the only tables the examples create; no SQL is built from names.
DROP_OWNED_TABLE = {
    "example_users": "DROP TABLE example_users",
    "example_documents": "DROP TABLE example_documents",
    "example_batch_users": "DROP TABLE example_batch_users",
}


@pytest.fixture(scope="module")
def endpoint() -> CubridEndpoint:
    return resolve_endpoint()


@pytest.fixture
def inspector(endpoint: CubridEndpoint) -> Iterator[Connection]:
    """A connection separate from the example's own, used only to check its effect."""
    with closing(pycubrid.connect(**endpoint.connect_kwargs())) as conn:
        yield conn


def _query(conn: Connection, sql: str, params: list[object] | None = None) -> list[tuple[Any, ...]]:
    with closing(conn.cursor()) as cur:
        cur.execute(sql, params or [])
        rows = cur.fetchall()
    # End the read transaction so the next check sees the example's later commits.
    conn.commit()
    return [tuple(row) for row in rows]


def _table_exists(conn: Connection, name: str) -> bool:
    rows = _query(conn, "SELECT COUNT(*) FROM db_class WHERE LOWER(class_name) = ?", [name])
    return int(rows[0][0]) > 0


def _mask(text: str, endpoint: CubridEndpoint) -> str:
    return text.replace(endpoint.password, "***") if endpoint.password else text


def _run_example(script: str, endpoint: CubridEndpoint) -> str:
    """Run one example as its own process and return its standard output."""
    env = {key: value for key, value in os.environ.items() if not key.startswith("CUBRID_")}
    env.update(
        CUBRID_HOST=endpoint.host,
        CUBRID_PORT=str(endpoint.port),
        CUBRID_DATABASE=endpoint.database,
        CUBRID_USER=endpoint.user,
        CUBRID_PASSWORD=endpoint.password,
    )
    result = subprocess.run(  # nosec B603 - fixed argv, no shell
        [sys.executable, str(EXAMPLES / script)],
        env=env,
        capture_output=True,
        text=True,
        timeout=SCRIPT_TIMEOUT,
        check=False,
    )
    assert result.returncode == 0, _mask(
        f"examples/{script} exited {result.returncode} against {endpoint.describe()}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        endpoint,
    )
    return result.stdout


def _printed(pattern: str, stdout: str) -> str:
    match = re.search(pattern, stdout, re.MULTILINE)
    assert match, f"expected output matching {pattern!r}, got:\n{stdout}"
    return match.group(1)


@pytest.fixture
def owned_table(
    request: pytest.FixtureRequest, inspector: Connection, endpoint: CubridEndpoint
) -> Iterator[str]:
    """Refuse a pre-existing table; drop the table only when this run created it."""
    name: str = request.param
    if _table_exists(inspector, name):
        pytest.fail(
            f"table {name} already exists in {endpoint.describe()}; it may hold another "
            "user's rows, so the example was not run and the table was left untouched. "
            "Use a fresh database (`make integration` creates one).",
            pytrace=False,
        )
    try:
        yield name
    finally:
        if _table_exists(inspector, name):
            with closing(inspector.cursor()) as cur:
                cur.execute(DROP_OWNED_TABLE[name])
            inspector.commit()


def test_connect_example_runs_select(endpoint: CubridEndpoint, inspector: Connection) -> None:
    stdout = _run_example("01_connect.py", endpoint)

    assert _printed(r"^Query result: (.*)$", stdout) == "(2,)"
    assert _printed(r"^Server version: (.*)$", stdout) == inspector.get_server_version()
    assert _query(inspector, "SELECT 1 + 1") == [(2,)]


@pytest.mark.parametrize("owned_table", ["example_users"], indirect=True)
def test_crud_example_leaves_an_empty_table(
    owned_table: str, endpoint: CubridEndpoint, inspector: Connection
) -> None:
    stdout = _run_example("02_crud.py", endpoint)

    # The table is new, so its AUTO_INCREMENT id starts at 1; the script updates
    # and deletes that one row, leaving the created table empty.
    assert _printed(r"^Inserted ID: (\d+)$", stdout) == "1"
    assert "Rows after insert: [(1, 'Alice', 30)]" in stdout
    assert _printed(r"^Updated rows: (\d+)$", stdout) == "1"
    assert _printed(r"^Deleted rows: (\d+)$", stdout) == "1"
    assert _table_exists(inspector, owned_table)
    assert _query(inspector, "SELECT id, name, age FROM example_users") == []
    columns = _query(
        inspector,
        "SELECT attr_name FROM db_attribute WHERE LOWER(class_name) = ? ORDER BY def_order",
        [owned_table],
    )
    assert columns == [("id",), ("name",), ("age",)]


@pytest.mark.parametrize("owned_table", ["example_documents"], indirect=True)
def test_lob_example_stores_exact_clob_text_and_blob_bytes(
    owned_table: str, endpoint: CubridEndpoint, inspector: Connection
) -> None:
    stdout = _run_example("04_lob.py", endpoint)

    # CLOB_TO_CHAR/BLOB_TO_BIT read the stored LOB values on the server, so the
    # check does not depend on the driver's LOB handle decoding.
    rows = _query(
        inspector,
        "SELECT id, title, CLOB_TO_CHAR(content), BLOB_TO_BIT(raw_payload) "
        "FROM example_documents ORDER BY id",
    )
    assert rows == [(1, "README", "This is CLOB text content.", b"\x00\x01\x02")]
    assert "LOB read: b'LOB object write example'" in stdout


@pytest.mark.parametrize("owned_table", ["example_batch_users"], indirect=True)
def test_batch_example_inserts_five_rows(
    owned_table: str, endpoint: CubridEndpoint, inspector: Connection
) -> None:
    stdout = _run_example("05_batch.py", endpoint)

    expected = [("Alice", 30), ("Bob", 25), ("Carol", 28), ("Dave", 35), ("Eve", 31)]
    assert _printed(r"^Batch inserted rows: (\d+)$", stdout) == "5"
    assert _query(inspector, "SELECT name, age FROM example_batch_users ORDER BY id") == expected
