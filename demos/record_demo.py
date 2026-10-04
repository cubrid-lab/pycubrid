#!/usr/bin/env python3
"""Run the real sync/async scenario recorded by the editable demo tapes.

Use ``short`` or ``extended``. DEMO_HOST, DEMO_PORT and
DEMO_EXPECTED_SERVER_VERSION are required; extended also requires DEMO_TABLE.
DEMO_DATABASE/DEMO_USER default to testdb/dba, DEMO_PASSWORD to empty.
The recording owner must provision the isolated endpoint and pinned wheel.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re

import pycubrid
from pycubrid import aio
from pycubrid.cursor import Cursor


def _verify(actual: object, expected: object, operation: str) -> None:
    if actual != expected:
        raise RuntimeError(operation + " verification failed")


def _options() -> dict[str, str | int]:
    host = os.environ.get("DEMO_HOST", "")
    port = int(os.environ.get("DEMO_PORT", "0"))
    if not host or not 1 <= port <= 65535:
        raise ValueError("DEMO_HOST and a valid DEMO_PORT are required")
    return {
        "host": host,
        "port": port,
        "database": os.environ.get("DEMO_DATABASE", "testdb"),
        "user": os.environ.get("DEMO_USER", "dba"),
        "password": os.environ.get("DEMO_PASSWORD", ""),
        "connect_timeout": 5,
        "read_timeout": 5,
    }


def _change(cursor: Cursor, sql: str, parameters: tuple[object, ...]) -> None:
    cursor.execute(sql, parameters)
    _verify(cursor.rowcount, 1, sql.split()[0] + " affected rows")
    print(sql.split()[0] + ": " + str(cursor.rowcount) + " row")


def _sync_select(options: dict[str, str | int], version: str) -> None:
    with pycubrid.connect(**options) as conn:
        observed_version = conn.get_server_version()
        _verify(observed_version, version, "server version")
        print("CUBRID " + observed_version)
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 + 1")
            row = cursor.fetchone()
            _verify(row, (2,), "sync SELECT")
            _verify(cursor.fetchone(), None, "sync EOF")
            print("Sync SELECT 1 + 1: " + repr(row))


async def _async_select(options: dict[str, str | int], version: str) -> None:
    async with await aio.connect(**options) as conn:
        _verify(await conn.get_server_version(), version, "async server version")
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1 + 1")
            row = await cursor.fetchone()
            _verify(row, (2,), "async SELECT")
            _verify(await cursor.fetchone(), None, "async EOF")
            print("Async SELECT 1 + 1: " + repr(row))


def _extended(options: dict[str, str | int], version: str, table: str) -> None:
    # Only this validated identifier enters SQL; every SQL value is bound.
    observer = pycubrid.connect(**options, autocommit=True)
    created = False
    try:
        observed_version = observer.get_server_version()
        _verify(observed_version, version, "server version")
        print("CUBRID " + observed_version)
        with observer.cursor() as setup:
            setup.execute("SELECT COUNT(*) FROM db_class WHERE class_name = ?", (table,))
            if setup.fetchone() != (0,):
                raise RuntimeError("refusing a preexisting demo table")
            setup.execute("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, txt VARCHAR(40))")
            created = True
        with pycubrid.connect(**options) as transaction:
            _verify(transaction.get_server_version(), version, "transaction server version")
            with transaction.cursor() as cursor:
                _change(cursor, "INSERT INTO " + table + " VALUES (?, ?)", (1, "hello"))
                cursor.execute("SELECT txt FROM " + table + " WHERE id = ?", (1,))
                row = cursor.fetchone()
                _verify(row, ("hello",), "CRUD SELECT")
                print("SELECT: " + repr(row))
                _change(cursor, "UPDATE " + table + " SET txt = ? WHERE id = ?", ("updated", 1))
                cursor.execute("SELECT txt FROM " + table + " WHERE id = ?", (1,))
                _verify(cursor.fetchone(), ("updated",), "UPDATE value")
                _change(cursor, "DELETE FROM " + table + " WHERE id = ?", (1,))
                cursor.execute("SELECT txt FROM " + table + " WHERE id = ?", (1,))
                _verify(cursor.fetchone(), None, "DELETE absence")
                _change(cursor, "INSERT INTO " + table + " VALUES (?, ?)", (2, "committed"))
        # The transaction context has committed and closed before this separate
        # connection observes the durable row; no explicit commit substitutes.
        with observer.cursor() as check:
            check.execute("SELECT txt FROM " + table + " WHERE id = ?", (2,))
            committed = check.fetchone()
            _verify(committed, ("committed",), "context commit observation")
            print("Context commit observed: " + repr(committed))
        asyncio.run(_async_select(options, version))
    finally:
        try:
            if created:
                with observer.cursor() as cleanup:
                    cleanup.execute("DROP TABLE " + table)
                    cleanup.execute("SELECT COUNT(*) FROM db_class WHERE class_name = ?", (table,))
                    _verify(cleanup.fetchone(), (0,), "table cleanup")
        finally:
            observer.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("short", "extended"))
    args = parser.parse_args(argv)
    options = _options()
    version = os.environ.get("DEMO_EXPECTED_SERVER_VERSION", "")
    if not version:
        raise ValueError("DEMO_EXPECTED_SERVER_VERSION is required")
    if args.mode == "short":
        _sync_select(options, version)
        asyncio.run(_async_select(options, version))
    else:
        table = os.environ.get("DEMO_TABLE", "")
        if re.fullmatch(r"[a-z][a-z0-9_]{0,62}", table) is None:
            raise ValueError("DEMO_TABLE must be an exact lowercase ASCII SQL identifier")
        _extended(options, version, table)
    # All context exits, cleanup verification and explicit closes have completed.
    print("DEMO_" + args.mode.upper() + "_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
