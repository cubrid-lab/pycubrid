"""Transaction patterns: explicit commit and rollback handling."""

from __future__ import annotations

import os
from typing import cast
import uuid

import pycubrid
from pycubrid.cursor import Cursor


def main(table_name: str | None = None) -> None:
    conn = pycubrid.connect(
        host=os.getenv("CUBRID_HOST", "localhost"),
        port=int(os.getenv("CUBRID_PORT", "33000")),
        database=os.getenv("CUBRID_DATABASE", "testdb"),
        user=os.getenv("CUBRID_USER", "dba"),
        password=os.getenv("CUBRID_PASSWORD", ""),
    )
    cur = cast(Cursor, conn.cursor())

    table = table_name or ("example_ledger_" + uuid.uuid4().hex[:8])
    created = False

    try:
        _ = cur.execute(
            "CREATE TABLE "
            + table
            + " (id INT AUTO_INCREMENT PRIMARY KEY, message VARCHAR(200) NOT NULL)"
        )
        created = True
        conn.commit()

        try:
            _ = cur.execute("INSERT INTO " + table + " (message) VALUES (?)", ["start transaction"])
            _ = cur.execute("INSERT INTO " + table + " (message) VALUES (?)", ["commit path"])
            conn.commit()
            print("Commit path succeeded")
        except pycubrid.Error:
            conn.rollback()
            raise

        try:
            _ = cur.execute("INSERT INTO " + table + " (message) VALUES (?)", ["rollback path"])
            # Deliberately violate NOT NULL constraint on message to force an error deterministically.
            _ = cur.execute("INSERT INTO " + table + " (message) VALUES (?)", [None])
            conn.commit()
        except pycubrid.Error as exc:
            conn.rollback()
            print(f"Rollback path executed: {exc}")

        _ = cur.execute("SELECT id, message FROM " + table + " ORDER BY id")
        print("Ledger rows:", cur.fetchall())
    finally:
        try:
            if created:
                _ = cur.execute("DROP TABLE IF EXISTS " + table)
                conn.commit()
        finally:
            cur.close()
            conn.close()


if __name__ == "__main__":
    main()
