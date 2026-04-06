from __future__ import annotations

"""Transaction patterns: explicit commit and rollback handling."""

import os
from typing import cast

import pycubrid
from pycubrid.cursor import Cursor


def main() -> None:
    conn = pycubrid.connect(
        host=os.getenv("CUBRID_HOST", "localhost"),
        port=int(os.getenv("CUBRID_PORT", "33000")),
        database=os.getenv("CUBRID_DATABASE", "testdb"),
        user=os.getenv("CUBRID_USER", "dba"),
        password=os.getenv("CUBRID_PASSWORD", ""),
    )
    cur = cast(Cursor, conn.cursor())

    try:
        _ = cur.execute(
            """
            CREATE TABLE IF NOT EXISTS example_ledger (
                id INT AUTO_INCREMENT PRIMARY KEY,
                message VARCHAR(200) NOT NULL
            )
            """
        )
        conn.commit()

        try:
            _ = cur.execute("INSERT INTO example_ledger (message) VALUES (?)", ["start transaction"])
            _ = cur.execute("INSERT INTO example_ledger (message) VALUES (?)", ["commit path"])
            conn.commit()
            print("Commit path succeeded")
        except pycubrid.Error:
            conn.rollback()
            raise

        try:
            _ = cur.execute("INSERT INTO example_ledger (message) VALUES (?)", ["rollback path"])
            _ = cur.execute("INSERT INTO missing_table (message) VALUES (?)", ["force error"])
            conn.commit()
        except pycubrid.Error as exc:
            conn.rollback()
            print(f"Rollback path executed: {exc}")

        _ = cur.execute("SELECT id, message FROM example_ledger ORDER BY id")
        print("Ledger rows:", cur.fetchall())
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
