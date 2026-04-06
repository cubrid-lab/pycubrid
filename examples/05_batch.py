from __future__ import annotations

"""Batch insert example with executemany."""

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
            CREATE TABLE IF NOT EXISTS example_batch_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                age INT NOT NULL
            )
            """
        )
        conn.commit()

        batch_rows = [
            ("Alice", 30),
            ("Bob", 25),
            ("Carol", 28),
            ("Dave", 35),
            ("Eve", 31),
        ]

        _ = cur.executemany(
            "INSERT INTO example_batch_users (name, age) VALUES (?, ?)",
            batch_rows,
        )
        conn.commit()
        print(f"Batch inserted rows: {cur.rowcount}")

        _ = cur.execute("SELECT name, age FROM example_batch_users ORDER BY id")
        for row in cur.fetchall():
            print(row)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
