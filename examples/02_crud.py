from __future__ import annotations

"""Create, read, update, and delete rows with pycubrid."""

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
            CREATE TABLE IF NOT EXISTS example_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                age INT NOT NULL
            )
            """
        )

        _ = cur.execute("INSERT INTO example_users (name, age) VALUES (?, ?)", ["Alice", 30])
        conn.commit()
        print(f"Inserted ID: {cur.lastrowid}")

        _ = cur.execute("SELECT id, name, age FROM example_users ORDER BY id")
        print("Rows after insert:", cur.fetchall())

        _ = cur.execute("UPDATE example_users SET age = ? WHERE name = ?", [31, "Alice"])
        conn.commit()
        print(f"Updated rows: {cur.rowcount}")

        _ = cur.execute("DELETE FROM example_users WHERE name = ?", ["Alice"])
        conn.commit()
        print(f"Deleted rows: {cur.rowcount}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
