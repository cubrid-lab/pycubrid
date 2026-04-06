from __future__ import annotations

"""Basic connection and query example for pycubrid."""

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
        _ = cur.execute("SELECT 1 + 1")
        print(f"Query result: {cur.fetchone()}")
        print(f"Server version: {conn.get_server_version()}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
