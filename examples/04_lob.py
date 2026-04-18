"""LOB write/read examples using direct values and Lob handles."""

from __future__ import annotations

import os
from typing import cast

import pycubrid
from pycubrid.constants import CUBRIDDataType
from pycubrid.cursor import Cursor
from pycubrid.lob import Lob


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
            CREATE TABLE IF NOT EXISTS example_documents (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(100) NOT NULL,
                content CLOB,
                raw_payload BLOB
            )
            """
        )
        conn.commit()

        _ = cur.execute(
            "INSERT INTO example_documents (title, content, raw_payload) VALUES (?, ?, ?)",
            ["README", "This is CLOB text content.", b"\x00\x01\x02"],
        )
        conn.commit()

        _ = cur.execute("SELECT id, title, content, raw_payload FROM example_documents ORDER BY id")
        first_row = cur.fetchone()
        print("Fetched row:", first_row)

        lob = cast(Lob, conn.create_lob(CUBRIDDataType.CLOB))
        _ = lob.write(b"LOB object write example")
        print("LOB read:", lob.read(length=1024, offset=0))
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
