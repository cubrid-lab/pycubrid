"""Session-only BLOB/CLOB empty-write regression, no shared tables (#394)."""

from __future__ import annotations

import pytest

import pycubrid
from pycubrid.constants import CUBRIDDataType
from tests._parity_helpers import connect_kwargs

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "lob_type", [CUBRIDDataType.BLOB, CUBRIDDataType.CLOB], ids=["blob", "clob"]
)
def test_empty_lob_write_preserves_bytes_and_connection(lob_type: int) -> None:
    conn = pycubrid.connect(**connect_kwargs())
    try:
        with conn.create_lob(lob_type) as lob:
            written = lob.write(b"abcd")
            assert written == 4
            handle = lob.lob_handle
            empty_written = lob.write(b"")
            assert empty_written == 0
            offset_written = lob.write(b"", offset=2)
            assert offset_written == 0
            assert lob.lob_handle == handle
            content = lob.read(4)
            assert content == b"abcd"
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
            row = cur.fetchone()
            assert row == (1,)
        finally:
            cur.close()
    finally:
        conn.close()  # Session-scoped LOB storage is reclaimed; no user/shared tables touched.
