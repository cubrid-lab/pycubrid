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
            assert lob.write(b"abcd") == 4
            handle = lob.lob_handle
            assert lob.write(b"") == 0
            assert lob.write(b"", offset=2) == 0
            assert lob.lob_handle == handle
            assert lob.read(4) == b"abcd"
            assert lob.write(b"XY", offset=1) == 2
            assert lob.read(4) == b"aXYd"
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
        finally:
            cur.close()
    finally:
        conn.close()  # Session-scoped LOB storage is reclaimed; no user/shared tables touched.
