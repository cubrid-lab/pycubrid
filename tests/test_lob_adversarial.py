"""Adversarial BLOB/CLOB boundary and partial-I/O tests (issue #347).

Exercises the LOB read/write contract against a live CUBRID server across size
boundaries and offsets, plus the lifecycle/error edges. LOB is sync-only
(``Lob`` rejects async connections), so this suite is sync-only by design.

Asserted contract:

* ``write(data)`` returns ``len(data)`` and a full read reproduces it;
* ``read(n)`` never returns more than ``n`` bytes, and never more than the LOB
  actually holds (over-read caps at the stored length);
* offset reads return the correct slice;
* ``read(-1)`` / negative offset raise :class:`InterfaceError`;
* operating on a closed LOB raises :class:`InterfaceError`.

Empirically grounded on CUBRID 11.2: a zero-length ``read(0)`` triggers a
server-side transaction abort, so it is exercised as an explicit error case
(must surface as a DB-API error, not a raw exception) rather than as a
success case.

Skipped when no CUBRID server is reachable.
"""

from __future__ import annotations

import sys

import pytest
from hypothesis import given, settings, strategies as st

import pycubrid
from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import InterfaceError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available"),
]

_CHUNK = 4096  # a representative chunk boundary for boundary sizing


def _connect() -> pycubrid.Connection:
    return pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )


@pytest.fixture(scope="module")
def conn() -> pycubrid.Connection:
    c = _connect()
    c.autocommit = True
    yield c
    c.close()


# Both LOB kinds store/return raw bytes; a CLOB additionally interprets them as
# UTF-8 text, so ASCII-range byte payloads round-trip identically through both.
_LOB_TYPES = [CUBRIDDataType.BLOB, CUBRIDDataType.CLOB]
_LOB_IDS = ["blob", "clob"]


class TestLobRoundTrip:
    @pytest.mark.parametrize("lob_type", _LOB_TYPES, ids=_LOB_IDS)
    @given(
        size=st.one_of(
            st.sampled_from([1, 2, _CHUNK - 1, _CHUNK, _CHUNK + 1, _CHUNK * 3]),
            st.integers(min_value=1, max_value=8192),
        )
    )
    @settings(deadline=None, max_examples=20)
    def test_write_read_full(self, conn: pycubrid.Connection, lob_type: int, size: int) -> None:
        # ASCII-range bytes so the payload is valid CLOB (UTF-8) text too.
        data = bytes((i * 7 + 3) & 0x7F for i in range(size))
        lob = conn.create_lob(lob_type)
        written = lob.write(data)
        assert written == len(data)
        assert lob.read(size) == data

    @pytest.mark.parametrize("lob_type", _LOB_TYPES, ids=_LOB_IDS)
    @given(size=st.integers(min_value=16, max_value=4096))
    @settings(deadline=None, max_examples=15)
    def test_read_never_exceeds_requested(
        self, conn: pycubrid.Connection, lob_type: int, size: int
    ) -> None:
        data = bytes((i * 7 + 3) & 0x7F for i in range(size))
        lob = conn.create_lob(lob_type)
        lob.write(data)
        # Requesting fewer bytes than stored never returns more than requested.
        assert len(lob.read(8)) <= 8
        # Over-reading past the end caps at the stored length.
        assert len(lob.read(size + 10_000)) == size

    @pytest.mark.parametrize("lob_type", _LOB_TYPES, ids=_LOB_IDS)
    @given(
        size=st.integers(min_value=8, max_value=2048),
        data=st.data(),
    )
    @settings(deadline=None, max_examples=15)
    def test_offset_reads(
        self, conn: pycubrid.Connection, lob_type: int, size: int, data: st.DataObject
    ) -> None:
        payload = bytes((i * 3) & 0x7F for i in range(size))
        lob = conn.create_lob(lob_type)
        lob.write(payload)
        offset = data.draw(st.integers(min_value=0, max_value=size - 1))
        length = data.draw(st.integers(min_value=1, max_value=size))
        chunk = lob.read(length, offset=offset)
        assert chunk == payload[offset : offset + length]

    def test_unicode_clob_round_trip(self, conn: pycubrid.Connection) -> None:
        # A CLOB stores UTF-8 bytes; multibyte/CJK text must round-trip intact.
        text = ("가나다 CLOB ☃ 漢字 " * 200).encode("utf-8")
        lob = conn.create_lob(CUBRIDDataType.CLOB)
        written = lob.write(text)
        assert written == len(text)
        assert lob.read(len(text)) == text


class TestLobErrorEdges:
    def test_negative_length_raises(self, conn: pycubrid.Connection) -> None:
        lob = conn.create_lob(CUBRIDDataType.BLOB)
        lob.write(b"abcd")
        with pytest.raises(InterfaceError):
            lob.read(-1)

    def test_negative_offset_raises(self, conn: pycubrid.Connection) -> None:
        lob = conn.create_lob(CUBRIDDataType.BLOB)
        lob.write(b"abcd")
        with pytest.raises(InterfaceError):
            lob.read(2, offset=-1)

    def test_read_after_close_raises(self, conn: pycubrid.Connection) -> None:
        lob = conn.create_lob(CUBRIDDataType.BLOB)
        lob.write(b"abcd")
        lob.close()
        with pytest.raises(InterfaceError):
            lob.read(2)

    def test_write_after_close_raises(self, conn: pycubrid.Connection) -> None:
        lob = conn.create_lob(CUBRIDDataType.BLOB)
        lob.close()
        with pytest.raises(InterfaceError):
            lob.write(b"x")

    def test_zero_length_read_returns_empty_without_server_round_trip(
        self, conn: pycubrid.Connection
    ) -> None:
        # After the #362 read-loop fix, read(0) short-circuits (remaining == 0)
        # and returns b"" with no LOB_READ request, so it no longer triggers the
        # server-side transaction abort the old one-shot read caused.
        lob = conn.create_lob(CUBRIDDataType.BLOB)
        lob.write(b"abcd")
        assert lob.read(0) == b""
        # The connection stays usable (no abort).
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
        cur.close()


class TestLargeLob:
    def test_large_blob_via_chunked_read(self, conn: pycubrid.Connection) -> None:
        # Reading a large LOB in explicit-offset chunks reassembles the full
        # payload; a chunk read never returns more than requested.
        size = 256 * 1024
        data = bytes((i * 131 + 7) & 0xFF for i in range(size))
        lob = conn.create_lob(CUBRIDDataType.BLOB)
        written = lob.write(data)
        assert written == size

        buf = b""
        offset = 0
        while offset < size:
            chunk = lob.read(65536, offset=offset)
            if not chunk:
                break
            assert len(chunk) <= 65536
            buf += chunk
            offset += len(chunk)
        assert buf == data

    def test_single_large_read_returns_full_length(self, conn: pycubrid.Connection) -> None:
        # Regression guard for #362: a single read of the whole LOB returns every
        # byte, because Lob.read loops past the broker's per-response size cap.
        size = 256 * 1024
        data = bytes((i * 131 + 7) & 0xFF for i in range(size))
        lob = conn.create_lob(CUBRIDDataType.BLOB)
        lob.write(data)
        assert lob.read(size) == data


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
