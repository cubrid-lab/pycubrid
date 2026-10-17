"""``error_source`` and authentication error classes (#903).

The broker puts an indicator before every error code: ``CAS_ERROR_INDICATOR``
(-1) when the CAS generated the error, ``DBMS_ERROR_INDICATOR`` (-2) when the
database server did (CUBRID ``src/broker/cas.h``, release/10.2 and 11.4). The
driver keeps the error number as sent (the old CAS numbering, for example
-1015) and exposes the indicator as ``DatabaseError.error_source``: ``"cas"``,
``"server"`` or ``None``. Authentication failures, -165 (``ER_AU_INVALID_USER``)
and -171 (``ER_AU_INVALID_PASSWORD``), raise ``OperationalError`` with SQLSTATE
``28000``.
"""

from __future__ import annotations

import copy
import pickle
import struct
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pycubrid._cursor_common import _batch_failure
from pycubrid.aio.connection import AsyncConnection
from pycubrid.compat import cubriddb
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDStatementType, DataSize
from pycubrid.exceptions import DatabaseError, OperationalError, ProgrammingError
from pycubrid.packet import PacketReader
from pycubrid.protocol import (
    ExecutePacket,
    GetSchemaPacket,
    OpenDatabasePacket,
    PrepareAndExecutePacket,
    _parse_result_infos,
    _raise_error,
)

from .test_async import make_mock_stream_pair
from .test_compat_wrapper_collections import _driver, wrapped  # noqa: F401
from .test_connection import build_handshake_response, make_socket

CAS_INFO = b"\x00\xff\xff\xff"
CAS = -1
DBMS = -2


def _error_reply(indicator: int, code: int, message: bytes = b"") -> bytes:
    """Reply body after DATA_LENGTH: CAS_INFO, indicator, code, message and NUL."""
    return CAS_INFO + struct.pack(">ii", indicator, code) + message + b"\x00"


def _framed(body: bytes) -> bytes:
    return struct.pack(">i", len(body) - DataSize.CAS_INFO) + body


# --- _raise_error ------------------------------------------------------------


@pytest.mark.parametrize(
    ("indicator", "source"),
    [(CAS, "cas"), (DBMS, "server"), (None, None), (0, None), (-3, None)],
)
def test_indicator_sets_error_source(indicator: int | None, source: str | None) -> None:
    body = struct.pack(">i", -1015) + b"\x00"
    with pytest.raises(DatabaseError) as raised:
        _raise_error(PacketReader(body), len(body), indicator)
    assert raised.value.error_source == source
    assert raised.value.errno == -1015  # the number on the wire is kept


def test_cas_error_keeps_its_old_number_and_class() -> None:
    # CAS_ER_SCHEMA_TYPE (-10015) arrives as -1015 to a client without renewed codes.
    packet = GetSchemaPacket(schema_type=99)
    with pytest.raises(DatabaseError) as raised:
        packet.parse(_error_reply(CAS, -1015))
    assert type(raised.value) is DatabaseError
    assert (raised.value.errno, raised.value.sqlstate) == (-1015, "HY000")
    assert raised.value.error_source == "cas"


def test_server_error_reports_server() -> None:
    packet = PrepareAndExecutePacket("SELEC 1")
    with pytest.raises(ProgrammingError) as raised:
        packet.parse(_error_reply(DBMS, -493, b"Syntax error"))
    assert raised.value.error_source == "server"


@pytest.mark.parametrize(
    ("code", "description"), [(-165, "Invalid user"), (-171, "Invalid password")]
)
def test_authentication_failure_is_operational_28000(code: int, description: str) -> None:
    packet = OpenDatabasePacket(database="testdb", user="dba", password="x")
    with pytest.raises(OperationalError) as raised:
        packet.parse(_error_reply(DBMS, code, b"Incorrect or missing password."))
    exc = raised.value
    assert (exc.errno, exc.sqlstate, exc.error_source) == (code, "28000", "server")
    assert f"description={description!r}" in str(exc)


def test_prepared_result_info_error_reports_its_indicator() -> None:
    body = bytes((CUBRIDStatementType.INSERT,)) + struct.pack(">iii", DBMS, -494, 0)
    with pytest.raises(DatabaseError) as raised:
        _parse_result_infos(PacketReader(body), 1, prepared=True)
    assert raised.value.error_source == "server"


# --- the attribute -------------------------------------------------------------


def test_error_source_is_read_only_and_none_by_default() -> None:
    exc = OperationalError("driver-side failure")
    assert exc.error_source is None
    with pytest.raises(AttributeError):
        setattr(exc, "error_source", "cas")


def test_error_source_survives_pickle_and_copy() -> None:
    body = struct.pack(">i", -1015) + b"\x00"
    with pytest.raises(DatabaseError) as raised:
        _raise_error(PacketReader(body), len(body), CAS)
    for clone in (
        pickle.loads(pickle.dumps(raised.value)),
        copy.copy(raised.value),
        copy.deepcopy(raised.value),
    ):
        assert clone.error_source == "cas"


# --- batch rows ------------------------------------------------------------------


@pytest.mark.parametrize(("protocol_version", "source"), [(8, "server"), (3, "server"), (2, None)])
def test_batch_row_errors_report_the_server(protocol_version: int, source: str | None) -> None:
    errors = [{"code": -670, "message": "dup", "index": 1}]
    exc = _batch_failure(((1, 0, ""), (-1, -670, "dup")), errors, errors[0], protocol_version)
    assert exc.error_source == source
    assert exc.batch_errors is not None
    assert [entry.error_source for _, entry in exc.batch_errors] == [source]


# --- both drivers ----------------------------------------------------------------


@pytest.fixture
def socket_queue(monkeypatch: pytest.MonkeyPatch) -> list[MagicMock]:
    queue: list[MagicMock] = []

    def fake_create_connection(*args: object, **kwargs: object) -> MagicMock:
        del args, kwargs
        return queue.pop(0)

    monkeypatch.setattr("socket.create_connection", fake_create_connection)
    return queue


@pytest.mark.parametrize("code", [-165, -171])
def test_sync_connect_with_bad_credentials_raises_operational_error(
    socket_queue: list[MagicMock], code: int
) -> None:
    reply = _framed(_error_reply(DBMS, code, b"Incorrect or missing password."))
    socket_queue.append(make_socket([build_handshake_response(), reply[:4], reply[4:]]))
    with pytest.raises(OperationalError) as raised:
        Connection("localhost", 33000, "testdb", "dba", "bad")
    assert (raised.value.errno, raised.value.sqlstate) == (code, "28000")
    assert raised.value.error_source == "server"


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [-165, -171])
async def test_async_connect_with_bad_credentials_raises_operational_error(code: int) -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "bad")
    reply = _framed(_error_reply(DBMS, code, b"Incorrect or missing password."))
    reader, writer, _ = make_mock_stream_pair([build_handshake_response(), reply[:4], reply[4:]])
    with patch.object(conn, "_open_connection", return_value=(reader, writer)):
        with pytest.raises(OperationalError) as raised:
            await conn.connect()
    assert (raised.value.errno, raised.value.sqlstate) == (code, "28000")
    assert raised.value.error_source == "server"


def test_compat_redaction_keeps_error_source(
    wrapped: cubriddb.Connection,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = _driver(wrapped)
    cur = wrapped.cursor()
    original = driver._send_and_receive

    def failing(packet: Any, **kwargs: Any) -> Any:
        if isinstance(packet, ExecutePacket):
            error = ProgrammingError("server text with bound data", code=-494)
            setattr(error, "_cas_server_error", True)
            error._error_source = "server"
            raise error
        return original(packet, **kwargs)

    monkeypatch.setattr(driver, "_send_and_receive", failing)
    with pytest.raises(ProgrammingError, match="failed on server") as raised:
        cur.execute("INSERT INTO t VALUES (?)", (1,))
    assert raised.value.error_source == "server"
    assert "bound data" not in str(raised.value)


# --- CAS_ER_NUM_BIND (#990) ---------------------------------------------------


def test_cas_num_bind_is_a_programming_error_with_an_explanation() -> None:
    # The CAS counts a ? inside a quoted identifier as a marker and replies
    # -1007 with no text when fewer values were bound.
    packet = PrepareAndExecutePacket(sql='SELECT 1 AS "a?b"', auto_commit=True)
    with pytest.raises(ProgrammingError) as raised:
        packet.parse(_error_reply(CAS, -1007))
    error = raised.value
    assert (error.errno, error.code, error.error_source) == (-1007, -1007, "cas")
    assert "including any inside a quoted identifier" in str(error)


def test_cas_num_bind_keeps_text_the_broker_sent() -> None:
    body = struct.pack(">i", -1007) + b"broker text\x00"
    with pytest.raises(ProgrammingError, match=r"^broker text \(the CUBRID broker counted"):
        _raise_error(PacketReader(body), len(body), CAS)


@pytest.mark.parametrize("indicator", [DBMS, None])
def test_num_bind_mapping_needs_the_cas_indicator(indicator: int | None) -> None:
    # The CAS numbering overlaps server codes; a server -1007 is not CAS_ER_NUM_BIND.
    body = struct.pack(">i", -1007) + b"\x00"
    with pytest.raises(DatabaseError) as raised:
        _raise_error(PacketReader(body), len(body), indicator)
    assert type(raised.value) is DatabaseError
    assert "quoted identifier" not in str(raised.value)
