"""Scripted in-process CAS broker for offline sync/async parity replay (#521).

:class:`ReplayBroker` is a small threaded TCP server that speaks enough of the
CUBRID CAS protocol for a real :class:`pycubrid.Connection` *and* a real
:class:`pycubrid.aio.AsyncConnection` to open sessions, run statements, fetch
pages, end transactions, ping and close. It differs from
:mod:`tests.helpers.fault_broker` (one connection, one fault right after
``OPEN_DATABASE``) in three ways:

* it accepts any number of sequential TCP sessions, so reconnects are real;
* it answers every request through a *script* (a callable that sees the decoded
  request and may return a custom reply or a transport action), falling back to
  deterministic default replies for the requests a scenario does not care about;
* it records every request it receives, per session, as a comparable tuple.

Both drivers are replayed against fresh brokers built from the same script, so
their recorded requests can be compared byte for byte. Replies are built with
:mod:`tests.helpers.cas_reply` (row data, column metadata) and
:func:`tests.helpers.fault_broker.build_open_db_body` (session open).

Everything is offline and bounded: every socket has a timeout and a broker
thread never outlives the test that started it.
"""

from __future__ import annotations

import socket
import struct
import threading
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace

from pycubrid.constants import CASFunctionCode, CUBRIDDataType, CUBRIDStatementType, DataSize

from .cas_reply import (
    SCHEMA_RESULT,
    Column,
    ResultSet,
    Row,
    fetch_reply,
    int_,
    lob_new_reply,
    prepare_and_execute_reply,
    schema_reply,
)
from .fault_broker import build_open_db_body, framed

_HANDSHAKE_LEN = 10
_OPEN_DB_LEN = 628
_SOCKET_TIMEOUT = 5.0

OUT_TRAN = 0
IN_TRAN = 1

# Pseudo function codes for the two framing-free steps of a session open.
HANDSHAKE = "HANDSHAKE"
OPEN_DB = "OPEN_DB"

ESCAPE_PROBE_SQL = "SELECT CHAR_LENGTH('\\\\')"


def cas_info(status: int) -> bytes:
    """CAS_INFO whose first byte is the transaction status (0 = OUT_TRAN)."""
    return bytes((status, 0, 0, 0))


def ok_body(status: int, response_code: int = 0) -> bytes:
    """A reply body carrying only CAS_INFO and a response code."""
    return cas_info(status) + struct.pack(">i", response_code)


def error_body(status: int, error_code: int, message: str) -> bytes:
    """A native CAS error reply: negative response code, error code, message."""
    return (
        cas_info(status)
        + struct.pack(">i", -1)
        + struct.pack(">i", error_code)
        + message.encode("utf-8")
        + b"\x00"
    )


def with_status(body: bytes, status: int) -> bytes:
    """Replace the transaction status byte of a reply body."""
    return bytes((status,)) + body[1:]


def split_args(payload: bytes) -> tuple[bytes, ...]:
    """Split a request payload (after the function code) into its sized args."""
    args: list[bytes] = []
    offset = 0
    while offset + DataSize.INT <= len(payload):
        (size,) = struct.unpack_from(">i", payload, offset)
        offset += DataSize.INT
        size = max(size, 0)
        args.append(bytes(payload[offset : offset + size]))
        offset += size
    return tuple(args)


def fc_name(code: int) -> str:
    try:
        return CASFunctionCode(code).name
    except ValueError:
        return f"FC{code}"


@dataclass(frozen=True)
class Request:
    """One request the broker received.

    ``session`` counts accepted TCP connections from 0. ``index`` counts the
    framed requests of that session from 0 (the handshake and ``OPEN_DB`` are
    not framed and carry index -1). ``cas_info`` is the CAS_INFO the client
    echoed and ``args`` its sized arguments.
    """

    session: int
    index: int
    function: str
    cas_info: bytes = b""
    # Sized arguments of a framed request; the raw bytes of HANDSHAKE/OPEN_DB.
    args: tuple[bytes, ...] = ()

    @property
    def sql(self) -> str | None:
        # PREPARE_AND_EXECUTE sends an argument count first, then the SQL.
        if self.function != "PREPARE_AND_EXECUTE" or len(self.args) < 2:
            return None
        return self.args[1].rstrip(b"\x00").decode("utf-8")

    @property
    def deferred_closes(self) -> tuple[int, ...]:
        """Handle ids a ``PREPARE_AND_EXECUTE`` asks the CAS to free first (#488).

        They are the prepare arguments after the auto-commit flag, as in
        CAS ``fn_prepare_internal`` (JDBC deferred close).
        """
        if self.function != "PREPARE_AND_EXECUTE" or not self.args:
            return ()
        prepare_argc = self.int_arg(0)
        return tuple(struct.unpack(">i", arg)[0] for arg in self.args[4 : 1 + prepare_argc])

    def int_arg(self, position: int) -> int:
        value: int = struct.unpack(">i", self.args[position])[0]
        return value

    def key(self) -> tuple[object, ...]:
        """The comparable form recorded for parity checks."""
        return (self.session, self.function, self.cas_info, self.args)


@dataclass(frozen=True)
class Reply:
    """What the broker does with one request.

    ``body`` (CAS_INFO first) is framed with its DATA_LENGTH unless ``raw`` is
    given, which is sent as is. ``close`` closes the session afterwards; a
    reply with neither ``body`` nor ``raw`` closes it without answering.
    """

    body: bytes | None = None
    raw: bytes | None = None
    close: bool = False


@dataclass
class Result:
    """A result set the broker holds open for FETCH, as a real CAS would."""

    rs: ResultSet
    inline: int
    auto_commit: bool = False


@dataclass
class Session:
    """Per-TCP-session broker state visible to a script."""

    number: int
    status: int = OUT_TRAN
    next_handle: int = 1
    results: dict[int, Result] = field(default_factory=dict)
    # Functions received so far in this session, including the current one.
    functions: list[str] = field(default_factory=list)

    def allocate_handle(self) -> int:
        """The lowest free handle id, as CAS ``hm_new_srv_handle`` allocates."""
        handle = 1
        while handle in self.results:
            handle += 1
        self.next_handle = max(self.next_handle, handle + 1)
        return handle


Script = Callable[[Request, Session], "Reply | None"]


def _no_script(_request: Request, _session: Session) -> Reply | None:
    return None


class ReplayBroker:
    """Threaded multi-session CAS broker driven by a :data:`Script`.

    Args:
        script: Called for every request (including ``HANDSHAKE`` and
            ``OPEN_DB``); returning ``None`` falls back to the default reply.
        results: Result set to return for a ``PREPARE_AND_EXECUTE`` of a given
            SQL text, as ``(result_set, inline_rows)``. SQL not listed returns
            one ``INT`` row. The escape-mode probe always returns ``2``.
        statement_pooling: The broker_info statement-pooling byte of every
            session (``0`` off, ``1`` on).
        free_on_autocommit: Opt in to transaction-ending FC41/final-FETCH/version
            replies, freeing every handle when pooling is off (#584). Disabled
            by default so existing scripted parity fixtures keep their statuses.
    """

    def __init__(
        self,
        script: Script = _no_script,
        *,
        results: dict[str, tuple[ResultSet, int]] | None = None,
        statement_pooling: int = 0,
        free_on_autocommit: bool = False,
    ) -> None:
        self._script = script
        self._results = dict(results or {})
        self._statement_pooling = statement_pooling
        self._free_on_autocommit = free_on_autocommit
        self._lock = threading.Lock()
        self._requests: list[Request] = []
        self._threads: list[threading.Thread] = []
        self._clients: list[socket.socket] = []
        self._sessions = 0
        self._stopping = False
        # First exception raised by a script or a default reply; stop() re-raises
        # it so a broken script fails the test instead of looking like an EOF.
        self.error: BaseException | None = None
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self._server.listen(8)
        self._server.settimeout(0.02)  # accept poll, so stop() returns promptly
        self._acceptor = threading.Thread(target=self._accept_loop, daemon=True)

    @property
    def port(self) -> int:
        port: int = self._server.getsockname()[1]
        return port

    @property
    def requests(self) -> list[Request]:
        with self._lock:
            return list(self._requests)

    def start(self) -> None:
        self._acceptor.start()

    def stop(self) -> None:
        self._stopping = True
        self._acceptor.join(timeout=_SOCKET_TIMEOUT)
        self._server.close()
        with self._lock:
            clients = list(self._clients)
            threads = list(self._threads)
        for client in clients:
            try:
                client.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # already closed by the session thread
            client.close()
        for thread in threads:
            thread.join(timeout=_SOCKET_TIMEOUT)
        if self.error is not None:
            raise AssertionError("replay broker script failed") from self.error

    # -- serving -------------------------------------------------------------

    def _accept_loop(self) -> None:
        while not self._stopping:
            try:
                client, _ = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self._lock:
                session = Session(self._sessions)
                self._sessions += 1
                self._clients.append(client)
                thread = threading.Thread(target=self._serve, args=(client, session), daemon=True)
                self._threads.append(thread)
            thread.start()

    def _record(self, request: Request) -> None:
        with self._lock:
            self._requests.append(request)

    def _serve(self, client: socket.socket, session: Session) -> None:
        client.settimeout(_SOCKET_TIMEOUT)
        try:
            with client:
                # The unframed handshake and OPEN_DATABASE bytes are recorded
                # whole, so a difference in magic, version or credentials is
                # part of the compared request.
                handshake = _recv_exact(client, _HANDSHAKE_LEN)
                if handshake is None:
                    return
                request = Request(session.number, -1, HANDSHAKE, args=(bytes(handshake),))
                if not self._answer(client, session, request):
                    return
                open_db = _recv_exact(client, _OPEN_DB_LEN)
                if open_db is None:
                    return
                request = Request(session.number, -1, OPEN_DB, args=(bytes(open_db),))
                if not self._answer(client, session, request):
                    return
                index = 0
                while True:
                    header = _recv_exact(client, DataSize.DATA_LENGTH + DataSize.CAS_INFO)
                    if header is None:
                        return
                    (length,) = struct.unpack(">i", header[: DataSize.DATA_LENGTH])
                    payload = _recv_exact(client, length)
                    if payload is None or not payload:
                        return
                    request = Request(
                        session.number,
                        index,
                        fc_name(payload[0]),
                        bytes(header[DataSize.DATA_LENGTH :]),
                        split_args(payload[1:]),
                    )
                    index += 1
                    if not self._answer(client, session, request):
                        return
        except OSError:
            return  # the client went away; nothing left to answer
        except Exception as exc:  # noqa: BLE001 - surfaced by stop()
            with self._lock:
                if self.error is None:
                    self.error = exc

    def _answer(self, client: socket.socket, session: Session, request: Request) -> bool:
        self._record(request)
        session.functions.append(request.function)
        reply = self._script(request, session)
        if reply is None:
            reply = self.default_reply(request, session)
        data = reply.raw
        if data is None and reply.body is not None:
            data = framed(reply.body)
        if data is not None:
            client.sendall(data)
        return data is not None and not reply.close

    # -- default replies -----------------------------------------------------

    def default_reply(self, request: Request, session: Session) -> Reply:
        """Deterministic reply of a healthy broker to ``request``."""
        function = request.function
        if function == HANDSHAKE:
            return Reply(raw=struct.pack(">i", 0))
        if function == OPEN_DB:
            return Reply(
                raw=framed(
                    build_open_db_body(
                        cas_info=cas_info(OUT_TRAN), statement_pooling=self._statement_pooling
                    )
                )
            )
        if function == "END_TRAN":
            session.status = OUT_TRAN
            if not self._statement_pooling:
                # CAS ux_end_tran frees every handle; with pooling it keeps them.
                session.results.clear()
            return Reply(ok_body(OUT_TRAN))
        if function == "CON_CLOSE":
            return Reply(ok_body(OUT_TRAN), close=True)
        if function == "CLOSE_REQ_HANDLE":
            session.results.pop(request.int_arg(0), None)
            return Reply(ok_body(session.status))
        if function == "GET_DB_VERSION":
            session.status = IN_TRAN
            if self._free_on_autocommit and request.args[0] == b"\x01":
                self._auto_commit(session)
            return Reply(ok_body(session.status) + b"11.4.0.0000\x00")
        if function == "PREPARE_AND_EXECUTE":
            return self._prepare_and_execute(request, session)
        if function == "FETCH":
            return self._fetch(request, session)
        if function == "LOB_NEW":
            session.status = IN_TRAN
            return Reply(lob_new_reply().data)
        if function == "SCHEMA_INFO":
            handle = session.allocate_handle()
            session.results[handle] = Result(SCHEMA_RESULT, 0)
            session.status = IN_TRAN
            return Reply(schema_reply(SCHEMA_RESULT, query_handle=handle).data)
        # SET_DB_PARAMETER, CHECK_CAS and anything else: success, status kept.
        return Reply(ok_body(session.status))

    def _prepare_and_execute(self, request: Request, session: Session) -> Reply:
        for handle in request.deferred_closes:  # freed before the prepare, as CAS does
            session.results.pop(handle, None)
        sql = request.sql or ""
        if sql == ESCAPE_PROBE_SQL:
            rs, inline = _single_int("length", 2), 1
        else:
            rs, inline = self._results.get(sql, (_single_int("value", 1), 1))
        handle = session.allocate_handle()
        auto_commit = request.args[3] == b"\x01"
        session.results[handle] = Result(rs, inline, auto_commit)
        session.status = IN_TRAN
        body = execute_body(rs, handle=handle, inline=inline)
        if (
            self._free_on_autocommit
            and auto_commit
            and (rs.statement_type != CUBRIDStatementType.SELECT or inline >= len(rs.rows))
        ):
            self._auto_commit(session)
        return Reply(with_status(body, session.status))

    def _auto_commit(self, session: Session) -> None:
        session.status = OUT_TRAN
        if not self._statement_pooling:
            session.results.clear()

    def _fetch(self, request: Request, session: Session) -> Reply:
        handle, start, size = request.int_arg(0), request.int_arg(1), request.int_arg(2)
        result = session.results.get(handle)
        if result is None:
            return Reply(error_body(session.status, -10004, "invalid query handle"))
        rows = result.rs.rows[start - 1 : start - 1 + size]
        body = page_body(result.rs, rows)
        if (
            self._free_on_autocommit
            and result.auto_commit
            and start - 1 + len(rows) >= len(result.rs.rows)
        ):
            self._auto_commit(session)
        return Reply(with_status(body, session.status))


def _single_int(name: str, value: int) -> ResultSet:
    return ResultSet(name, (Column(name, CUBRIDDataType.INT, precision=10),), ((int_(value),),))


def execute_body(rs: ResultSet, *, handle: int, inline: int) -> bytes:
    """A ``PREPARE_AND_EXECUTE`` reply for ``rs`` with only ``inline`` rows inline."""
    return prepare_and_execute_reply(
        replace(rs, rows=rs.rows[:inline]), query_handle=handle, total=len(rs.rows)
    ).data


def page_body(rs: ResultSet, rows: Sequence[Row]) -> bytes:
    """A ``FETCH`` reply carrying ``rows`` of ``rs``."""
    return fetch_reply(replace(rs, rows=tuple(tuple(row) for row in rows))).data


def _recv_exact(sock: socket.socket, size: int) -> bytearray | None:
    buf = bytearray()
    while len(buf) < size:
        try:
            chunk = sock.recv(size - len(buf))
        except OSError:
            return None
        if not chunk:
            return None
        buf.extend(chunk)
    return buf


@contextmanager
def run_replay_broker(
    script: Script = _no_script,
    *,
    results: dict[str, tuple[ResultSet, int]] | None = None,
    statement_pooling: int = 0,
    free_on_autocommit: bool = False,
) -> Iterator[ReplayBroker]:
    """Start a :class:`ReplayBroker`, yield it, and tear it down."""
    broker = ReplayBroker(
        script,
        results=results,
        statement_pooling=statement_pooling,
        free_on_autocommit=free_on_autocommit,
    )
    broker.start()
    try:
        yield broker
    finally:
        broker.stop()
