"""Offline sync/async parity: replay scripted broker replies through both drivers (#521).

Every scenario below is a list of public operations plus a broker *script*
(:mod:`tests.helpers.replay_broker`). The same scenario is replayed through a
real :class:`pycubrid.Connection` and a real :class:`pycubrid.aio.AsyncConnection`,
each against its own fresh in-process broker that runs the same script over a
real TCP socket, and four observations are compared:

``outcomes``
    For each step, the value it returned or the name of the exception it raised.
``requests``
    Every request the broker received, in order: session number, CAS function
    (or ``HANDSHAKE``/``OPEN_DB``), echoed CAS_INFO and the exact arguments.
``reusable``
    Whether the connection still works after the last step: the result (or the
    exception type) of ``ping(reconnect=False)``.
``sessions``
    How many TCP sessions the broker accepted.

A scenario lists the aspects in which the drivers are *intended* to differ,
with the reason; the test asserts that exactly those aspects differ. An
unintended difference therefore fails the test until it is fixed (or, if the
fix is too large for this suite, until it is filed and listed here with its
issue number). Each scenario also has a ``check`` run against both drivers'
observations, so a scenario cannot silently stop exercising the path it names.

Intended differences that are not wire-visible (and so are absorbed by the step
adapters rather than listed per scenario):

* the async constructor does not connect; ``pycubrid.aio.connect()`` does;
* async autocommit is set with ``await conn.set_autocommit(v)`` instead of the
  ``conn.autocommit = v`` property setter;
* every async I/O method is a coroutine;
* async ``create_lob()`` raises ``NotSupportedError`` (no async LOB support);
* task cancellation exists only on the async side, so it has no sync
  counterpart and is covered by ``tests/test_async_cancellation.py`` instead.

The table of intended and unintended differences found by this harness is
kept in ``docs/DEVELOPMENT.md`` ("Sync/Async Replay Parity").
"""

from __future__ import annotations

import asyncio
import datetime
import struct
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest

import pycubrid
import pycubrid.aio
from pycubrid._cursor_common import format_parameter
from pycubrid.connection import Connection
from pycubrid.constants import CCIDbParam
from pycubrid.constants import CUBRIDDataType as T
from pycubrid.constants import CUBRIDStatementType
from pycubrid.types import Multiset, Sequence, Set

from .helpers.cas_reply import BatchStatement, Column, ResultSet, Value, batch_reply, date, int_
from .helpers.replay_broker import (
    HANDSHAKE,
    IN_TRAN,
    OUT_TRAN,
    Reply,
    Request,
    Script,
    Session,
    error_body,
    execute_body,
    ok_body,
    run_replay_broker,
    with_status,
)

pytestmark = pytest.mark.no_escape_pin

_TIMEOUT = 5.0

Step = tuple[Any, ...]
Outcome = tuple[Any, ...]


@dataclass
class Observation:
    outcomes: list[Outcome]
    requests: list[tuple[object, ...]]
    reusable: object
    sessions: int
    raw_requests: list[Request] = field(default_factory=list)
    driver: str = ""

    def aspects(self) -> dict[str, object]:
        return {
            "outcomes": self.outcomes,
            "requests": self.requests,
            "reusable": self.reusable,
            "sessions": self.sessions,
        }

    def functions(self, session: int | None = None) -> list[str]:
        return [r.function for r in self.raw_requests if session is None or r.session == session]


def _no_script(_request: Request, _session: Session) -> Reply | None:
    return None


def _no_check(_obs: Observation) -> None:
    return None


@dataclass(frozen=True)
class Scenario:
    name: str
    steps: tuple[Step, ...]
    script: Script = _no_script
    results: dict[str, tuple[ResultSet, int]] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)
    check: Callable[[Observation], None] = _no_check
    # aspect -> reason the drivers are meant to differ there
    intended: dict[str, str] = field(default_factory=dict)
    # A known, not yet fixed divergence: the scenario is a strict xfail.
    unintended: str | None = None


# ---------------------------------------------------------------------------
# Step adapters
# ---------------------------------------------------------------------------


def _options(scenario: Scenario, port: int) -> dict[str, Any]:
    options: dict[str, Any] = {
        "host": "127.0.0.1",
        "port": port,
        "database": "testdb",
        "user": "dba",
        "password": "",
        "connect_timeout": _TIMEOUT,
        "read_timeout": _TIMEOUT,
        "no_backslash_escapes": True,
    }
    options.update(scenario.options)
    return options


def _error(exc: BaseException) -> Outcome:
    return ("raise", type(exc).__name__)


class _SyncReplay:
    def __init__(self, options: dict[str, Any]) -> None:
        self.options = options
        self.conn: Any = None
        self.cursor: Any = None
        self.schema: Any = None

    def _cur(self) -> Any:
        if self.cursor is None:
            self.cursor = self.conn.cursor()
        return self.cursor

    def step(self, step: Step) -> Outcome:
        op, *args = step
        try:
            return (op, "ok", self._run(op, *args))
        except Exception as exc:  # noqa: BLE001 - the exception type is the observation
            return (op, *_error(exc))

    def _run(self, op: str, *args: Any) -> Any:
        if op == "open":
            self.conn = pycubrid.connect(**self.options)
            return None
        conn = self.conn
        if op == "connect":
            conn.connect()
        elif op == "close":
            self.cursor = None
            conn.close()
        elif op == "set_autocommit":
            conn.autocommit = args[0]
        elif op == "get_autocommit":
            return conn.autocommit
        elif op == "commit":
            conn.commit()
        elif op == "rollback":
            conn.rollback()
        elif op == "ping":
            return conn.ping(reconnect=args[0])
        elif op == "version":
            return conn.get_server_version()
        elif op == "execute":
            self._cur().execute(*args)
        elif op == "executemany":
            self._cur().executemany(*args)
        elif op == "fetchone":
            return self._cur().fetchone()
        elif op == "fetchall":
            return self._cur().fetchall()
        elif op == "schema":
            self.schema = conn.get_schema_info(*args)
        elif op == "fetch_schema":
            return conn.fetch_schema_info(self.schema)
        elif op == "create_lob":
            return type(conn.create_lob(*args)).__name__
        else:  # pragma: no cover - a typo in a scenario
            raise AssertionError(f"unknown step {op!r}")
        return None

    def reusable(self) -> object:
        if self.conn is None:
            return "never opened"
        try:
            return self.conn.ping(reconnect=False)
        except Exception as exc:  # noqa: BLE001
            return _error(exc)

    def teardown(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:  # noqa: BLE001 # nosec B110 - best-effort teardown
                pass


class _AsyncReplay:
    def __init__(self, options: dict[str, Any]) -> None:
        self.options = options
        self.conn: Any = None
        self.cursor: Any = None
        self.schema: Any = None

    def _cur(self) -> Any:
        if self.cursor is None:
            self.cursor = self.conn.cursor()
        return self.cursor

    async def step(self, step: Step) -> Outcome:
        op, *args = step
        try:
            return (op, "ok", await self._run(op, *args))
        except Exception as exc:  # noqa: BLE001 - the exception type is the observation
            return (op, *_error(exc))

    async def _run(self, op: str, *args: Any) -> Any:
        if op == "open":
            self.conn = await pycubrid.aio.connect(**self.options)
            return None
        conn = self.conn
        if op == "connect":
            await conn.connect()
        elif op == "close":
            self.cursor = None
            await conn.close()
        elif op == "set_autocommit":
            await conn.set_autocommit(args[0])
        elif op == "get_autocommit":
            return conn.autocommit
        elif op == "commit":
            await conn.commit()
        elif op == "rollback":
            await conn.rollback()
        elif op == "ping":
            return await conn.ping(reconnect=args[0])
        elif op == "version":
            return await conn.get_server_version()
        elif op == "execute":
            await self._cur().execute(*args)
        elif op == "executemany":
            await self._cur().executemany(*args)
        elif op == "fetchone":
            return await self._cur().fetchone()
        elif op == "fetchall":
            return await self._cur().fetchall()
        elif op == "schema":
            self.schema = await conn.get_schema_info(*args)
        elif op == "fetch_schema":
            return await conn.fetch_schema_info(self.schema)
        elif op == "create_lob":
            return type(conn.create_lob(*args)).__name__
        else:  # pragma: no cover - a typo in a scenario
            raise AssertionError(f"unknown step {op!r}")
        return None

    async def reusable(self) -> object:
        if self.conn is None:
            return "never opened"
        try:
            return await self.conn.ping(reconnect=False)
        except Exception as exc:  # noqa: BLE001
            return _error(exc)

    async def teardown(self) -> None:
        if self.conn is not None:
            try:
                await self.conn.close()
            except Exception:  # noqa: BLE001 # nosec B110 - best-effort teardown
                pass


def replay_sync(scenario: Scenario) -> Observation:
    with run_replay_broker(scenario.script, results=scenario.results) as broker:
        driver = _SyncReplay(_options(scenario, broker.port))
        try:
            outcomes = [driver.step(step) for step in scenario.steps]
            requests = broker.requests
            reusable = driver.reusable()
        finally:
            driver.teardown()
    return _observation("sync", outcomes, requests, reusable)


def replay_async(scenario: Scenario) -> Observation:
    async def run() -> Observation:
        with run_replay_broker(scenario.script, results=scenario.results) as broker:
            driver = _AsyncReplay(_options(scenario, broker.port))
            try:
                outcomes = [await driver.step(step) for step in scenario.steps]
                requests = broker.requests
                reusable = await driver.reusable()
            finally:
                await driver.teardown()
        return _observation("async", outcomes, requests, reusable)

    return asyncio.run(run())


def _observation(
    driver: str, outcomes: list[Outcome], requests: list[Request], reusable: object
) -> Observation:
    return Observation(
        driver=driver,
        outcomes=outcomes,
        requests=[r.key() for r in requests],
        reusable=reusable,
        sessions=len({r.session for r in requests}),
        raw_requests=requests,
    )


# ---------------------------------------------------------------------------
# Scripts and result sets
# ---------------------------------------------------------------------------


def _on(
    function: str, reply: Callable[[Request, Session], Reply], *, session: int | None = None
) -> Script:
    """Answer ``function`` (optionally only in one session) with ``reply``."""

    def script(request: Request, state: Session) -> Reply | None:
        if request.function == function and (session is None or request.session == session):
            return reply(request, state)
        return None

    return script


def _hang_up_after_ok(request: Request, state: Session) -> Reply:
    """Answer OUT_TRAN, then close the socket: a CAS recycled at a boundary."""
    state.status = OUT_TRAN
    state.results.clear()
    return Reply(ok_body(OUT_TRAN), close=True)


def _after_first_statement(script: Script) -> Script:
    """Run ``script`` only once its session has received a PREPARE_AND_EXECUTE."""

    def wrapped(request: Request, state: Session) -> Reply | None:
        if "PREPARE_AND_EXECUTE" in state.functions:
            return script(request, state)
        return None

    return wrapped


def _reject_handshake(_request: Request, _state: Session) -> Reply:
    return Reply(raw=struct.pack(">i", -1))


def _both(*scripts: Script) -> Script:
    def script(request: Request, state: Session) -> Reply | None:
        for part in scripts:
            reply = part(request, state)
            if reply is not None:
                return reply
        return None

    return script


def _ints(name: str, *values: int) -> ResultSet:
    return ResultSet(name, (Column("n", T.INT, precision=10),), tuple((int_(v),) for v in values))


_ZERO_DATE = Value(T.DATE, lambda w: w.raw(struct.pack(">3h", 0, 0, 0)), None)


def _dates(*cells: Value) -> ResultSet:
    return ResultSet("dates", (Column("d", T.DATE, precision=10),), tuple((c,) for c in cells))


_THREE = {"SELECT n FROM t": (_ints("three", 1, 2, 3), 1)}
_SELECT = ("execute", "SELECT n FROM t")


def _set_autocommit_args(value: int) -> tuple[bytes, ...]:
    return (struct.pack(">i", CCIDbParam.AUTO_COMMIT), struct.pack(">i", value))


def _set_autocommit_requests(obs: Observation, session: int) -> list[tuple[bytes, ...]]:
    return [
        r.args
        for r in obs.raw_requests
        if r.session == session and r.function == "SET_DB_PARAMETER"
    ]


# ---------------------------------------------------------------------------
# Scenario checks (run against both drivers' observations)
# ---------------------------------------------------------------------------


def _check_connect_close(obs: Observation) -> None:
    assert obs.functions() == [HANDSHAKE, "OPEN_DB", "CON_CLOSE"]
    assert obs.outcomes[-1] == ("version", "raise", "InterfaceError")
    assert obs.reusable is False


def _check_reconnect(obs: Observation) -> None:
    assert obs.sessions == 2
    assert obs.functions(1)[:2] == [HANDSHAKE, "OPEN_DB"]
    assert obs.outcomes[-1] == ("version", "ok", "11.4.0.0000")
    assert obs.reusable is True


def _check_autocommit_restored(expected: int) -> Callable[[Observation], None]:
    def check(obs: Observation) -> None:
        # #520: a reconnect after close() re-sends the explicit autocommit.
        assert _set_autocommit_requests(obs, 1) == [_set_autocommit_args(expected)]
        assert obs.reusable is True

    return check


def _check_autocommit_not_sent(obs: Observation) -> None:
    assert _set_autocommit_requests(obs, 1) == []


def _check_autocommit_set(obs: Observation) -> None:
    # Each change is SET_DB_PARAMETER then COMMIT (an OUT_TRAN reply between
    # them is probed with CHECK_CAS like any other request).
    changes = [f for f in obs.functions(0)[2:] if f != "CHECK_CAS"]
    assert changes == ["SET_DB_PARAMETER", "END_TRAN"] * 2
    assert _set_autocommit_requests(obs, 0) == [_set_autocommit_args(1), _set_autocommit_args(0)]
    assert [o for o in obs.outcomes if o[0] == "get_autocommit"] == [
        ("get_autocommit", "ok", True),
        ("get_autocommit", "ok", False),
    ]


def _check_invalidated(obs: Observation) -> None:
    assert obs.outcomes[-1] == ("fetchall", "raise", "InterfaceError")
    functions = obs.functions(0)
    boundary = functions.index("END_TRAN")
    assert functions[boundary - 1] == "CLOSE_REQ_HANDLE"  # handle released first (#485)
    assert "FETCH" not in functions[boundary:]
    assert obs.reusable is True


def _check_out_tran_probe(obs: Observation) -> None:
    functions = obs.functions(0)
    boundary = functions.index("END_TRAN")
    assert functions[boundary + 1 : boundary + 3] == ["CHECK_CAS", "PREPARE_AND_EXECUTE"]
    assert obs.sessions == 1


def _check_one_recovery(obs: Observation) -> None:
    assert obs.sessions == 2
    # The statement runs once, on the replacement session only; the first
    # session never sees it again (SQL is never replayed).
    assert obs.functions(1)[-2:] == ["PREPARE_AND_EXECUTE", "FETCH"]
    assert obs.functions(1).count("PREPARE_AND_EXECUTE") == 1
    assert obs.functions(0).count("PREPARE_AND_EXECUTE") == 1
    assert obs.outcomes[-1] == ("fetchall", "ok", [(1,), (2,), (3,)])


def _check_recovery_restores_autocommit(obs: Observation) -> None:
    _check_one_recovery(obs)
    assert _set_autocommit_requests(obs, 1) == [_set_autocommit_args(1)]


def _check_escape_probe_on_recovery(obs: Observation) -> None:
    assert obs.sessions == 2
    sqls = [r.sql for r in obs.raw_requests if r.session == 1 and r.sql is not None]
    assert sqls == ["SELECT CHAR_LENGTH('\\\\')", "SELECT n FROM t"]


def _check_recovery_fails(obs: Observation) -> None:
    assert obs.outcomes[-1] == ("execute", "raise", "OperationalError")
    assert obs.reusable is False


def _check_bound_sql_fenced(obs: Observation) -> None:
    fenced = obs.outcomes[-2]
    assert fenced == ("execute", "raise", "OperationalError")
    # The SQL bound for session 0 never reaches session 1; the retry does.
    retried = [r.sql for r in obs.raw_requests if r.session == 1 and r.sql is not None]
    assert retried == ["SELECT 'x'"]
    assert obs.outcomes[-1] == ("execute", "ok", None)


def _check_ping_failed(obs: Observation) -> None:
    # A negative CHECK_CAS is reported, not recovered: the confirmed-broken
    # session is retired and nothing reconnects behind the caller's back.
    assert obs.outcomes[1:] == [("ping", "ok", False), ("version", "raise", "InterfaceError")]
    assert obs.sessions == 1
    assert obs.reusable is False


def _escape_rollback_hang_up(session: int) -> Script:
    """Recycle the CAS right after the escape-mode probe's ROLLBACK in ``session``."""

    def script(request: Request, state: Session) -> Reply | None:
        if (
            request.session == session
            and request.function == "END_TRAN"
            and request.args == (b"\x02",)
        ):
            return _hang_up_after_ok(request, state)
        return None

    return script


def _check_setup_survives_recycle(session: int, value: int) -> Callable[[Observation], None]:
    def check(obs: Observation) -> None:
        # The OUT_TRAN left by the probe is verified before autocommit is
        # applied; a recycled CAS is replaced once and configured there.
        assert all(outcome[1] == "ok" for outcome in obs.outcomes), obs.outcomes
        assert obs.sessions == session + 2
        assert _set_autocommit_requests(obs, session) == []
        assert _set_autocommit_requests(obs, session + 1) == [_set_autocommit_args(value)]
        assert obs.reusable is True

    return check


def _check_restore_failure(obs: Observation) -> None:
    assert obs.outcomes[-2:] == [
        ("connect", "raise", "OperationalError"),
        ("version", "raise", "InterfaceError"),
    ]
    assert obs.reusable is False


def _check_ping_restores_once(obs: Observation) -> None:
    assert ("ping", "ok", True) in obs.outcomes
    assert obs.sessions == 2
    assert _set_autocommit_requests(obs, 1) == [_set_autocommit_args(1)]
    assert obs.reusable is True


def _escape_probe_hang_up(session: int) -> Script:
    """Answer the escape probe OUT_TRAN, then recycle the CAS in ``session``."""

    def script(request: Request, state: Session) -> Reply | None:
        if request.session == session and request.sql == "SELECT CHAR_LENGTH('\\\\')":
            state.status = OUT_TRAN
            body = execute_body(_ints("length", 2), handle=1, inline=1)
            return Reply(with_status(body, OUT_TRAN), close=True)
        return None

    return script


def _check_configured_once(obs: Observation) -> None:
    # The probe's own CHECK_CAS replaces the session; that replacement is
    # configured once, and connect() does not configure it again.
    assert all(outcome[1] == "ok" for outcome in obs.outcomes), obs.outcomes
    assert obs.sessions == 3
    assert _set_autocommit_requests(obs, 1) == []
    assert _set_autocommit_requests(obs, 2) == [_set_autocommit_args(1)]
    assert obs.reusable is True


def _check_setter_not_split(obs: Observation) -> None:
    # #551: SET_DB_PARAMETER and its COMMIT must reach one CAS session: the
    # COMMIT's CHECK_CAS finds the CAS recycled, and the one reconnect restores
    # the new value on the replacement before the COMMIT is sent there.
    assert obs.outcomes[1:] == [("set_autocommit", "ok", None), ("get_autocommit", "ok", True)]
    assert obs.sessions == 2
    assert _set_autocommit_requests(obs, 1) == [_set_autocommit_args(1)]
    session_1 = [r.function for r in obs.raw_requests if r.session == 1]
    assert session_1.index("SET_DB_PARAMETER") < session_1.index("END_TRAN")
    assert obs.reusable is True


def _check_setter_failure_keeps_previous_value(sessions: int) -> Callable[[Observation], None]:
    def check(obs: Observation) -> None:
        # #551: a setter that cannot finish on one session fails, retires that
        # session and keeps the previous value; the next connect() restores
        # nothing because autocommit was never explicitly set.
        assert obs.outcomes[-3:] == [
            ("set_autocommit", "raise", "OperationalError"),
            ("connect", "ok", None),
            ("get_autocommit", "ok", False),
        ]
        assert obs.sessions == sessions
        assert _set_autocommit_requests(obs, sessions - 1) == []
        assert obs.reusable is True

    return check


def _check_lob(obs: Observation) -> None:
    expected = {
        "sync": ("create_lob", "ok", "Lob"),
        "async": ("create_lob", "raise", "NotSupportedError"),
    }
    assert obs.outcomes[1] == expected[obs.driver]
    assert obs.reusable is True


def _check_constructor_autocommit_failure(obs: Observation) -> None:
    assert obs.outcomes == [("open", "raise", "OperationalError")]


def _check_constructor_autocommit_not_split(obs: Observation) -> None:
    # SET_DB_PARAMETER and its COMMIT must reach the same CAS session.
    assert obs.outcomes[0] == ("open", "raise", "OperationalError")
    assert obs.sessions == 1


def _check_ping_recovers(obs: Observation) -> None:
    assert ("ping", "ok", True) in obs.outcomes
    assert obs.sessions == 2
    assert obs.reusable is True


def _check_ping_recovery_fails(obs: Observation) -> None:
    assert ("ping", "ok", False) in obs.outcomes
    assert obs.outcomes[-1] == ("version", "raise", "InterfaceError")
    assert obs.reusable is False


def _check_malformed_closes(obs: Observation) -> None:
    assert obs.outcomes[1] == ("version", "raise", "OperationalError")
    assert obs.outcomes[-1] == ("version", "raise", "InterfaceError")
    assert obs.reusable is False


def _check_truncated_row_closes(obs: Observation) -> None:
    assert obs.outcomes[1] == ("execute", "raise", "OperationalError")
    assert obs.outcomes[-1] == ("version", "raise", "InterfaceError")
    assert obs.reusable is False


def _check_data_error_keeps_session(obs: Observation) -> None:
    assert obs.outcomes[1] == ("execute", "raise", "DataError")
    assert obs.outcomes[-1] == ("version", "ok", "11.4.0.0000")
    assert obs.reusable is True
    assert obs.sessions == 1


def _check_fetch_page_data_error(obs: Observation) -> None:
    # fetchall() reaches the bad page and raises; the rows it had collected
    # stay buffered, then every later fetch raises the same DataError (#536).
    assert obs.outcomes[2:] == [
        ("fetchall", "raise", "DataError"),
        ("fetchone", "ok", (datetime.date(2024, 1, 1),)),
        ("fetchone", "ok", (datetime.date(2024, 1, 2),)),
        ("fetchone", "raise", "DataError"),
        ("fetchone", "raise", "DataError"),
    ]
    # The failing page is requested once, never again on retry.
    positions = [
        struct.unpack(">i", r.args[1])[0] for r in obs.raw_requests if r.function == "FETCH"
    ]
    assert positions == [2, 3]
    assert obs.reusable is True


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


_COLLECTION_INSERT = "INSERT INTO t VALUES (?, ?, ?)"


def _check_typed_collections(obs: Observation) -> None:
    assert obs.outcomes[1:] == [
        ("execute", "ok", None),
        ("execute", "raise", "ProgrammingError"),
        ("execute", "raise", "ProgrammingError"),
    ]
    # Only the valid statement is sent; both rejections happen before binding.
    sent = [r.sql for r in obs.raw_requests if r.sql is not None]
    assert sent == ["INSERT INTO t VALUES (SET{3, 1, 1}, MULTISET{'a', 'a'}, SEQUENCE{3, 1, 2})"], (
        sent
    )


def _batch_sql(request: Request) -> list[str]:
    """The SQL statements of an EXECUTE_BATCH request, in order (#568 review).

    Unlike PREPARE_AND_EXECUTE, args[0] is the auto-commit byte and args[1]
    the protocol>3 timeout int; every arg after that is one null-terminated
    SQL string (``BatchExecutePacket.write``, ``pycubrid/protocol.py``).
    """
    return [a.rstrip(b"\x00").decode("utf-8") for a in request.args[2:]]


def _check_executemany_typed_collections(obs: Observation) -> None:
    # executemany() with typed collection parameters renders each row through
    # the same hardened format_parameter() path as execute() and batches them
    # into one EXECUTE_BATCH request (#568 review).
    assert obs.outcomes[1] == ("executemany", "ok", None)
    batch_requests = [r for r in obs.raw_requests if r.function == "EXECUTE_BATCH"]
    assert len(batch_requests) == 1
    assert _batch_sql(batch_requests[0]) == [
        "INSERT INTO t VALUES (SET{1, 2})",
        "INSERT INTO t VALUES (MULTISET{'a', 'a'})",
        "INSERT INTO t VALUES (SEQUENCE{3, 1, 2})",
    ]


_BACKSLASH_SEQUENCE = Sequence(["a\\b"])


def _check_collection_backslash_escape_processing(obs: Observation) -> None:
    # With no_backslash_escapes=False (escape-processing mode), a backslash in
    # a string *element* of a typed collection is doubled exactly like a
    # scalar string parameter (#568 review).
    assert obs.outcomes[1] == ("execute", "ok", None)
    sent = [r.sql for r in obs.raw_requests if r.sql is not None]
    expected_literal = format_parameter(_BACKSLASH_SEQUENCE, no_backslash_escapes=False)
    assert sent == [f"INSERT INTO t VALUES ({expected_literal})"]


def _truncated_execute(request: Request, state: Session) -> Reply:
    body = execute_body(_ints("t", 7), handle=1, inline=1)
    return Reply(body[:-6])  # well framed, but the row cell runs past the end (#533)


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        "constructor_autocommit_failure",
        (("open",),),
        script=_on(
            "SET_DB_PARAMETER", lambda _r, s: Reply(error_body(s.status, -1, "denied")), session=0
        ),
        options={"autocommit": True},
        check=_check_constructor_autocommit_failure,
    ),
    Scenario(
        "constructor_autocommit_cas_recycled",
        (("open",),),
        script=_on("SET_DB_PARAMETER", _hang_up_after_ok, session=0),
        options={"autocommit": True},
        check=_check_constructor_autocommit_not_split,
    ),
    Scenario(
        "constructor_autocommit_survives_recycle_after_escape_probe",
        (("open",), ("get_autocommit",)),
        script=_escape_rollback_hang_up(0),
        options={"autocommit": True, "no_backslash_escapes": None},
        check=_check_setup_survives_recycle(0, 1),
    ),
    Scenario(
        "reconnect_restore_survives_recycle_after_escape_probe",
        (("open",), ("set_autocommit", True), ("close",), ("connect",), ("get_autocommit",)),
        script=_escape_rollback_hang_up(1),
        options={"no_backslash_escapes": None},
        check=_check_setup_survives_recycle(1, 1),
    ),
    Scenario(
        "reconnect_restore_failure_closes_connection",
        (("open",), ("set_autocommit", True), ("close",), ("connect",), ("version",)),
        script=_on(
            "SET_DB_PARAMETER", lambda _r, s: Reply(error_body(s.status, -1, "denied")), session=1
        ),
        check=_check_restore_failure,
    ),
    Scenario(
        "failed_ping_reconnects_and_restores_autocommit_once",
        (("open",), ("set_autocommit", True), ("ping", True), ("version",)),
        script=_on(
            "CHECK_CAS",
            lambda r, s: (
                Reply(error_body(s.status, -1, "db down")) if "END_TRAN" in s.functions else None
            ),
            session=0,
        ),
        check=_check_ping_restores_once,
    ),
    Scenario(
        "reconnect_configured_once_when_escape_probe_sees_recycle",
        (("open",), ("set_autocommit", True), ("close",), ("connect",), ("get_autocommit",)),
        script=_escape_probe_hang_up(1),
        options={"no_backslash_escapes": None},
        check=_check_configured_once,
    ),
    Scenario(
        "autocommit_setter_survives_recycle_after_set_db_parameter",
        (("open",), ("set_autocommit", True), ("get_autocommit",)),
        script=_on("SET_DB_PARAMETER", _hang_up_after_ok, session=0),
        check=_check_setter_not_split,
    ),
    Scenario(
        "autocommit_setter_commit_failure_keeps_previous_value",
        (("open",), ("set_autocommit", True), ("connect",), ("get_autocommit",)),
        script=_on("END_TRAN", lambda _r, s: Reply(error_body(s.status, -1, "denied")), session=0),
        check=_check_setter_failure_keeps_previous_value(2),
    ),
    Scenario(
        # The SET_DB_PARAMETER probe already replaced session 0, so a CAS
        # recycled again before the COMMIT is not replaced a second time.
        "autocommit_setter_replaces_the_session_at_most_once",
        (("open",), ("commit",), ("set_autocommit", True), ("connect",), ("get_autocommit",)),
        script=_both(
            _on("CHECK_CAS", lambda _r, s: Reply(error_body(s.status, -1, "gone")), session=0),
            _on("SET_DB_PARAMETER", _hang_up_after_ok, session=1),
        ),
        check=_check_setter_failure_keeps_previous_value(3),
    ),
    Scenario(
        "create_lob",
        (("open",), ("create_lob", T.BLOB)),
        check=_check_lob,
        intended={
            "outcomes": "async create_lob() raises NotSupportedError: no async LOB support",
            "requests": "so the async driver never sends LOB_NEW",
        },
    ),
    Scenario(
        "connect_close",
        (("open",), ("close",), ("version",)),
        check=_check_connect_close,
    ),
    Scenario(
        "reconnect_after_close",
        (("open",), ("close",), ("connect",), ("version",)),
        check=_check_reconnect,
    ),
    Scenario(
        "reconnect_restores_explicit_autocommit_on",
        (
            ("open",),
            ("set_autocommit", True),
            ("close",),
            ("connect",),
            ("get_autocommit",),
            ("version",),
        ),
        check=_check_autocommit_restored(1),
    ),
    Scenario(
        "reconnect_restores_explicit_autocommit_off",
        (("open",), ("set_autocommit", False), ("close",), ("connect",), ("get_autocommit",)),
        check=_check_autocommit_restored(0),
    ),
    Scenario(
        "reconnect_restores_constructor_autocommit",
        (("open",), ("close",), ("connect",), ("get_autocommit",), ("version",)),
        options={"autocommit": True},
        check=_check_autocommit_restored(1),
    ),
    Scenario(
        "reconnect_leaves_untouched_autocommit_at_default",
        (("open",), ("close",), ("connect",), ("get_autocommit",)),
        check=_check_autocommit_not_sent,
    ),
    Scenario(
        "autocommit_set_and_restore",
        (
            ("open",),
            ("set_autocommit", True),
            ("get_autocommit",),
            ("set_autocommit", False),
            ("get_autocommit",),
        ),
        check=_check_autocommit_set,
    ),
    Scenario(
        "commit_invalidates_open_handle",
        (("open",), _SELECT, ("fetchone",), ("commit",), ("fetchall",)),
        results=_THREE,
        check=_check_invalidated,
    ),
    Scenario(
        "rollback_invalidates_open_handle",
        (("open",), _SELECT, ("fetchone",), ("rollback",), ("fetchall",)),
        results=_THREE,
        check=_check_invalidated,
    ),
    Scenario(
        "out_tran_check_cas_keeps_session",
        (("open",), _SELECT, ("fetchall",), ("commit",), _SELECT, ("fetchall",)),
        results=_THREE,
        check=_check_out_tran_probe,
    ),
    Scenario(
        "check_cas_failure_recovers_once",
        (("open",), _SELECT, ("fetchall",), ("commit",), _SELECT, ("fetchall",)),
        script=_on("END_TRAN", _hang_up_after_ok, session=0),
        results=_THREE,
        check=_check_one_recovery,
    ),
    Scenario(
        "check_cas_recovery_restores_autocommit",
        (
            ("open",),
            ("set_autocommit", True),
            _SELECT,
            ("fetchall",),
            ("commit",),
            _SELECT,
            ("fetchall",),
        ),
        script=_after_first_statement(_on("END_TRAN", _hang_up_after_ok, session=0)),
        results=_THREE,
        check=_check_recovery_restores_autocommit,
    ),
    Scenario(
        "check_cas_recovery_reprobes_escape_mode",
        (("open",), _SELECT, ("fetchall",), ("commit",), _SELECT, ("fetchall",)),
        # Hang up after the first user commit only (request 4 follows the
        # escape probe's PREPARE_AND_EXECUTE, CLOSE_REQ and ROLLBACK).
        script=lambda r, s: (
            _hang_up_after_ok(r, s)
            if r.session == 0 and r.function == "END_TRAN" and r.args == (b"\x01",)
            else None
        ),
        results=_THREE,
        options={"no_backslash_escapes": None},
        check=_check_escape_probe_on_recovery,
    ),
    Scenario(
        "check_cas_recovery_fails",
        (("open",), _SELECT, ("fetchall",), ("commit",), _SELECT),
        script=_both(
            _on("END_TRAN", _hang_up_after_ok, session=0),
            _on(HANDSHAKE, _reject_handshake, session=1),
        ),
        results=_THREE,
        check=_check_recovery_fails,
    ),
    Scenario(
        "sql_bound_to_replaced_session_is_not_sent",
        (
            ("open",),
            ("set_autocommit", True),
            ("schema", 1, "t"),
            ("execute", "SELECT ?", ("x",)),
            ("execute", "SELECT ?", ("x",)),
        ),
        # The auto-committing statement first closes the live schema result;
        # that CLOSE_REQ ends OUT_TRAN and the CAS is recycled (#471/#485).
        script=_on("CLOSE_REQ_HANDLE", _hang_up_after_ok, session=0),
        check=_check_bound_sql_fenced,
    ),
    Scenario(
        "failed_ping_without_reconnect",
        (("open",), ("ping", False), ("version",)),
        script=_on(
            "CHECK_CAS", lambda _r, s: Reply(error_body(s.status, -1, "db down")), session=0
        ),
        check=_check_ping_failed,
    ),
    Scenario(
        "failed_ping_reconnects",
        (("open",), ("ping", True), ("version",)),
        script=_on(
            "CHECK_CAS", lambda _r, s: Reply(error_body(s.status, -1, "db down")), session=0
        ),
        check=_check_ping_recovers,
    ),
    Scenario(
        "failed_ping_recovery_fails",
        (("open",), ("ping", True), ("version",)),
        script=_both(
            _on("CHECK_CAS", lambda _r, _s: Reply(close=True), session=0),
            _on(HANDSHAKE, _reject_handshake, session=1),
        ),
        check=_check_ping_recovery_fails,
    ),
    Scenario(
        "malformed_frame_closes_session",
        (("open",), ("version",), ("version",)),
        script=_on("GET_DB_VERSION", lambda _r, _s: Reply(raw=struct.pack(">i", -5))),
        check=_check_malformed_closes,
    ),
    Scenario(
        "truncated_reply_closes_session",
        (("open",), ("version",), ("version",)),
        script=_on(
            "GET_DB_VERSION",
            lambda _r, _s: Reply(raw=struct.pack(">i", 64) + ok_body(IN_TRAN), close=True),
        ),
        check=_check_malformed_closes,
    ),
    Scenario(
        "truncated_row_closes_session",
        (("open",), ("execute", "SELECT n FROM t"), ("version",)),
        script=_on("PREPARE_AND_EXECUTE", _truncated_execute),
        check=_check_truncated_row_closes,
    ),
    Scenario(
        "data_error_keeps_session",
        (("open",), ("execute", "SELECT d FROM t"), ("version",)),
        results={"SELECT d FROM t": (_dates(_ZERO_DATE), 1)},
        check=_check_data_error_keeps_session,
    ),
    Scenario(
        "fetch_page_data_error_keeps_fetched_rows",
        (
            ("open",),
            ("execute", "SELECT d FROM t"),
            ("fetchall",),
            ("fetchone",),
            ("fetchone",),
            ("fetchone",),
            ("fetchone",),
        ),
        results={"SELECT d FROM t": (_dates(date(2024, 1, 1), date(2024, 1, 2), _ZERO_DATE), 1)},
        options={"fetch_size": 1},
        check=_check_fetch_page_data_error,
    ),
    Scenario(
        # Typed collection parameters render identically on both drivers (#567).
        "typed_collection_parameters",
        (
            ("open",),
            (
                "execute",
                _COLLECTION_INSERT,
                (Set([3, 1, 1]), Multiset(["a", "a"]), Sequence([3, 1, 2])),
            ),
            ("execute", _COLLECTION_INSERT, (Set([Set([1])]), 1, 2)),
            ("execute", _COLLECTION_INSERT, ([1], 1, 2)),
        ),
        check=_check_typed_collections,
    ),
    Scenario(
        # executemany() batches typed collection parameters the same way it
        # batches scalars (#568 review).
        "executemany_typed_collection_parameters",
        (
            ("open",),
            (
                "executemany",
                "INSERT INTO t VALUES (?)",
                [(Set([1, 2]),), (Multiset(["a", "a"]),), (Sequence([3, 1, 2]),)],
            ),
        ),
        script=_on(
            "EXECUTE_BATCH",
            lambda _r, _s: Reply(
                body=batch_reply(
                    (
                        BatchStatement(CUBRIDStatementType.INSERT, 1),
                        BatchStatement(CUBRIDStatementType.INSERT, 1),
                        BatchStatement(CUBRIDStatementType.INSERT, 1),
                    )
                ).data
            ),
        ),
        check=_check_executemany_typed_collections,
    ),
    Scenario(
        # A backslash inside a typed collection's string element is doubled
        # under no_backslash_escapes=False, exactly like a scalar parameter
        # (#568 review).
        "typed_collection_backslash_escape_processing",
        (("open",), ("execute", "INSERT INTO t VALUES (?)", (_BACKSLASH_SEQUENCE,))),
        options={"no_backslash_escapes": False},
        check=_check_collection_backslash_escape_processing,
    ),
)


def _params() -> list[Any]:
    return [
        pytest.param(
            s,
            id=s.name,
            marks=[pytest.mark.xfail(strict=True, reason=s.unintended)] if s.unintended else [],
        )
        for s in SCENARIOS
    ]


@pytest.mark.parametrize("scenario", _params())
def test_sync_and_async_replay_the_same_scenario_identically(scenario: Scenario) -> None:
    sync = replay_sync(scenario)
    aio = replay_async(scenario)

    sync_aspects, async_aspects = sync.aspects(), aio.aspects()
    differing = {name for name in sync_aspects if sync_aspects[name] != async_aspects[name]}
    detail = {
        name: {"sync": sync_aspects[name], "async": async_aspects[name]} for name in differing
    }
    assert differing == set(scenario.intended), f"unexpected sync/async difference: {detail}"

    for name, observation in (("sync", sync), ("async", aio)):
        try:
            scenario.check(observation)
        except AssertionError as exc:
            raise AssertionError(f"{name} driver: {exc}\n{observation.aspects()}") from exc


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s.name for s in SCENARIOS])
def test_replays_are_deterministic(scenario: Scenario) -> None:
    assert replay_sync(scenario).aspects() == replay_sync(scenario).aspects()
    assert replay_async(scenario).aspects() == replay_async(scenario).aspects()


# ---------------------------------------------------------------------------
# Sync-only contracts the parity table does not show
# ---------------------------------------------------------------------------


def _sync_options(port: int, **overrides: Any) -> dict[str, Any]:
    return _options(Scenario("sync", ()), port) | overrides


def test_sync_constructor_autocommit_failure_closes_socket_and_chains_cause() -> None:
    script = _on(
        "SET_DB_PARAMETER", lambda _r, s: Reply(error_body(s.status, -1, "denied")), session=0
    )
    with run_replay_broker(script) as broker:
        # Keep a reference to the half-built object, as compat.native does.
        conn = Connection.__new__(Connection)
        with pytest.raises(pycubrid.OperationalError) as info:
            Connection.__init__(conn, **_sync_options(broker.port, autocommit=True))

    assert isinstance(info.value.__cause__, pycubrid.DatabaseError)
    assert conn._socket is None
    assert conn._connected is False


def test_sync_connect_on_a_live_connection_sends_nothing() -> None:
    with run_replay_broker() as broker:
        conn = pycubrid.connect(**_sync_options(broker.port))
        conn.autocommit = True
        before = len(broker.requests)
        conn.connect()
        assert len(broker.requests) == before
        conn.close()


def test_broker_reports_a_failing_script() -> None:
    def broken(_request: Request, _state: Session) -> Reply | None:
        raise KeyError("typo in a scenario script")

    with pytest.raises(AssertionError, match="replay broker script failed"):
        with run_replay_broker(broken) as broker:
            with pytest.raises(pycubrid.OperationalError):
                pycubrid.connect(**_sync_options(broker.port))
