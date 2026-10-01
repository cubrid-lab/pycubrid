"""Async cursor implementation for pycubrid."""

from __future__ import annotations

import contextlib
import logging
import re
import time
from typing import TYPE_CHECKING, Any, Sequence

from pycubrid._cursor_common import (
    CursorParamsMixin,
    DescriptionItem,
    DML_BATCH_VERBS,
    extract_first_keyword,
    _raise_batch_error,
)
from pycubrid.constants import CUBRIDStatementType
from pycubrid.exceptions import DataError, InterfaceError, OperationalError, ProgrammingError

from pycubrid.protocol import (
    BatchExecutePacket,
    CloseQueryPacket,
    ColumnMetaData,
    FetchPacket,
    GetLastInsertIdPacket,
    PrepareAndExecutePacket,
)

_LOGGER = logging.getLogger(__name__)

# Identifier validation for stored procedure names (prevents SQL injection).
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")


if TYPE_CHECKING:
    from pycubrid.aio.connection import AsyncConnection

    _AsyncCursorBase = CursorParamsMixin[AsyncConnection]
else:
    _AsyncCursorBase = CursorParamsMixin


class AsyncCursor(_AsyncCursorBase):
    """Async database cursor implementing a DB-API 2.0–like interface."""

    _connection: AsyncConnection

    def __init__(self, connection: AsyncConnection) -> None:
        self._connection = connection
        self._closed = False
        self._description: tuple[DescriptionItem, ...] | None = None
        self._rowcount: int = -1
        self._arraysize: int = 1
        self._query_handle: int | None = None
        # Physical session generation the query handle was opened on (#488).
        self._handle_generation = 0
        self._columns: list[ColumnMetaData] = []
        self._rows: list[tuple[Any, ...]] = []
        self._row_index: int = 0
        self._fetched_count: int = 0  # total rows fetched from server (absolute position)
        # DataError raised by a fetch page of the current result set (#507).
        self._page_error: DataError | None = None
        self._statement_type: int = 0
        self._total_tuple_count: int = 0
        self._lastrowid: int | None = None
        self._fetch_size: int = connection._fetch_size
        self._timing = connection._timing
        self._invalidated_by_reconnect: bool = False

    @property
    def description(self) -> tuple[DescriptionItem, ...] | None:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def lastrowid(self) -> int | None:
        return self._lastrowid

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if type(value) is not int or value < 1:
            raise ProgrammingError("arraysize must be a positive integer")
        self._arraysize = value

    @property
    def fetch_size(self) -> int:
        """Return the server-side fetch batch size for this cursor."""
        return self._fetch_size

    @fetch_size.setter
    def fetch_size(self, value: int) -> None:
        """Set the server-side fetch batch size for this cursor."""
        if type(value) is not int or value < 1:
            raise ProgrammingError("fetch_size must be an integer >= 1")
        self._fetch_size = value

    def __del__(self) -> None:
        # A cursor dropped without close() releases its handle later (#488).
        connection = getattr(self, "_connection", None)
        if connection is None:
            return
        try:
            connection._defer_dropped_cursor_close(self)
        except Exception:  # noqa: BLE001 - e.g. interpreter shutdown; never raise from __del__
            with contextlib.suppress(Exception):  # logging may be torn down too
                _LOGGER.debug("Could not queue a collected cursor's handle", exc_info=True)

    async def _release_handle(self) -> None:
        """Release the current result's handle before a new request (#488).

        Deferred to the next ``PREPARE_AND_EXECUTE`` when the connection allows
        it, otherwise closed now with ``CLOSE_REQ``.
        """
        handle = self._query_handle
        if handle is None:
            return
        if not self._connection._defer_close(handle, self._handle_generation):
            await self._connection._send_and_receive(CloseQueryPacket(handle), handle_owner=self)
        self._query_handle = None

    async def close(self) -> None:
        if self._closed:
            return
        _LOGGER.debug("cursor.close (handle=%s)", self._query_handle)
        try:
            handle = self._query_handle
            if handle is not None:
                self._connection._ensure_connected()
                if not self._connection._defer_close(handle, self._handle_generation):
                    await self._connection._send_and_receive(
                        CloseQueryPacket(handle), handle_owner=self
                    )
        except (InterfaceError, OperationalError, OSError):
            pass
        finally:
            self._query_handle = None
            self._closed = True
            self._invalidated_by_reconnect = False
            self._connection._cursors.discard(self)

    async def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | None = None,
    ) -> AsyncCursor:
        self._check_closed()
        await self._connection._wait_for_setup_if_needed()
        self._connection._ensure_connected()

        if re.match(r"INSERT\b", extract_first_keyword(operation)):
            self._connection._last_insert_id = None
            self._lastrowid = None

        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()

        # In autocommit the CLOSE_REQ rides on this execute's request (#488).
        await self._release_handle()

        # Once the previous query is closed, a failed execute has no result set.
        self._description = None
        self._columns = []
        self._rows = []
        self._row_index = 0
        self._fetched_count = 0
        self._page_error = None
        self._total_tuple_count = 0
        self._rowcount = -1
        self._lastrowid = None

        sql = operation
        expected_escape_generation = None
        if parameters is not None:
            expected_escape_generation = await self._connection._generation_for_binding()
            sql = self._bind_parameters(operation, parameters)

        packet = PrepareAndExecutePacket(
            sql=sql,
            auto_commit=self._connection.autocommit,
            protocol_version=self._connection._protocol_version,
            decode_collections=self._connection._decode_collections,
            json_deserializer=self._connection._json_deserializer,
        )
        try:
            if expected_escape_generation is None:
                await self._connection._send_and_receive(packet)
            else:
                await self._connection._send_and_receive(
                    packet, expected_escape_generation=expected_escape_generation
                )
        except DataError:
            # A row value failed to decode after the whole reply was read, so
            # the session is intact (#492). Own the server handle the reply
            # opened, with no result set, so the usual lifecycle releases it.
            self._query_handle = packet.query_handle or None
            self._handle_generation = self._connection._physical_generation
            raise
        # Cleared only now: a reconnect before this send flags every cursor.
        self._invalidated_by_reconnect = False
        if _LOGGER.isEnabledFor(logging.DEBUG):
            _LOGGER.debug(
                "execute: type=%d cols=%d rows=%d",
                packet.statement_type,
                packet.column_count,
                packet.total_tuple_count,
            )

        self._query_handle = packet.query_handle
        self._handle_generation = self._connection._physical_generation
        self._statement_type = packet.statement_type
        self._columns = list(packet.columns)
        self._description = self._build_description(self._columns)
        self._total_tuple_count = packet.total_tuple_count
        self._rows = list(packet.rows)
        self._row_index = 0
        self._fetched_count = len(packet.rows)
        self._page_error = None
        self._lastrowid = None

        if packet.statement_type == CUBRIDStatementType.SELECT:
            self._rowcount = -1
        elif packet.result_infos:
            self._rowcount = packet.result_infos[0].result_count
        else:
            self._rowcount = -1

        if packet.statement_type == CUBRIDStatementType.INSERT:
            self._connection._last_insert_id = None
            try:
                lid_packet = GetLastInsertIdPacket()
                await self._connection._send_and_receive(lid_packet)
                if lid_packet.last_insert_id:
                    self._lastrowid = int(lid_packet.last_insert_id)
                    self._connection._last_insert_id = lid_packet.last_insert_id
            except (InterfaceError, OperationalError, OSError, TypeError, ValueError) as exc:
                _LOGGER.debug("lastrowid retrieval failed: %s", exc)
                self._lastrowid = None

        if _timing is not None:
            _timing.record_execute(time.perf_counter_ns() - _start)

        return self

    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]],
    ) -> AsyncCursor:
        self._check_closed()
        if not seq_of_parameters:
            await self._release_handle()
            self._description = None
            self._columns = []
            self._rows = []
            self._row_index = 0
            self._fetched_count = 0
            self._page_error = None
            self._statement_type = 0
            self._total_tuple_count = 0
            self._invalidated_by_reconnect = False
            self._rowcount = 0
            self._lastrowid = None
            return self

        first_word = extract_first_keyword(operation)
        is_dml = first_word in DML_BATCH_VERBS

        if not is_dml:
            total_rowcount = 0
            has_non_select = False
            for params in seq_of_parameters:
                await self.execute(operation, params)
                if self._statement_type != CUBRIDStatementType.SELECT and self._rowcount >= 0:
                    total_rowcount += self._rowcount
                    has_non_select = True
            if has_non_select:
                self._rowcount = total_rowcount
            return self

        await self._connection._wait_for_setup_if_needed()
        self._connection._ensure_connected()
        # Release the previous result first (as execute() does): its CLOSE_REQ
        # can end OUT_TRAN, and the pre-bind check must run after it (#485).
        await self._release_handle()
        expected_escape_generation = await self._connection._generation_for_binding()
        sql_list = [self._bind_parameters(operation, params) for params in seq_of_parameters]
        _LOGGER.debug("executemany: batch_size=%d", len(sql_list))
        await self._executemany_batch(
            sql_list, auto_commit=None, expected_escape_generation=expected_escape_generation
        )
        return self

    async def executemany_batch(
        self,
        sql_list: list[str],
        auto_commit: bool | None = None,
    ) -> list[tuple[int, int]]:
        return await self._executemany_batch(
            sql_list, auto_commit=auto_commit, expected_escape_generation=None
        )

    async def _executemany_batch(
        self,
        sql_list: list[str],
        auto_commit: bool | None,
        *,
        expected_escape_generation: int | None,
    ) -> list[tuple[int, int]]:
        self._check_closed()
        await self._connection._wait_for_setup_if_needed()
        self._connection._ensure_connected()

        await self._release_handle()

        if sql_list:
            self._connection._last_insert_id = None

        if auto_commit is None:
            auto_commit = self._connection.autocommit
        assert auto_commit is not None

        packet = BatchExecutePacket(
            sql_list=sql_list,
            auto_commit=auto_commit,
            protocol_version=self._connection._protocol_version,
        )
        self._description = None
        self._rows = []
        self._row_index = 0
        self._fetched_count = 0
        self._page_error = None
        self._query_handle = None
        self._rowcount = -1
        self._lastrowid = None

        # A failed transport or response parse must not expose prior results.
        if expected_escape_generation is None:
            await self._connection._send_and_receive(packet)
        else:
            await self._connection._send_and_receive(
                packet, expected_escape_generation=expected_escape_generation
            )

        # Raise on per-statement batch failures (issue #186).
        if packet.errors:
            err = packet.errors[0]
            _raise_batch_error(err)

        if packet.results:
            self._rowcount = sum(count for _, count in packet.results)
        else:
            self._rowcount = 0

        return packet.results

    async def fetchone(self) -> tuple[Any, ...] | None:
        self._check_closed()
        self._check_result_set()

        if self._row_index >= len(self._rows):
            if not await self._next_page([]):
                return None

        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        self._check_closed()
        self._check_result_set()
        fetch_size = self.arraysize if size is None else size

        rows: list[tuple[Any, ...]] = []
        remaining = fetch_size
        while remaining > 0:
            available = len(self._rows) - self._row_index
            if available <= 0:
                if not await self._next_page(rows):
                    break
                available = len(self._rows) - self._row_index

            take = min(available, remaining)
            end = self._row_index + take
            rows.extend(self._rows[self._row_index : end])
            self._row_index = end
            remaining -= take
        return rows

    async def fetchall(self) -> list[tuple[Any, ...]]:
        self._check_closed()
        self._check_result_set()

        rows: list[tuple[Any, ...]] = []
        while True:
            available = len(self._rows) - self._row_index
            if available > 0:
                rows.extend(self._rows[self._row_index :])
                self._row_index = len(self._rows)
            if not await self._next_page(rows):
                break
        # All rows consumed — release the buffer to free memory.
        self._rows = []
        self._row_index = 0
        return rows

    def setinputsizes(self, sizes: Any) -> None:
        _ = sizes

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        _ = (size, column)

    async def callproc(self, procname: str, parameters: Sequence[Any] = ()) -> Sequence[Any]:
        """Call a stored procedure and return the original parameters."""
        if not _IDENTIFIER_RE.match(procname):
            raise ProgrammingError(f"Invalid stored procedure name: {procname!r}")
        placeholders = ", ".join(["?"] * len(parameters))
        if placeholders:
            sql = "CALL %s(%s)" % (procname, placeholders)
        else:
            sql = "CALL %s()" % procname
        await self.execute(sql, parameters)
        return parameters

    async def nextset(self) -> None:
        """Not supported — CUBRID does not have multiple result sets."""
        self._check_closed()
        from pycubrid.exceptions import NotSupportedError

        raise NotSupportedError("CUBRID does not support multiple result sets")

    def __aiter__(self) -> AsyncCursor:
        return self

    async def __anext__(self) -> tuple[Any, ...]:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> AsyncCursor:
        self._check_closed()
        return self

    async def __aexit__(self, *args: object) -> None:
        _ = args
        await self.close()

    # -- internal helpers (no I/O) -------------------------------------------

    def _check_closed(self) -> None:
        if self._closed:
            raise InterfaceError("Cursor is closed")

    def _check_result_set(self) -> None:
        if self._description is None:
            raise InterfaceError("No result set available")

    async def _next_page(self, collected: list[tuple[Any, ...]]) -> bool:
        """Load the next page for a fetch call that has ``collected`` rows so far.

        A page that raises ``DataError`` was read in full, so the session stays
        usable and the cursor keeps its handle (#492, #512), but the page
        cannot be returned. The fetch call raises, and the rows it had
        collected stay buffered: the next fetch calls return them without
        contacting the server. After that every fetch raises the same
        ``DataError`` until the cursor executes again or closes, so no row
        past the failing page is returned (#507). The page is not requested
        again: in autocommit mode the CAS may already have closed the result
        after sending its last page.
        """
        if self._page_error is not None:
            if collected:
                return False
            raise self._page_error.with_traceback(None)
        try:
            return await self._fetch_more_rows()
        except DataError as exc:
            self._rows = collected
            self._row_index = 0
            self._page_error = exc
            raise

    async def _fetch_more_rows(self) -> bool:
        if self._query_handle is None:
            if self._invalidated_by_reconnect and self._fetched_count < self._total_tuple_count:
                raise OperationalError(
                    "result set lost due to broker reconnect mid-fetch; "
                    "re-execute the query to continue"
                )
            if self._fetched_count < self._total_tuple_count:
                raise InterfaceError(
                    "result set invalidated before all rows were fetched; "
                    "re-execute the query to continue"
                )
            return False
        if self._fetched_count >= self._total_tuple_count:
            return False

        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()

        packet = FetchPacket(
            self._query_handle,
            self._fetched_count,
            fetch_size=self._fetch_size,
            columns=self._columns,
            statement_type=self._statement_type,
            decode_collections=self._connection._decode_collections,
            json_deserializer=self._connection._json_deserializer,
        )
        await self._connection._send_and_receive(packet, handle_owner=self)

        if _timing is not None:
            _timing.record_fetch(time.perf_counter_ns() - _start)

        if not packet.rows:
            return False

        # Replace the buffer entirely instead of extending.
        # _fetch_more_rows is only called when all buffered rows have been
        # consumed (verified in fetchone/fetchmany/fetchall), so replacing
        # is safe and keeps memory bounded by the fetch page size.
        self._rows = list(packet.rows)
        self._row_index = 0
        self._fetched_count += len(packet.rows)
        if _LOGGER.isEnabledFor(logging.DEBUG):
            _LOGGER.debug(
                "fetch: got %d rows (fetched=%d/%d)",
                len(packet.rows),
                self._fetched_count,
                self._total_tuple_count,
            )
        return True
