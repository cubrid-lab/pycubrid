"""PEP 249 cursor implementation for pycubrid."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, Sequence

from .constants import CUBRIDStatementType
from ._cursor_common import (
    DescriptionItem,
    DML_BATCH_VERBS,
    bind_parameters,
    build_description,
    escape_string,
    extract_first_keyword,
    format_parameter,
    split_on_placeholders,
)
from .exceptions import InterfaceError, OperationalError, ProgrammingError
from .protocol import (
    BatchExecutePacket,
    CloseQueryPacket,
    ColumnMetaData,
    FetchPacket,
    GetLastInsertIdPacket,
    PrepareAndExecutePacket,
)

if TYPE_CHECKING:
    from .connection import Connection

# Backward-compatible aliases for external imports.
_DML_BATCH_VERBS = DML_BATCH_VERBS
_extract_first_keyword = extract_first_keyword
_split_on_placeholders = split_on_placeholders

_LOGGER = logging.getLogger(__name__)


class Cursor:
    """Database cursor implementing the DB-API 2.0 cursor interface."""

    def __init__(self, connection: Connection) -> None:
        """Initialize a cursor bound to a connection."""
        self._connection = connection
        self._closed = False
        self._description: tuple[DescriptionItem, ...] | None = None
        self._rowcount: int = -1
        self._arraysize: int = 1
        self._query_handle: int | None = None
        self._columns: list[ColumnMetaData] = []
        self._rows: list[tuple[Any, ...]] = []
        self._row_index: int = 0
        self._statement_type: int = 0
        self._total_tuple_count: int = 0
        self._lastrowid: int | None = None
        self._fetch_size: int = connection._fetch_size
        self._timing = connection._timing

    @property
    def description(self) -> tuple[DescriptionItem, ...] | None:
        """Return result-set metadata for the last executed statement."""
        return self._description

    @property
    def rowcount(self) -> int:
        """Return number of rows affected by the last execute call."""
        return self._rowcount

    @property
    def lastrowid(self) -> int | None:
        """Return the last generated identifier for an INSERT statement."""
        return self._lastrowid

    @property
    def arraysize(self) -> int:
        """Return the default number of rows for fetchmany."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        """Set the default number of rows for fetchmany."""
        if value < 1:
            raise ProgrammingError("arraysize must be greater than zero")
        self._arraysize = value

    @property
    def fetch_size(self) -> int:
        """Return the server-side fetch batch size for this cursor.

        This controls how many rows are requested from the CAS broker per
        network round-trip.  Defaults to the connection-level setting.
        """
        return self._fetch_size

    @fetch_size.setter
    def fetch_size(self, value: int) -> None:
        """Set the server-side fetch batch size for this cursor.

        Args:
            value: Number of rows per server fetch (must be >= 1).
        """
        if type(value) is not int or value < 1:
            raise ProgrammingError("fetch_size must be an integer >= 1")
        self._fetch_size = value

    def close(self) -> None:
        """Close the cursor and release the active query handle if present."""
        if self._closed:
            return
        _LOGGER.debug("cursor.close (handle=%s)", self._query_handle)
        try:
            if self._query_handle is not None:
                self._connection._ensure_connected()
                self._connection._send_and_receive(CloseQueryPacket(self._query_handle))
        except (InterfaceError, OperationalError, OSError):
            pass
        finally:
            self._query_handle = None
            self._closed = True
            self._connection._cursors.discard(self)

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | None = None,
    ) -> Cursor:
        """Prepare and execute a SQL statement."""
        self._check_closed()
        self._connection._ensure_connected()

        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()

        if self._query_handle is not None:
            self._connection._send_and_receive(CloseQueryPacket(self._query_handle))
            self._query_handle = None

        sql = operation
        if parameters is not None:
            sql = self._bind_parameters(operation, parameters)

        packet = PrepareAndExecutePacket(
            sql=sql,
            auto_commit=self._connection.autocommit,
            protocol_version=self._connection._protocol_version,
            decode_collections=self._connection._decode_collections,
            json_deserializer=self._connection._json_deserializer,
        )
        self._connection._send_and_receive(packet)
        if _LOGGER.isEnabledFor(logging.DEBUG):
            _LOGGER.debug(
                "execute: type=%d cols=%d rows=%d",
                packet.statement_type,
                packet.column_count,
                packet.total_tuple_count,
            )

        self._query_handle = packet.query_handle
        self._statement_type = packet.statement_type
        self._columns = list(packet.columns)
        self._description = self._build_description(self._columns)
        self._total_tuple_count = packet.total_tuple_count
        self._rows = list(packet.rows)
        self._row_index = 0
        self._lastrowid = None

        if packet.statement_type == CUBRIDStatementType.SELECT:
            self._rowcount = -1
        elif packet.result_infos:
            self._rowcount = packet.result_infos[0].result_count
        else:
            self._rowcount = -1

        if packet.statement_type == CUBRIDStatementType.INSERT:
            try:
                lid_packet = GetLastInsertIdPacket()
                self._connection._send_and_receive(lid_packet)
                if lid_packet.last_insert_id:
                    self._lastrowid = int(lid_packet.last_insert_id)
            except (InterfaceError, OperationalError, OSError, TypeError, ValueError) as exc:
                _LOGGER.debug("lastrowid retrieval failed: %s", exc)
                self._lastrowid = None

        if _timing is not None:
            _timing.record_execute(time.perf_counter_ns() - _start)

        return self

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]],
    ) -> Cursor:
        """Execute the same operation repeatedly with multiple parameter sets.

        For DML statements (INSERT, UPDATE, DELETE) the driver renders all
        parameter sets into complete SQL strings and sends them as a single
        batch request via ``BatchExecutePacket``, reducing N network
        round-trips to one.  SELECT statements fall back to the per-row loop
        to preserve cursor result-set semantics.
        """
        self._check_closed()
        if not seq_of_parameters:
            return self

        # Use DML whitelist: only batch for known DML verbs.
        first_word = _extract_first_keyword(operation)
        is_dml = first_word in _DML_BATCH_VERBS

        if not is_dml:
            return self._executemany_loop(operation, seq_of_parameters)

        # --- DML batch path: render + single RPC --------------------------
        sql_list = [self._bind_parameters(operation, params) for params in seq_of_parameters]
        _LOGGER.debug("executemany: batch_size=%d", len(sql_list))
        self.executemany_batch(sql_list)
        return self

    def _executemany_loop(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]],
    ) -> Cursor:
        """Fallback per-row execution loop (used for SELECT in executemany)."""
        total_rowcount = 0
        has_non_select = False

        for params in seq_of_parameters:
            self.execute(operation, params)
            if self._statement_type != CUBRIDStatementType.SELECT and self._rowcount >= 0:
                total_rowcount += self._rowcount
                has_non_select = True

        if has_non_select:
            self._rowcount = total_rowcount

        return self

    def executemany_batch(
        self,
        sql_list: list[str],
        auto_commit: bool | None = None,
    ) -> list[tuple[int, int]]:
        """Execute multiple SQL statements in a single batch request."""
        self._check_closed()
        self._connection._ensure_connected()

        if auto_commit is None:
            auto_commit = self._connection.autocommit

        packet = BatchExecutePacket(
            sql_list=sql_list,
            auto_commit=auto_commit,
            protocol_version=self._connection._protocol_version,
        )
        self._connection._send_and_receive(packet)

        self._description = None
        self._rows = []
        self._row_index = 0
        self._query_handle = None

        if packet.results:
            self._rowcount = sum(count for _, count in packet.results)
        else:
            self._rowcount = 0

        return packet.results

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row of a query result set."""
        self._check_closed()
        self._check_result_set()

        if self._row_index >= len(self._rows):
            if not self._fetch_more_rows():
                return None

        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch the next set of rows of a query result."""
        self._check_closed()
        self._check_result_set()
        fetch_size = self.arraysize if size is None else size

        rows: list[tuple[Any, ...]] = []
        remaining = fetch_size
        while remaining > 0:
            available = len(self._rows) - self._row_index
            if available <= 0:
                if not self._fetch_more_rows():
                    break
                available = len(self._rows) - self._row_index

            take = min(available, remaining)
            end = self._row_index + take
            rows.extend(self._rows[self._row_index : end])
            self._row_index = end
            remaining -= take
        return rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows of a query result."""
        self._check_closed()
        self._check_result_set()

        rows: list[tuple[Any, ...]] = []
        while True:
            available = len(self._rows) - self._row_index
            if available > 0:
                rows.extend(self._rows[self._row_index :])
                self._row_index = len(self._rows)
            if not self._fetch_more_rows():
                return rows

    def setinputsizes(self, sizes: Any) -> None:
        """DB-API no-op for input size hints."""
        _ = sizes

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """DB-API no-op for output size hints."""
        _ = (size, column)

    def callproc(self, procname: str, parameters: Sequence[Any] = ()) -> Sequence[Any]:
        """Call a stored procedure and return the original parameters."""
        placeholders = ", ".join(["?"] * len(parameters))
        if placeholders:
            sql = "CALL %s(%s)" % (procname, placeholders)
        else:
            sql = "CALL %s()" % procname
        self.execute(sql, parameters)
        return parameters

    def nextset(self) -> None:
        """Not supported — CUBRID does not have multiple result sets."""
        self._check_closed()
        from .exceptions import NotSupportedError

        raise NotSupportedError("CUBRID does not support multiple result sets")

    def __iter__(self) -> Cursor:
        """Return the cursor itself as an iterator over rows."""
        return self

    def __next__(self) -> tuple[Any, ...]:
        """Return the next row or raise StopIteration."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> Cursor:
        """Enter context manager scope with this cursor."""
        self._check_closed()
        return self

    def __exit__(self, *args: object) -> None:
        """Close the cursor when leaving context manager scope."""
        _ = args
        self.close()

    def _check_closed(self) -> None:
        if self._closed:
            raise InterfaceError("Cursor is closed")

    def _check_result_set(self) -> None:
        if self._description is None:
            raise InterfaceError("No result set available")

    def _fetch_more_rows(self) -> bool:
        if self._query_handle is None:
            return False

        if self._row_index >= self._total_tuple_count:
            return False

        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()

        packet = FetchPacket(
            self._query_handle,
            self._row_index,
            fetch_size=self._fetch_size,
            columns=self._columns,
            statement_type=self._statement_type,
            decode_collections=self._connection._decode_collections,
            json_deserializer=self._connection._json_deserializer,
        )
        self._connection._send_and_receive(packet)

        if _timing is not None:
            _timing.record_fetch(time.perf_counter_ns() - _start)

        if not packet.rows:
            return False

        self._rows.extend(packet.rows)
        if _LOGGER.isEnabledFor(logging.DEBUG):
            _LOGGER.debug(
                "fetch: got %d rows (total=%d/%d)",
                len(packet.rows),
                len(self._rows),
                self._total_tuple_count,
            )
        return True

    def _bind_parameters(
        self,
        operation: str,
        parameters: Sequence[Any],
    ) -> str:
        return bind_parameters(
            operation,
            parameters,
            no_backslash_escapes=self._connection._no_backslash_escapes,
        )

    def _format_parameter(self, value: Any) -> str:
        return format_parameter(value, no_backslash_escapes=self._connection._no_backslash_escapes)

    @staticmethod
    def _escape_string(value: str, *, no_backslash_escapes: bool = False) -> str:
        return escape_string(value, no_backslash_escapes=no_backslash_escapes)

    def _build_description(
        self, columns: list[ColumnMetaData]
    ) -> tuple[DescriptionItem, ...] | None:
        return build_description(columns)
