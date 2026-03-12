"""PEP 249 cursor implementation for pycubrid."""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from .constants import CUBRIDStatementType
from .exceptions import InterfaceError, ProgrammingError
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


DescriptionItem = tuple[str, int, None, None, int, int, bool]


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
        self._rows: list[list[Any]] = []
        self._row_index: int = 0
        self._statement_type: int = 0
        self._total_tuple_count: int = 0
        self._lastrowid: str | None = None
        self._connection._cursors.add(self)

    @property
    def description(self) -> tuple[DescriptionItem, ...] | None:
        """Return result-set metadata for the last executed statement."""
        return self._description

    @property
    def rowcount(self) -> int:
        """Return number of rows affected by the last execute call."""
        return self._rowcount

    @property
    def lastrowid(self) -> str | None:
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

    def close(self) -> None:
        """Close the cursor and release the active query handle if present."""
        if self._closed:
            return

        if self._query_handle is not None:
            self._connection._ensure_connected()
            self._connection._send_and_receive(CloseQueryPacket(self._query_handle))
            self._query_handle = None

        self._closed = True
        self._connection._cursors.discard(self)

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Cursor:
        """Prepare and execute a SQL statement."""
        self._check_closed()
        self._connection._ensure_connected()

        if self._query_handle is not None:
            self._connection._send_and_receive(CloseQueryPacket(self._query_handle))
            self._query_handle = None

        sql = operation
        if parameters is not None:
            sql = self._bind_parameters(operation, parameters)

        packet = PrepareAndExecutePacket(sql=sql, auto_commit=self._connection.autocommit)
        self._connection._send_and_receive(packet)

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
            last_insert_packet = GetLastInsertIdPacket()
            try:
                self._connection._send_and_receive(last_insert_packet)
                self._lastrowid = last_insert_packet.last_insert_id or None
            except Exception:
                self._lastrowid = None

        return self

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> Cursor:
        """Execute the same operation repeatedly with multiple parameter sets."""
        self._check_closed()
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

        packet = BatchExecutePacket(sql_list=sql_list, auto_commit=auto_commit)
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
        return tuple(row)

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch the next set of rows of a query result."""
        self._check_closed()
        self._check_result_set()
        fetch_size = self.arraysize if size is None else size

        rows: list[tuple[Any, ...]] = []
        while len(rows) < fetch_size:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows of a query result."""
        self._check_closed()
        self._check_result_set()

        rows: list[tuple[Any, ...]] = []
        while True:
            row = self.fetchone()
            if row is None:
                return rows
            rows.append(row)

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

        packet = FetchPacket(self._query_handle, self._row_index, fetch_size=100)
        self._connection._send_and_receive(packet)
        if not packet.rows:
            return False

        self._rows.extend(packet.rows)
        return True

    def _bind_parameters(
        self,
        operation: str,
        parameters: Sequence[Any] | Mapping[str, Any],
    ) -> str:
        if isinstance(parameters, Mapping):
            values = list(parameters.values())
        elif isinstance(parameters, Sequence) and not isinstance(
            parameters, (str, bytes, bytearray)
        ):
            values = list(parameters)
        else:
            raise ProgrammingError("parameters must be a sequence or mapping")

        parts = operation.split("?")
        placeholder_count = len(parts) - 1
        if placeholder_count != len(values):
            raise ProgrammingError("wrong number of parameters")

        result = [parts[0]]
        for index, value in enumerate(values, start=1):
            result.append(self._format_parameter(value))
            result.append(parts[index])
        return "".join(result)

    def _format_parameter(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, str):
            return "'%s'" % value.replace("'", "''")
        if isinstance(value, bytes):
            return "X'%s'" % value.hex()
        if isinstance(value, datetime.datetime):
            milliseconds = value.microsecond // 1000
            return "DATETIME'%s.%03d'" % (value.strftime("%Y-%m-%d %H:%M:%S"), milliseconds)
        if isinstance(value, datetime.date):
            return "DATE'%s'" % value.strftime("%Y-%m-%d")
        if isinstance(value, datetime.time):
            return "TIME'%s'" % value.strftime("%H:%M:%S")
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (int, float)):
            return str(value)
        raise ProgrammingError("unsupported parameter type")

    def _build_description(
        self, columns: list[ColumnMetaData]
    ) -> tuple[DescriptionItem, ...] | None:
        if not columns:
            return None
        return tuple(
            (
                column.name,
                column.column_type,
                None,
                None,
                column.precision,
                column.scale,
                column.is_nullable,
            )
            for column in columns
        )
