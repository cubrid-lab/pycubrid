"""Async cursor implementation for pycubrid."""

from __future__ import annotations

import time
from typing import Any, Sequence

from pycubrid.constants import CUBRIDStatementType
from pycubrid.exceptions import InterfaceError, OperationalError, ProgrammingError
from pycubrid.protocol import (
    BatchExecutePacket,
    CloseQueryPacket,
    ColumnMetaData,
    FetchPacket,
    GetLastInsertIdPacket,
    PrepareAndExecutePacket,
)

DescriptionItem = tuple[str, int, None, None, int, int, bool]


class AsyncCursor:
    """Async database cursor implementing a DB-API 2.0–like interface."""

    def __init__(self, connection: Any) -> None:
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
        if value < 1:
            raise ProgrammingError("arraysize must be greater than zero")
        self._arraysize = value

    async def close(self) -> None:
        if self._closed:
            return
        try:
            if self._query_handle is not None:
                self._connection._ensure_connected()
                await self._connection._send_and_receive(CloseQueryPacket(self._query_handle))
        except (InterfaceError, OperationalError, OSError):
            pass
        finally:
            self._query_handle = None
            self._closed = True
            self._connection._cursors.discard(self)

    async def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | None = None,
    ) -> AsyncCursor:
        self._check_closed()
        self._connection._ensure_connected()

        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()

        if self._query_handle is not None:
            await self._connection._send_and_receive(CloseQueryPacket(self._query_handle))
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
        await self._connection._send_and_receive(packet)

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
                await self._connection._send_and_receive(lid_packet)
                if lid_packet.last_insert_id:
                    self._lastrowid = int(lid_packet.last_insert_id)
            except (InterfaceError, OperationalError, OSError, TypeError, ValueError):
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
            return self

        stripped = operation.lstrip()
        is_select = stripped[:6].upper().startswith("SELECT")

        if is_select:
            for params in seq_of_parameters:
                await self.execute(operation, params)
            return self

        sql_list = [self._bind_parameters(operation, params) for params in seq_of_parameters]
        await self.executemany_batch(sql_list)
        return self

    async def executemany_batch(
        self,
        sql_list: list[str],
        auto_commit: bool | None = None,
    ) -> list[tuple[int, int]]:
        self._check_closed()
        self._connection._ensure_connected()

        if auto_commit is None:
            auto_commit = self._connection.autocommit
        assert auto_commit is not None

        packet = BatchExecutePacket(
            sql_list=sql_list,
            auto_commit=auto_commit,
            protocol_version=self._connection._protocol_version,
        )
        await self._connection._send_and_receive(packet)

        self._description = None
        self._rows = []
        self._row_index = 0
        self._query_handle = None

        if packet.results:
            self._rowcount = sum(count for _, count in packet.results)
        else:
            self._rowcount = 0

        return packet.results

    async def fetchone(self) -> tuple[Any, ...] | None:
        self._check_closed()
        self._check_result_set()

        if self._row_index >= len(self._rows):
            if not await self._fetch_more_rows():
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
                if not await self._fetch_more_rows():
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
            if not await self._fetch_more_rows():
                return rows

    def setinputsizes(self, sizes: Any) -> None:
        _ = sizes

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        _ = (size, column)

    async def callproc(self, procname: str, parameters: Sequence[Any] = ()) -> Sequence[Any]:
        placeholders = ", ".join(["?"] * len(parameters))
        if placeholders:
            sql = "CALL %s(%s)" % (procname, placeholders)
        else:
            sql = "CALL %s()" % procname
        await self.execute(sql, parameters)
        return parameters

    async def nextset(self) -> None:
        self._check_closed()
        return None

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

    async def _fetch_more_rows(self) -> bool:
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
        await self._connection._send_and_receive(packet)

        if _timing is not None:
            _timing.record_fetch(time.perf_counter_ns() - _start)

        if not packet.rows:
            return False

        self._rows.extend(packet.rows)
        return True

    def _bind_parameters(
        self,
        operation: str,
        parameters: Sequence[Any],
    ) -> str:
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
            values = list(parameters)
        else:
            raise ProgrammingError("parameters must be a sequence")

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
        import datetime
        import math
        from decimal import Decimal

        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, str):
            return self._escape_string(
                value, no_backslash_escapes=self._connection._no_backslash_escapes
            )
        if isinstance(value, (bytes, bytearray)):
            return "X'%s'" % value.hex()
        if isinstance(value, datetime.datetime):
            milliseconds = value.microsecond // 1000
            if value.tzinfo is not None and value.utcoffset() is not None:
                tz_key = getattr(value.tzinfo, "key", None)
                if tz_key:
                    tz_str = tz_key
                else:
                    offset = value.utcoffset()
                    assert offset is not None
                    total_seconds = int(offset.total_seconds())
                    sign = "+" if total_seconds >= 0 else "-"
                    hours, remainder = divmod(abs(total_seconds), 3600)
                    minutes = remainder // 60
                    tz_str = "%s%02d:%02d" % (sign, hours, minutes)
                return "DATETIMETZ'%s.%03d %s'" % (
                    value.strftime("%Y-%m-%d %H:%M:%S"),
                    milliseconds,
                    tz_str,
                )
            return "DATETIME'%s.%03d'" % (
                value.strftime("%Y-%m-%d %H:%M:%S"),
                milliseconds,
            )
        if isinstance(value, datetime.date):
            return "DATE'%s'" % value.strftime("%Y-%m-%d")
        if isinstance(value, datetime.time):
            return "TIME'%s'" % value.strftime("%H:%M:%S")
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (int, float)):
            if math.isnan(value) or math.isinf(value):
                raise ProgrammingError("nan and inf are not supported by CUBRID")
            return str(value)
        raise ProgrammingError("unsupported parameter type")

    @staticmethod
    def _escape_string(value: str, *, no_backslash_escapes: bool = False) -> str:
        if "\x00" in value:
            raise ProgrammingError("string parameter contains null byte")
        if no_backslash_escapes:
            return "'%s'" % value.replace("'", "''")
        escaped = value.replace("\\", "\\\\").replace("'", "''")
        for ch in ("\r", "\n", "\x1a"):
            if ch in escaped:
                escaped = escaped.replace(ch, "\\" + ch)
        return "'%s'" % escaped

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
