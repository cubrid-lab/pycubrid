"""Async cursor implementation for pycubrid."""

from __future__ import annotations

import logging
import time
from typing import Any, Sequence

from pycubrid._cursor_common import (
    DescriptionItem,
    DML_BATCH_VERBS,
    bind_parameters,
    build_description,
    escape_string,
    extract_first_keyword,
    format_parameter,
)
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

_LOGGER = logging.getLogger(__name__)


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
        _LOGGER.debug("cursor.close (handle=%s)", self._query_handle)
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
                await self._connection._send_and_receive(lid_packet)
                if lid_packet.last_insert_id:
                    self._lastrowid = int(lid_packet.last_insert_id)
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

        sql_list = [self._bind_parameters(operation, params) for params in seq_of_parameters]
        _LOGGER.debug("executemany: batch_size=%d", len(sql_list))
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
