"""Qualified CUBRIDdb-style row cursors over the explicit native scalar path.

This module does not add wrapper collection/LOB binding, positional scrolling
or ordinary DB-API execution. Its cursors keep the official wrapper's row and
converter behavior while reusing the already-supported native FC2/FC3 subset.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Iterator, Protocol

from pycubrid.constants import CUBRIDStatementType
from pycubrid.exceptions import InterfaceError, NotSupportedError, ProgrammingError

if TYPE_CHECKING:
    from pycubrid.compat.native import connection as NativeConnection
    from pycubrid.compat.native import cursor as NativeCursor

_LOGGER = logging.getLogger(__name__)
Description = tuple[tuple[str, int, int, int, int, int, int], ...]
_ROWCOUNT_TYPES = frozenset(
    {
        CUBRIDStatementType.SELECT,
        CUBRIDStatementType.INSERT,
        CUBRIDStatementType.UPDATE,
        CUBRIDStatementType.DELETE,
        CUBRIDStatementType.CALL,
    }
)


class _RowCursorOwner(Protocol):
    """Only the connection state consumed by the qualified row cursors."""

    @property
    def connection(self) -> NativeConnection: ...

    fetch_value_converter: Any


class _CursorBase:
    """Shared fetch mechanics; only the qualified concrete classes are public."""

    def __init__(self, conn: _RowCursorOwner) -> None:
        self.con = conn
        self._cs: NativeCursor | None = conn.connection.cursor()
        self.arraysize = 1
        self.rowcount = -1
        self.description: Any = None  # public writable snapshot, as upstream
        self._native_description: Description | None = None

    def _open(self) -> NativeCursor:
        cursor = self._cs
        if cursor is None:
            raise InterfaceError("The cursor has been closed. No operation is allowed any more.")
        return cursor

    def close(self) -> None:
        cursor = self._open()
        cursor.close()
        self._cs = None

    def __del__(self) -> None:
        # Native prepared owners are strongly registered on their connection.
        # Detach first; a destructor must not reconnect or raise during GC.
        try:
            cursor = getattr(self, "_cs", None)
            if cursor is None:
                return
            self._cs = None
            cursor._close_collected_wrapper()
        except BaseException:
            try:
                # Shutdown may have dismantled logging; never emit SQL or errors.
                _LOGGER.warning("Failed to release a collected wrapper cursor")
            except BaseException:
                return

    def execute(self, query: str, args: Any = None, set_type: Any = None) -> int:
        """Run only scalars that the existing native prepared cursor can bind."""
        cursor = self._open()
        if set_type is not None:
            raise NotSupportedError("wrapper set_type binding is not supported")
        if args is None:
            values: tuple[Any, ...] | list[Any] = ()
        elif type(args) in (tuple, list):
            values = args
        elif type(args) in (int, str):
            values = (args,)
        else:
            raise ProgrammingError("wrapper args must be positional native scalar values")
        cursor.prepare(query)
        for index, value in enumerate(values, 1):
            cursor.bind_param(index, value)
        result = cursor.execute()
        statement_type = cursor._statement_type
        self.rowcount = result if statement_type in _ROWCOUNT_TYPES else -1
        if statement_type == CUBRIDStatementType.SELECT:
            metadata: Description = tuple(
                (
                    column.name,
                    int(column.column_type),
                    0,
                    0,
                    int(column.precision),
                    int(column.scale),
                    int(column.is_nullable),
                )
                for column in cursor._columns
            )
            self._native_description = metadata
            self.description = metadata
        else:
            self._native_description = None
            self.description = None
        return result

    def _shape(self, row: tuple[Any, ...]) -> Any:
        return row

    def fetchone(self) -> Any:
        row = self._open().fetch_row()
        if row is None:
            return None
        shaped = self._shape(row)
        converter = self.con.fetch_value_converter
        if shaped and converter:
            return converter(shaped, self._native_description)
        return shaped

    def _fetch_many(self, size: int) -> list[Any]:
        rows: list[Any] = []
        while size < 0 or len(rows) < size:
            row = self.fetchone()
            if not row:  # official bulk loop consumes this falsey result
                break
            rows.append(row)
        return rows

    def fetchmany(self, size: int | None = None) -> list[Any]:
        self._open()
        selected = self.arraysize if size is None else size
        if selected <= 0:
            return []
        return self._fetch_many(selected)

    def fetchall(self) -> list[Any]:
        self._open()
        return self._fetch_many(-1)

    def __iter__(self) -> Iterator[Any]:
        self._open()
        return self

    def __next__(self) -> Any:
        self._open()
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def next(self) -> Any:
        """Match the official wrapper's explicit next() alias."""
        return self.__next__()


class Cursor(_CursorBase):
    """Return native decoded rows as tuples."""


class DictCursor(_CursorBase):
    """Return exact metadata names as dictionary keys (last duplicate wins)."""

    def _shape(self, row: tuple[Any, ...]) -> dict[str, Any]:
        metadata = self._native_description
        if metadata is None:
            raise InterfaceError("cursor has no SELECT description")
        return {column[0]: value for column, value in zip(metadata, row)}


__all__ = ["Cursor", "DictCursor"]
