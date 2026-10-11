"""Qualified CUBRIDdb-style row cursors over the explicit native prepared path.

Its cursors keep the official wrapper's row and converter behavior while
reusing the already-supported native FC2/FC3 subset: INT32/string/NULL scalars
and collections through the native ``set.imports()``/``bind_set()`` (#610).
This module does not add wrapper LOB binding, positional scrolling or ordinary
DB-API execution.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date, time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Iterator, Protocol

from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
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
# Exact positional argument types bound as a collection, like the official
# is_iterable() check but without dicts, generators or other iterables.
_COLLECTION_ARGS = (list, tuple, set, frozenset)
_BIT_CODES = frozenset({CUBRIDDataType.BIT, CUBRIDDataType.VARBIT})


def _element_code(value: Any) -> int:
    """The official get_set_element_type() category of one non-None element."""
    # Same order as upstream: bool is an int, and datetime is a date.
    if isinstance(value, int):
        return CUBRIDDataType.INT
    if isinstance(value, float):
        return CUBRIDDataType.FLOAT
    if isinstance(value, Decimal):
        return CUBRIDDataType.NUMERIC
    if isinstance(value, date):
        return CUBRIDDataType.DATE
    if isinstance(value, time):
        return CUBRIDDataType.TIME
    if isinstance(value, str):
        return CUBRIDDataType.STRING
    if isinstance(value, (bytes, bytearray)):
        return CUBRIDDataType.VARBIT
    raise ProgrammingError("unsupported collection element type")


def _infer_code(elements: Iterable[Any]) -> int:
    """Infer one element type code; ``None`` elements are skipped."""
    chosen: int | None = None
    for value in elements:
        if value is None:
            continue
        code = _element_code(value)
        if chosen is None:
            chosen = code
        elif code != chosen:
            raise TypeError(
                f"Iterable contains elements of different types: {int(code)} != {int(chosen)}"
            )
    return CUBRIDDataType.STRING if chosen is None else chosen


def _collection_code(set_type: Any, index: int) -> Any:
    """The explicit code for one-based ``index``, or ``None`` to infer it."""
    if isinstance(set_type, (list, tuple)):  # as upstream, subclasses included
        try:
            return set_type[index - 1]
        except IndexError:
            return None
    return set_type


def _positional(args: Any) -> tuple[Any, ...]:
    if args is None:
        return ()
    if type(args) in (tuple, list):
        return tuple(args)
    if type(args) in (int, str):
        return (args,)
    raise ProgrammingError("wrapper args must be positional native scalar values")


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

    def _collection(self, value: Any, code: Any) -> Any:
        """Build a native set for one collection argument; no I/O."""
        elements = tuple(value)
        if code is None:
            code = _infer_code(elements)
        elif isinstance(code, bool) or not isinstance(code, int):
            raise InterfaceError("collection element type must be an int type code")
        if code in _BIT_CODES:
            raise NotSupportedError("BIT/VARBIT collection elements are not supported")
        adapted: list[str | None] = []
        for element in elements:
            if element is None:
                adapted.append(None)  # a NULL element, not the official text 'None'
            elif isinstance(element, str):
                adapted.append(element)
            elif _element_code(element) == CUBRIDDataType.VARBIT:
                raise NotSupportedError("BIT/VARBIT collection elements are not supported")
            else:
                adapted.append(str(element))  # the official adapt=str text
        collection = self.con.connection.set()
        collection.imports(tuple(adapted), code)
        return collection

    def _plan(self, args: Any, set_type: Any) -> list[tuple[bool, Any]]:
        """Check one parameter group and build its collections before any I/O."""
        plan: list[tuple[bool, Any]] = []
        for index, value in enumerate(_positional(args), 1):
            if type(value) in _COLLECTION_ARGS:
                code = None if set_type is None else _collection_code(set_type, index)
                plan.append((True, self._collection(value, code)))
            elif value is None or (not isinstance(value, bool) and isinstance(value, (int, str))):
                plan.append((False, value))  # range/NUL/charset are checked by bind
            else:
                raise ProgrammingError("unsupported prepared parameter type")
        return plan

    @staticmethod
    def _bind(cursor: NativeCursor, plan: list[tuple[bool, Any]]) -> None:
        for index, (is_collection, value) in enumerate(plan, 1):
            if is_collection:
                cursor.bind_set(index, value)
            else:
                cursor.bind_param(index, value)

    def _snapshot(self, cursor: NativeCursor, result: int | None) -> None:
        statement_type = cursor._statement_type
        if result is None:  # executemany() with no parameter groups
            self.rowcount = -1
        else:
            self.rowcount = result if statement_type in _ROWCOUNT_TYPES else -1
        if result is not None and statement_type == CUBRIDStatementType.SELECT:
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

    def _discard(self, cursor: NativeCursor) -> None:
        """Drop the previous result and snapshot without I/O, as a failed call does.

        Upstream prepares before binding, so a bind error leaves no fetchable
        result; the open handle is released by the next prepare or close.
        """
        with cursor._connection._session_lock:
            cursor._invalidate_result()
        self.rowcount = -1
        self.description = None
        self._native_description = None

    def execute(self, query: str, args: Any = None, set_type: Any = None) -> int:
        """Prepare, bind and execute once; returns the native result count.

        Positional INT32/string/NULL scalars bind as native parameters; a
        ``list``, ``tuple``, ``set`` or ``frozenset`` argument binds as a native
        collection. ``set_type`` is one element type code for every collection
        argument or a per-position list/tuple (missing or ``None`` entries are
        inferred). Argument, type and collection errors are raised before any
        I/O and discard the previous result, ``rowcount`` and ``description``.
        Scalar range, NUL and charset errors are raised at bind time, after the
        prepare.
        """
        cursor = self._open()
        try:
            plan = self._plan(args, set_type)
        except BaseException:
            self._discard(cursor)
            raise
        cursor.prepare(query)
        self._bind(cursor, plan)
        result = cursor.execute()
        self._snapshot(cursor, result)
        return result

    def executemany(self, query: str, args_list: Iterable[Any]) -> None:
        """Prepare once, then bind and execute each parameter group in order.

        Every group follows the ``execute()`` rules with inferred collection
        types and is checked before the statement is prepared; an error there
        discards the previous result like ``execute()``. After the prepare,
        every group's value count must equal the placeholder count before any
        group runs. ``rowcount`` and ``description`` come from the last
        execution; with no groups the statement is only prepared and they are
        ``-1`` and ``None`` (pycubrid's own choice).
        """
        cursor = self._open()
        try:
            plans = [self._plan(args, None) for args in args_list]
        except BaseException:
            self._discard(cursor)
            raise
        cursor.prepare(query)
        expected = cursor._bind_count
        for number, plan in enumerate(plans, 1):
            if len(plan) != expected:
                # Checked for every group first, so no group runs.
                raise ProgrammingError(
                    f"executemany group {number} has {len(plan)} values for {expected} placeholders"
                )
        result: int | None = None
        for plan in plans:
            self._bind(cursor, plan)
            result = cursor.execute()
        self._snapshot(cursor, result)

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
