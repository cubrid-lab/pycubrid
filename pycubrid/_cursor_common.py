"""Shared pure helpers for sync and async cursor implementations.

This module centralises parameter binding, SQL tokenisation, and
result-description logic so that ``cursor.py`` and ``aio/cursor.py``
import from one place instead of duplicating code.

All functions are pure (no I/O, no connection state) and therefore
usable from both sync and async call-sites.
"""

from __future__ import annotations

import datetime
import math
import re
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Generic, Protocol, Sequence, TypeVar

from .exceptions import InterfaceError, ProgrammingError
from .error_codes import CAS_ERROR_TO_EXCEPTION, _DEFAULT_SQLSTATE

if TYPE_CHECKING:
    from .protocol import ColumnMetaData

# ---- constants -------------------------------------------------------------

DescriptionItem = tuple[str, int, None, None, int, int, bool]

# DML verbs eligible for batch execution in executemany().
DML_BATCH_VERBS = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE"})

# Regex to strip leading SQL comments (block /* ... */ and line -- ... to EOL/EOF).
_RE_LEADING_COMMENTS = re.compile(r"^(\s*(/\*.*?\*/|--[^\n]*(\n|$)))*\s*", re.DOTALL)


# ---- SQL parsing -----------------------------------------------------------


def extract_first_keyword(sql: str) -> str:
    """Extract the first SQL keyword, skipping leading comments and whitespace."""
    stripped = _RE_LEADING_COMMENTS.sub("", sql)
    if not stripped:
        return ""
    return stripped.split(None, 1)[0].upper()


def split_on_placeholders(sql: str, *, no_backslash_escapes: bool = True) -> list[str]:
    """Split SQL on unquoted, uncommented ``?`` placeholders.

    Tracks CUBRID lexical contexts to skip ``?`` inside:

    - Single-quoted strings (``'...'``): always honours doubled ``''``
      escapes.  When ``no_backslash_escapes`` is ``False`` (CUBRID system
      parameter ``no_backslash_escapes=no``), a backslash additionally
      escapes the following character, so ``\'`` does not terminate the
      literal and ``\\`` is a literal backslash.  When ``True`` (the
      CUBRID default) a backslash is an ordinary character.
    - Double-quoted identifiers (``"..."``): honours doubled ``""``.
    - Backtick identifiers (`` `...` ``) and bracket identifiers
      (``[...]``): the first closing delimiter terminates (CUBRID does
      not document an escape for these).
    - Line comments (``-- ...`` and ``// ...`` to EOL).
    - Block comments (``/* ... */``).

    Returns a list of *N + 1* parts where *N* is the number of real
    placeholders.

    .. note::
       Double quotes are treated as identifier delimiters, which is
       correct for CUBRID's default ``ansi_quotes=yes``.  Under
       ``ansi_quotes=no`` double quotes delimit *strings*; pycubrid does
       not track that parameter, so such SQL is not specially handled.
    """
    parts: list[str] = []
    start = 0
    i = 0
    n = len(sql)

    while i < n:
        c = sql[i]

        if c == "'":
            # Single-quoted string: advance past closing quote
            i += 1
            while i < n:
                if not no_backslash_escapes and sql[i] == "\\":
                    # Backslash escapes the next char (or ends at EOF)
                    i += 2
                    continue
                if sql[i] == "'":
                    i += 1
                    if i < n and sql[i] == "'":
                        # Doubled quote escape ''
                        i += 1
                    else:
                        break
                else:
                    i += 1

        elif c == '"':
            # Double-quoted identifier: advance past closing quote
            i += 1
            while i < n:
                if sql[i] == '"':
                    i += 1
                    if i < n and sql[i] == '"':
                        i += 1
                    else:
                        break
                else:
                    i += 1

        elif c == "`":
            # Backtick identifier: first closing backtick terminates
            i += 1
            while i < n and sql[i] != "`":
                i += 1
            if i < n:
                i += 1

        elif c == "[":
            # Bracket identifier: first closing bracket terminates
            i += 1
            while i < n and sql[i] != "]":
                i += 1
            if i < n:
                i += 1

        elif c == "-" and i + 1 < n and sql[i + 1] == "-":
            # Line comment: skip to end of line
            i += 2
            while i < n and sql[i] != "\n":
                i += 1

        elif c == "/" and i + 1 < n and sql[i + 1] == "/":
            # C++-style line comment: skip to end of line
            i += 2
            while i < n and sql[i] != "\n":
                i += 1

        elif c == "/" and i + 1 < n and sql[i + 1] == "*":
            # Block comment: skip to */
            i += 2
            while i < n:
                if sql[i] == "*" and i + 1 < n and sql[i + 1] == "/":
                    i += 2
                    break
                i += 1

        elif c == "?":
            # Real placeholder found
            parts.append(sql[start:i])
            i += 1
            start = i

        else:
            i += 1

    parts.append(sql[start:])
    return parts


# ---- parameter formatting --------------------------------------------------


def escape_string(value: str, *, no_backslash_escapes: bool = False) -> str:
    """Escape a string value for safe inclusion in a SQL literal.

    Raises :class:`ProgrammingError` if the string contains a null byte
    (``\\x00``) or a Ctrl-Z byte (``\\x1a``). CUBRID does not support the
    null byte in string parameters, and CUBRID's SQL grammar defines no safe
    literal escape for ``\\x1a`` (there is no MySQL-style ``\\Z``), so it is
    rejected rather than emitted as a raw control byte.
    """
    if "\x00" in value:
        raise ProgrammingError("string parameter contains null byte")
    if "\x1a" in value:
        raise ProgrammingError("string parameter contains Ctrl-Z (0x1A) byte")
    if no_backslash_escapes:
        return "'%s'" % value.replace("'", "''")
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    for ch in ("\r", "\n"):
        if ch in escaped:
            escaped = escaped.replace(ch, "\\" + ch)
    return "'%s'" % escaped


def format_parameter(value: Any, *, no_backslash_escapes: bool = False) -> str:
    """Format a single Python value as a CUBRID SQL literal string."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, str):
        return escape_string(value, no_backslash_escapes=no_backslash_escapes)
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
        return "DATETIME'%s.%03d'" % (value.strftime("%Y-%m-%d %H:%M:%S"), milliseconds)
    if isinstance(value, datetime.date):
        return "DATE'%s'" % value.strftime("%Y-%m-%d")
    if isinstance(value, datetime.time):
        return "TIME'%s'" % value.strftime("%H:%M:%S")
    if isinstance(value, Decimal):
        if value.is_nan() or value.is_infinite():
            raise ProgrammingError("nan and inf are not supported by CUBRID")
        return str(value)
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            raise ProgrammingError("nan and inf are not supported by CUBRID")
        return str(value)
    raise ProgrammingError("unsupported parameter type")


def bind_parameters(
    operation: str,
    parameters: Sequence[Any],
    *,
    no_backslash_escapes: bool = False,
) -> str:
    """Bind *parameters* into *operation* by replacing ``?`` placeholders.

    Returns the fully-rendered SQL string ready for execution.
    """
    if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
        values = list(parameters)
    else:
        raise ProgrammingError("parameters must be a sequence")

    parts = split_on_placeholders(operation, no_backslash_escapes=no_backslash_escapes)
    placeholder_count = len(parts) - 1
    if placeholder_count != len(values):
        raise ProgrammingError("wrong number of parameters")

    result = [parts[0]]
    for index, value in enumerate(values, start=1):
        result.append(format_parameter(value, no_backslash_escapes=no_backslash_escapes))
        result.append(parts[index])
    return "".join(result)


# ---- result description ----------------------------------------------------


def build_description(
    columns: list[ColumnMetaData],
) -> tuple[DescriptionItem, ...] | None:
    """Convert protocol column metadata into a DB-API ``cursor.description``."""
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


# ---- mixin for cursor parameter helpers ------------------------------------


class _EscapeModeSource(Protocol):
    """Structural type for the subset of the connection the mixin reads."""

    @property
    def _no_backslash_escapes(self) -> bool | None: ...


_ConnT = TypeVar("_ConnT", bound=_EscapeModeSource)


class CursorParamsMixin(Generic[_ConnT]):
    """Mixin providing parameter binding/formatting wrappers for cursors.

    Both ``Cursor`` and ``AsyncCursor`` share identical forwarding methods
    to the module-level helpers above.  This mixin eliminates that duplication.

    Parameterised over the concrete connection type so each cursor keeps its
    own (sync vs async) connection API while sharing this escape-mode logic.
    """

    _connection: _ConnT

    def _resolve_escape_mode(self) -> bool:
        """Return the negotiated backslash-escape mode as a concrete bool.

        ``Connection._no_backslash_escapes`` is ``None`` until it is
        negotiated once at connect time; by the time any statement is
        bound it must be a concrete bool.  Guard the invariant so a
        premature call surfaces as :class:`InterfaceError` instead of
        silently mis-escaping.
        """
        mode = self._connection._no_backslash_escapes
        if mode is None:
            raise InterfaceError("connection escape mode not negotiated")
        return mode

    def _bind_parameters(
        self,
        operation: str,
        parameters: Sequence[Any],
    ) -> str:
        return bind_parameters(
            operation,
            parameters,
            no_backslash_escapes=self._resolve_escape_mode(),
        )

    def _format_parameter(self, value: Any) -> str:
        return format_parameter(value, no_backslash_escapes=self._resolve_escape_mode())

    @staticmethod
    def _escape_string(value: str, *, no_backslash_escapes: bool = False) -> str:
        return escape_string(value, no_backslash_escapes=no_backslash_escapes)

    def _build_description(
        self,
        columns: list[ColumnMetaData],
    ) -> tuple[DescriptionItem, ...] | None:
        return build_description(columns)


def _raise_batch_error(err: dict[str, Any]) -> None:
    """Raise the appropriate PEP 249 exception for a batch statement failure.

    Uses CAS error code dispatch (same mapping as protocol._raise_error)
    to select the correct exception class. Falls back to DatabaseError
    for unknown codes.
    """
    code = err.get("code", -1)
    message = err.get("message", "batch execute statement failed")
    exc_name = CAS_ERROR_TO_EXCEPTION.get(code, "DatabaseError")
    sqlstate = _DEFAULT_SQLSTATE.get(exc_name, "HY000")
    # Import here to avoid circular import at module load time.
    from .exceptions import (
        DataError,
        IntegrityError,
        InternalError,
        DatabaseError,
        OperationalError,
    )

    exc_map = {
        "DataError": DataError,
        "IntegrityError": IntegrityError,
        "InternalError": InternalError,
        "OperationalError": OperationalError,
        "ProgrammingError": ProgrammingError,
        "DatabaseError": DatabaseError,
    }
    exc_cls = exc_map.get(exc_name, DatabaseError)
    raise exc_cls(message, code=code, sqlstate=sqlstate)
