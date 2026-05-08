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
from typing import TYPE_CHECKING, Any, Sequence

from .exceptions import ProgrammingError

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


def split_on_placeholders(sql: str) -> list[str]:
    """Split SQL on unquoted, uncommented ``?`` placeholders.

    Tracks four states to skip ``?`` inside:
    - Single-quoted strings (handles doubled ``''`` escapes)
    - Double-quoted identifiers
    - Line comments (``-- ...`` to EOL)
    - Block comments (``/* ... */``)

    Returns a list of *N + 1* parts where *N* is the number of real
    placeholders.
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

        elif c == "-" and i + 1 < n and sql[i + 1] == "-":
            # Line comment: skip to end of line
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
    (``\\x00``), which CUBRID does not support in string parameters.
    """
    if "\x00" in value:
        raise ProgrammingError("string parameter contains null byte")
    if no_backslash_escapes:
        return "'%s'" % value.replace("'", "''")
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    for ch in ("\r", "\n", "\x1a"):
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

    parts = split_on_placeholders(operation)
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
