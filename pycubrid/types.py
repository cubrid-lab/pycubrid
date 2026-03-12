"""PEP 249 type objects and constructors for pycubrid.

This module provides the five required DB-API 2.0 type objects
(``STRING``, ``BINARY``, ``NUMBER``, ``DATETIME``, ``ROWID``) and
the seven required constructor functions.

Type objects compare equal to CUBRID CCI_U_TYPE codes that belong
to their category, enabling ``cursor.description`` type comparison::

    if description[1] == pycubrid.STRING:
        ...
"""

from __future__ import annotations

import datetime


class DBAPIType:
    """DB-API 2.0 type object.

    Compares equal to any integer type code in its ``values`` set.
    This follows the PEP 249 specification for type objects.
    """

    def __init__(self, name: str, values: frozenset[int]) -> None:
        self.name = name
        self.values = values

    def __eq__(self, other: object) -> bool:
        if isinstance(other, int):
            return other in self.values
        if isinstance(other, DBAPIType):
            return self.values == other.values
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def __hash__(self) -> int:
        return hash(self.values)

    def __repr__(self) -> str:
        return f"DBAPIType({self.name!r})"


# ---------------------------------------------------------------------------
# CUBRID CCI_U_TYPE codes (from CASConstants.js)
# ---------------------------------------------------------------------------
_CHAR = 1
_STRING = 2
_NCHAR = 3
_VARNCHAR = 4
_BIT = 5
_VARBIT = 6
_NUMERIC = 7
_INT = 8
_SHORT = 9
_MONETARY = 10
_FLOAT = 11
_DOUBLE = 12
_DATE = 13
_TIME = 14
_TIMESTAMP = 15
_SET = 16
_MULTISET = 17
_SEQUENCE = 18
_OBJECT = 19
_RESULTSET = 20
_BIGINT = 21
_DATETIME = 22
_BLOB = 23
_CLOB = 24
_ENUM = 25
_TIMESTAMPTZ = 29
_TIMESTAMPLTZ = 30
_DATETIMETZ = 31
_DATETIMELTZ = 32

# ---------------------------------------------------------------------------
# PEP 249 Type Objects
# ---------------------------------------------------------------------------

STRING = DBAPIType(
    "STRING",
    frozenset({_CHAR, _STRING, _NCHAR, _VARNCHAR, _ENUM, _CLOB}),
)
"""Describes string-based columns (CHAR, VARCHAR, NCHAR, VARNCHAR, ENUM, CLOB)."""

BINARY = DBAPIType(
    "BINARY",
    frozenset({_BIT, _VARBIT, _BLOB}),
)
"""Describes binary columns (BIT, VARBIT, BLOB)."""

NUMBER = DBAPIType(
    "NUMBER",
    frozenset({_NUMERIC, _INT, _SHORT, _MONETARY, _FLOAT, _DOUBLE, _BIGINT}),
)
"""Describes numeric columns (NUMERIC, INT, SHORT, MONETARY, FLOAT, DOUBLE, BIGINT)."""

DATETIME = DBAPIType(
    "DATETIME",
    frozenset(
        {
            _DATE,
            _TIME,
            _TIMESTAMP,
            _DATETIME,
            _TIMESTAMPTZ,
            _TIMESTAMPLTZ,
            _DATETIMETZ,
            _DATETIMELTZ,
        }
    ),
)
"""Describes date/time columns (DATE, TIME, TIMESTAMP, DATETIME and TZ variants)."""

ROWID = DBAPIType(
    "ROWID",
    frozenset({_OBJECT}),
)
"""Describes row-id columns (OBJECT/OID)."""

# ---------------------------------------------------------------------------
# PEP 249 Constructors
# ---------------------------------------------------------------------------


def Date(year: int, month: int, day: int) -> datetime.date:
    """Construct a date value."""
    return datetime.date(year, month, day)


def Time(hour: int, minute: int, second: int) -> datetime.time:
    """Construct a time value."""
    return datetime.time(hour, minute, second)


def Timestamp(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
) -> datetime.datetime:
    """Construct a datetime (timestamp) value."""
    return datetime.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks: float) -> datetime.date:
    """Construct a date value from a Unix timestamp (ticks)."""
    return datetime.date.fromtimestamp(ticks)


def TimeFromTicks(ticks: float) -> datetime.time:
    """Construct a time value from a Unix timestamp (ticks)."""
    return datetime.datetime.fromtimestamp(ticks).time()


def TimestampFromTicks(ticks: float) -> datetime.datetime:
    """Construct a datetime value from a Unix timestamp (ticks)."""
    return datetime.datetime.fromtimestamp(ticks)


def Binary(value: bytes | bytearray | str) -> bytes:
    """Construct a binary value.

    Accepts ``bytes``, ``bytearray``, or ``str`` (encoded as UTF-8).
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    msg = f"Binary() argument must be bytes, bytearray, or str, not {type(value).__name__}"
    raise TypeError(msg)
