"""PEP 249 type objects and constructors for pycubrid.

This module provides the five required DB-API 2.0 type objects
(``STRING``, ``BINARY``, ``NUMBER``, ``DATETIME``, ``ROWID``) and
the seven required constructor functions, plus the typed collection
parameters :class:`Set`, :class:`Multiset` and :class:`Sequence`.

Type objects compare equal to CUBRID CCI_U_TYPE codes that belong
to their category, enabling ``cursor.description`` type comparison::

    if description[1] == pycubrid.STRING:
        ...
"""

from __future__ import annotations

import copy
import datetime
from collections.abc import Iterable, Iterator
from typing import Any


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
        return NotImplemented  # noqa: PYI034

    def __ne__(self, other: object) -> bool:
        if isinstance(other, int):
            return other not in self.values
        if isinstance(other, DBAPIType):
            return self.values != other.values
        return NotImplemented  # noqa: PYI034

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
_JSON = 34

# ---------------------------------------------------------------------------
# PEP 249 Type Objects
# ---------------------------------------------------------------------------

STRING = DBAPIType(
    "STRING",
    frozenset({_CHAR, _STRING, _NCHAR, _VARNCHAR, _ENUM, _CLOB, _JSON}),
)
"""Describes string-based columns (CHAR, VARCHAR, NCHAR, VARNCHAR, ENUM, CLOB, JSON)."""

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


# ---------------------------------------------------------------------------
# Typed collection parameters
# ---------------------------------------------------------------------------


class _Collection:
    """Immutable tuple of elements bound as one typed CUBRID collection literal.

    Plain Python ``set``/``list``/``tuple`` values stay rejected as parameters;
    wrap the elements in :class:`Set`, :class:`Multiset` or :class:`Sequence`
    to choose the collection type explicitly. The classes cannot be subclassed,
    and the renderer reads the stored tuple directly, so the SQL text depends
    only on the elements (#567).

    A ``dict`` is rejected: iterating it yields only its keys, so its values
    would be silently dropped. :class:`Sequence` additionally rejects a
    ``set``/``frozenset``, since its iteration order is not guaranteed and
    would make an ordered collection's element order nondeterministic.
    """

    __slots__ = ("_elements",)
    _elements: tuple[Any, ...]
    # Overridden by Sequence: order matters there, so a set/frozenset (whose
    # iteration order is not guaranteed) is rejected rather than silently
    # frozen into one arbitrary order.
    _rejects_unordered = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        if cls.__bases__ != (_Collection,):
            raise TypeError(f"{cls.__mro__[1].__name__} cannot be subclassed")
        super().__init_subclass__(**kwargs)

    def __new__(cls, elements: Iterable[Any] = ()) -> _Collection:
        # Construction happens here, not in __init__: __setattr__ is
        # overridden to keep instances immutable, and __init__ runs again
        # whenever someone calls instance.__init__(...) directly, which must
        # not be able to mutate an existing instance (#568 review).
        if isinstance(elements, (str, bytes, bytearray)):
            raise TypeError(
                f"{cls.__name__}() takes an iterable of elements, not a single "
                f"{type(elements).__name__}; wrap it in a list"
            )
        if isinstance(elements, dict):
            raise TypeError(
                f"{cls.__name__}() does not accept a dict; iterating it would silently "
                f"use only its keys and drop the values, pass the keys (list(d)) or the "
                f"values (list(d.values())) explicitly"
            )
        if cls._rejects_unordered and isinstance(elements, (set, frozenset)):
            raise TypeError(
                f"{cls.__name__}() does not accept a set/frozenset; their iteration "
                f"order is not guaranteed, which would make this ordered collection's "
                f"element order nondeterministic, pass a list or tuple instead"
            )
        self = object.__new__(cls)
        object.__setattr__(self, "_elements", tuple(elements))
        return self

    def __init__(self, elements: Iterable[Any] = ()) -> None:
        # No-op: all construction happens in __new__ above. Keeping this
        # method a no-op means re-invoking __init__ on an already-built
        # instance (``obj.__init__(other_elements)``) cannot mutate it.
        pass

    @property
    def elements(self) -> tuple[Any, ...]:
        """The elements, in the order given."""
        return self._elements

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __iter__(self) -> Iterator[Any]:
        return iter(self._elements)

    def __len__(self) -> int:
        return len(self._elements)

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, _Collection)
        return self._elements == other._elements

    def __hash__(self) -> int:
        return hash((type(self).__name__, self._elements))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({list(self._elements)!r})"

    # A shallow copy shares the same element references either way, so this
    # instance already behaves as its own shallow copy.
    def __copy__(self) -> _Collection:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> _Collection:
        # Most accepted element types (None, bool, int, float, Decimal, str,
        # bytes, date, time, datetime) are themselves immutable, so
        # copy.deepcopy() of the elements tuple hands back that same tuple
        # object and this is a no-op. A bytearray element is mutable, though
        # (#568 review): deep-copying it independently keeps deepcopy's
        # contract that mutating the copy must not affect the original.
        elements = copy.deepcopy(self._elements, memo)
        if elements is self._elements:
            return self
        new = type(self)(elements)
        memo[id(self)] = new
        return new

    def __reduce__(self) -> tuple[type[_Collection], tuple[tuple[Any, ...]]]:
        # Reconstructs through __new__ via the public constructor call, the
        # same path a fresh Set(...)/Multiset(...)/Sequence(...) call takes;
        # pickle's default slot-restoring __setstate__ would otherwise call
        # setattr() on the restored instance and hit __setattr__ above.
        return (type(self), (self._elements,))


class Set(_Collection):
    """A CUBRID ``SET`` parameter, rendered as ``SET{...}``.

    The server removes duplicate elements and does not keep their order.
    """

    __slots__ = ()


class Multiset(_Collection):
    """A CUBRID ``MULTISET`` parameter, rendered as ``MULTISET{...}``.

    The server keeps duplicate elements but not their order.
    """

    __slots__ = ()


class Sequence(_Collection):
    """A CUBRID ``SEQUENCE`` (``LIST``) parameter, rendered as ``SEQUENCE{...}``.

    The server keeps duplicate elements and their order.
    """

    __slots__ = ()
    _rejects_unordered = True
