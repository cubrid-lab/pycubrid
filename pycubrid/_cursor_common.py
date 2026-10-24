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
import sys
import warnings
from decimal import Decimal
from importlib import import_module
from typing import Any, Generic, Iterable, Protocol, Sequence, TypeVar

from .exceptions import (
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from .error_codes import CAS_ERROR_TO_EXCEPTION, _DEFAULT_SQLSTATE, get_sqlstate
from .types import Multiset, Set, _Collection
from .types import Sequence as SequenceParam

_NONPOSITIVE_FETCH_SIZE_MESSAGE = (
    "fetchmany() with a size of 0 or less returns [] without fetching rows; "
    "this is deprecated and will raise ProgrammingError in pycubrid 2.0. "
    "Skip the call instead, for example: "
)
_SYNC_FETCH_SIZE_REPLACEMENT = "rows = cursor.fetchmany(n) if n > 0 else []"
_ASYNC_FETCH_SIZE_REPLACEMENT = "rows = await cursor.fetchmany(n) if n > 0 else []"


def warn_nonpositive_fetch_size(*, asynchronous: bool = False) -> None:
    """Emit the 1.x deprecation warning for ``fetchmany(size)`` with ``size <= 0``.

    The 1.x result (``[]``, no row consumed, no FETCH) is unchanged; pycubrid
    2.0 will raise :class:`ProgrammingError` instead (#678). The plain
    sync ``fetchmany()`` and the async coroutine alike are called (or awaited)
    directly from the caller's frame, so level 3 (this helper, ``fetchmany()``,
    then its caller) names the caller's own line in both cases. The suggested
    replacement uses ``await`` for the async cursor.
    """
    replacement = _ASYNC_FETCH_SIZE_REPLACEMENT if asynchronous else _SYNC_FETCH_SIZE_REPLACEMENT
    warnings.warn(
        _NONPOSITIVE_FETCH_SIZE_MESSAGE + replacement,
        DeprecationWarning,
        stacklevel=3,
    )


# The C implementation of Decimal, or None when only _pydecimal is available.
_CDecimal: type[Decimal] | None
try:
    import _decimal

    _CDecimal = _decimal.Decimal
except ImportError:  # pragma: no cover - CPython builds without the C module
    _CDecimal = None

# The active datetime classes may instead come from the pure-Python fallback.
_CDateTime: type[datetime.datetime] | None
try:
    _CDateTime = import_module("_datetime").datetime
except ImportError:  # pragma: no cover - exercised in a fresh subprocess
    _CDateTime = None


# ---- constants -------------------------------------------------------------

DescriptionItem = tuple[str, int, None, None, int, int, bool]

# DML verbs eligible for batch execution in executemany().
DML_BATCH_VERBS = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE"})

# Regex to strip leading SQL comments: block /* ... */, ANSI line -- ... , and
# CUBRID C++-style line // ... (each to EOL/EOF).  Kept consistent with the
# comment styles skipped by split_on_placeholders().
_RE_LEADING_COMMENTS = re.compile(r"^(\s*(/\*.*?\*/|--[^\n]*(\n|$)|//[^\n]*(\n|$)))*\s*", re.DOTALL)
# A comment that starts inside the first word ends it: INSERT/*x*/INTO (#898).
_RE_COMMENT_START = re.compile(r"/\*|--|//")

# IANA-style time zone name accepted from ``tzinfo.key`` in DATETIMETZ literals.
_RE_TZ_KEY = re.compile(r"[A-Za-z0-9_+/-]+")
# Alphabetic zone abbreviation (``EST``) from ``tzinfo.tzname()``, appended to
# the region of a repeated wall time (#819). Numeric ones (``-04``) are not used.
_RE_TZ_ABBREV = re.compile(r"[A-Za-z]{3,6}")

# Field readers taken from the base classes. A subclass can shadow ``year``,
# ``strftime()`` and friends, but not the base-class descriptors, so literals
# built from these reflect the stored value (#528).
_DATE_YEAR = datetime.date.year.__get__
_DATE_MONTH = datetime.date.month.__get__
_DATE_DAY = datetime.date.day.__get__
_DT_HOUR = datetime.datetime.hour.__get__
_DT_MINUTE = datetime.datetime.minute.__get__
_DT_SECOND = datetime.datetime.second.__get__
_DT_MICROSECOND = datetime.datetime.microsecond.__get__
_DT_TZINFO = datetime.datetime.tzinfo.__get__
_DT_FOLD = datetime.datetime.fold.__get__
_TIME_HOUR = datetime.time.hour.__get__
_TIME_MINUTE = datetime.time.minute.__get__
_TIME_SECOND = datetime.time.second.__get__
_TD_DAYS = datetime.timedelta.days.__get__
_TD_SECONDS = datetime.timedelta.seconds.__get__
_TD_MICROSECONDS = datetime.timedelta.microseconds.__get__
# The slot reader of the typed collection parameters (#567): reads the stored
# tuple without going through an attribute lookup on the instance.
_COLLECTION_ELEMENTS = _Collection.__dict__["_elements"].__get__


# ---- SQL parsing -----------------------------------------------------------


def extract_first_keyword(sql: str) -> str:
    """Extract the first SQL keyword, skipping leading comments and whitespace."""
    stripped = _RE_LEADING_COMMENTS.sub("", sql)
    if not stripped:
        return ""
    word = stripped.split(None, 1)[0]
    comment = _RE_COMMENT_START.search(word)
    if comment is not None:
        word = word[: comment.start()]
    return word.upper()


def check_prepared_batch_operation(operation: object) -> str:
    """Return the SQL of ``executemany(prepared=True)`` after its checks (#839).

    Only INSERT, UPDATE, DELETE and MERGE are sent as ``PREPARE`` +
    ``EXECUTE_ARRAY``; every other statement raises ``NotSupportedError``
    instead of falling back to the literal path. A ``str`` subclass is read
    through ``str.__str__()``, so its overrides cannot change the SQL.
    """
    if not isinstance(operation, str):
        raise ProgrammingError("prepared SQL must be a string without NUL")
    operation = str.__str__(operation)
    if "\x00" in operation:
        raise ProgrammingError("prepared SQL must be a string without NUL")
    if extract_first_keyword(operation) not in DML_BATCH_VERBS:
        raise NotSupportedError(
            "executemany(prepared=True) supports only INSERT, UPDATE, DELETE and MERGE"
        )
    return operation


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


def escape_string(value: str, *, no_backslash_escapes: bool = True) -> str:
    """Escape a string value for safe inclusion in a SQL literal.

    Raises :class:`ProgrammingError` if the string contains a null byte
    (``\\x00``) or a Ctrl-Z byte (``\\x1a``). CUBRID does not support the
    null byte in string parameters, and CUBRID's SQL grammar defines no safe
    literal escape for ``\\x1a`` (there is no MySQL-style ``\\Z``), so it is
    rejected rather than emitted as a raw control byte.

    ``no_backslash_escapes`` defaults to ``True`` to match CUBRID's server
    default (``no_backslash_escapes=yes``, i.e. a backslash is an ordinary
    literal character) and to stay consistent with the other public escaping
    helpers in this module (``format_parameter``, ``bind_parameters``,
    ``split_on_placeholders``). Callers that know the server runs with
    backslash-escape processing on should pass ``no_backslash_escapes=False``;
    internal driver paths always pass the connection's negotiated value
    explicitly.
    """
    if not issubclass(type(value), str):
        raise ProgrammingError("escape_string() requires a str")
    # Copy to a plain str through the base-class slot: a subclass can override
    # replace(), __contains__(), __str__() and the rest, and those overrides
    # would otherwise decide what reaches the SQL text (#528).
    value = str.__str__(value)
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


def _offset_us(offset: datetime.timedelta | None) -> int:
    """Return *offset* in microseconds, read through the base-class fields."""
    if offset is None:
        raise TypeError("utcoffset() returned None")
    if datetime.datetime is not _CDateTime and type(offset) is not datetime.timedelta:
        raise TypeError("timedelta subclasses require the C datetime module")
    total_us: int = (_TD_DAYS(offset) * 86400 + _TD_SECONDS(offset)) * 1000000 + _TD_MICROSECONDS(
        offset
    )
    return total_us


def _format_tz(value: datetime.datetime, tzinfo: datetime.tzinfo) -> str | None:
    """Return the DATETIMETZ zone for an aware *value*, or ``None`` if naive."""
    # The unbound call bypasses a datetime subclass override, but tzinfo
    # callbacks and key lookup remain caller-controlled.
    try:
        offset = datetime.datetime.utcoffset(value)
        if offset is None:
            return None
        if datetime.datetime is not _CDateTime and type(offset) is not datetime.timedelta:
            raise TypeError("timedelta subclasses require the C datetime module")
        tz_key = getattr(tzinfo, "key", None)
    except Exception as exc:
        raise ProgrammingError("invalid tzinfo on datetime parameter") from exc
    if tz_key is not None and not (type(tz_key) is str and tz_key == ""):
        if type(tz_key) is not str or not _RE_TZ_KEY.fullmatch(tz_key):
            raise ProgrammingError("time zone key must be an IANA name matching [A-Za-z0-9_+/-]+")
        # A region alone makes CUBRID take the first occurrence of a repeated
        # wall time, so the second one (fold=1) would be stored an hour off
        # (#819). The region alone already stores fold=0 correctly and keeps
        # the region, so only a repeated wall time (the fold=0 offset larger
        # than the fold=1 one) with fold=1 names its occurrence: by its
        # abbreviation when it is alphabetic and differs from the first one's,
        # else by the offset alone. A skipped wall time keeps the region, which
        # CUBRID rejects.
        try:
            fields = (
                _DATE_YEAR(value),
                _DATE_MONTH(value),
                _DATE_DAY(value),
                _DT_HOUR(value),
                _DT_MINUTE(value),
                _DT_SECOND(value),
                _DT_MICROSECOND(value),
            )
            first = datetime.datetime(*fields, tzinfo=tzinfo, fold=0)
            second = datetime.datetime(*fields, tzinfo=tzinfo, fold=1)
            first_us = _offset_us(datetime.datetime.utcoffset(first))
            second_us = _offset_us(datetime.datetime.utcoffset(second))
            if first_us <= second_us or not _DT_FOLD(value):
                return tz_key
            other = datetime.datetime.tzname(first)
            name = datetime.datetime.tzname(second)
        except Exception as exc:
            raise ProgrammingError("invalid tzinfo on datetime parameter") from exc
        if (
            type(name) is str
            and type(other) is str
            and name != other
            and _RE_TZ_ABBREV.fullmatch(name)
        ):
            return "%s %s" % (tz_key, name)
        total_us = second_us
    else:
        try:
            total_us = _offset_us(offset)
        except Exception as exc:
            raise ProgrammingError("invalid tzinfo on datetime parameter") from exc
    # CUBRID zone offsets are +HH:MM; rendering a finer offset would silently
    # store a different instant (#898).
    if total_us % 60000000:
        raise DataError("UTC offset of a datetime parameter must be a whole number of minutes")
    total_seconds = abs(total_us) // 1000000
    sign = "-" if total_us < 0 and total_seconds else "+"
    hours, remainder = divmod(total_seconds, 3600)
    return "%s%02d:%02d" % (sign, hours, remainder // 60)


def _check_temporal_subclass(cls: type) -> None:
    """Refuse date/time subclasses when only the pure-Python datetime is active."""
    # Pure-Python descriptors read attributes such as _year, which a subclass
    # can forge. Refuse those subclasses before any field read or callback.
    if datetime.datetime is not _CDateTime and issubclass(cls, (datetime.date, datetime.time)):
        if cls is not datetime.datetime and cls is not datetime.date and cls is not datetime.time:
            raise ProgrammingError(
                "date/time subclass parameters require the C datetime module; "
                "pass a plain date, datetime or time"
            )


def _checked_decimal(cls: type, value: Decimal) -> Decimal:
    """Return *value* as a plain finite ``Decimal`` of at most 38 digits.

    Shared by the literal renderer and the prepared NUMERIC encoder (#840).
    """
    # With CPython's C decimal module, Decimal(subclass) copies the
    # internal value without calling any overridable method, so the checks
    # below see the real value. The pure-Python fallback (_pydecimal)
    # copies _sign/_int/_exp through ordinary attribute reads, which a
    # subclass can forge, so subclasses are refused there.
    if cls is not Decimal and Decimal is not _CDecimal:
        raise ProgrammingError(
            "Decimal subclass parameters require the C decimal module; pass a plain Decimal"
        )
    value = Decimal(value)
    if value.is_nan() or value.is_infinite():
        raise ProgrammingError("nan and inf are not supported by CUBRID")
    # str() switches to E notation (1E-7), which CUBRID parses as DOUBLE.
    # Render plain fixed-point digits instead; the server rejects a plain
    # numeric literal with more than 38 digits (leading fractional zeros
    # count), so check that before expanding a possibly huge exponent.
    _, digits, exponent = value.as_tuple()
    assert isinstance(exponent, int)
    if exponent >= 0:
        precision = len(digits) + exponent if value else 1
    else:
        precision = max(len(digits), -exponent)
    if precision > 38:
        raise DataError(
            "Decimal parameter needs %d digits; CUBRID NUMERIC literals allow "
            "at most 38 digits" % precision
        )
    return value


# Scaling by 2**600 moves every subnormal double into the normal range, and
# 2**-600 is itself normal, so the server computes (m * 2**-600) exactly (#988).
_SUBNORMAL_SHIFT = 600
_SUBNORMAL_UNSCALE = float.__repr__(math.ldexp(1.0, -_SUBNORMAL_SHIFT))


def _float_literal(value: float) -> str:
    """Render a finite float so that CUBRID stores the same double.

    A plain literal such as ``1.5`` is NUMERIC in CUBRID, which drops the sign
    of ``-0.0``; ``-0.0e0`` is a DOUBLE literal that keeps it. CUBRID rejects
    any subnormal literal ("exceeds limit of double", -493), so a subnormal is
    sent as an exact product of two normal doubles. Every other value keeps
    its shortest round-trip form, which NUMERIC and string targets rely on.
    ``math`` reads the stored double, so a subclass cannot alter the result.
    """
    magnitude = math.fabs(value)
    if magnitude == 0.0:
        return "-0.0e0" if math.copysign(1.0, value) < 0 else "0.0"
    if magnitude < sys.float_info.min:
        scaled = math.ldexp(math.copysign(magnitude, value), _SUBNORMAL_SHIFT)
        return f"({float.__repr__(scaled)} * {_SUBNORMAL_UNSCALE})"
    return float.__repr__(value)


def format_parameter(value: Any, *, no_backslash_escapes: bool = True) -> str:
    """Format a single Python value as a CUBRID SQL literal string."""
    if value is None:
        return "NULL"
    # Dispatch on type(value), not isinstance(): isinstance() also trusts an
    # overridden __class__, and such an object would then reach the base-class
    # renderers below with the wrong layout. It is rejected as unsupported.
    cls = type(value)
    if cls is bool:
        return "1" if value else "0"
    if issubclass(cls, str):
        return escape_string(value, no_backslash_escapes=no_backslash_escapes)
    # bytes.hex()/bytearray.hex() unbound read the buffer directly, so an
    # overridden hex() or __bytes__() on a subclass is never called (#528).
    if issubclass(cls, bytes):
        return "X'%s'" % bytes.hex(value)
    if issubclass(cls, bytearray):
        return "X'%s'" % bytearray.hex(value)
    _check_temporal_subclass(cls)
    # Dates and times are built from their integer fields, read through the
    # base-class descriptors, instead of strftime(): a subclass can override
    # strftime() or the field properties (#528), and %Y does not zero-pad
    # years below 1000, which CUBRID then misreads ('99-01-02' is 1999) (#519).
    if issubclass(cls, datetime.datetime):
        literal = "%04d-%02d-%02d %02d:%02d:%02d.%03d" % (
            _DATE_YEAR(value),
            _DATE_MONTH(value),
            _DATE_DAY(value),
            _DT_HOUR(value),
            _DT_MINUTE(value),
            _DT_SECOND(value),
            _DT_MICROSECOND(value) // 1000,
        )
        tzinfo = _DT_TZINFO(value)
        tz_str = None if tzinfo is None else _format_tz(value, tzinfo)
        if tz_str is not None:
            return "DATETIMETZ'%s %s'" % (literal, tz_str)
        return "DATETIME'%s'" % literal
    if issubclass(cls, datetime.date):
        return "DATE'%04d-%02d-%02d'" % (_DATE_YEAR(value), _DATE_MONTH(value), _DATE_DAY(value))
    if issubclass(cls, datetime.time):
        return "TIME'%02d:%02d:%02d'" % (
            _TIME_HOUR(value),
            _TIME_MINUTE(value),
            _TIME_SECOND(value),
        )
    # Numeric values are rendered through the base-class methods, never
    # str()/format() on the value itself: a subclass (IntEnum, IntFlag or any
    # user type) can override __str__/__repr__/__format__ and would otherwise
    # put arbitrary text into the SQL (#518).
    if issubclass(cls, Decimal):
        value = _checked_decimal(cls, value)
        return format(value, "f")
    if issubclass(cls, int):
        return int.__repr__(value)
    if issubclass(cls, float):
        if math.isnan(value) or math.isinf(value):
            raise ProgrammingError("nan and inf are not supported by CUBRID")
        return _float_literal(value)
    # Typed collections (#567). The classes cannot be subclassed; they are
    # matched by identity and each element goes through this same renderer.
    if cls is Set or cls is Multiset or cls is SequenceParam:
        try:
            elements = _COLLECTION_ELEMENTS(value)
        except AttributeError:
            # object.__new__(Set) (or an unpickled/forged instance) never ran
            # _Collection.__new__, so the _elements slot was never set (#568
            # review). Surface this as the same exception family as every
            # other binding failure instead of leaking the raw AttributeError.
            raise ProgrammingError(
                f"{cls.__name__} parameter is missing its elements; it was not "
                f"constructed through {cls.__name__}(...)"
            ) from None
        if type(elements) is not tuple:
            raise ProgrammingError("collection parameter elements must be a tuple")
        rendered = []
        for element in elements:
            if issubclass(type(element), _Collection):
                raise ProgrammingError("nested collection parameters are not supported")
            rendered.append(format_parameter(element, no_backslash_escapes=no_backslash_escapes))
        keyword = "SET" if cls is Set else "MULTISET" if cls is Multiset else "SEQUENCE"
        return "%s{%s}" % (keyword, ", ".join(rendered))
    if issubclass(cls, (list, tuple, set, frozenset, dict)):
        raise ProgrammingError(
            "cannot bind a collection (list/tuple/set/frozenset/dict) as a "
            "single parameter; pycubrid does not auto-expand IN (?, ?, ...) — "
            "expand the placeholders explicitly in the SQL, or wrap the elements "
            "in pycubrid.types.Set, Multiset or Sequence to bind a CUBRID collection"
        )
    raise ProgrammingError("unsupported parameter type")


def bind_parameters(
    operation: str,
    parameters: Sequence[Any],
    *,
    no_backslash_escapes: bool = True,
) -> str:
    """Bind *parameters* into *operation* by replacing ``?`` placeholders.

    Returns the fully-rendered SQL string ready for execution.
    """
    values = _parameter_values(parameters)
    parts = split_on_placeholders(operation, no_backslash_escapes=no_backslash_escapes)
    return _render_parameters(parts, values, no_backslash_escapes)


def _parameter_values(parameters: Sequence[Any]) -> Sequence[Any]:
    """Return a snapshot of one parameter set, or raise if it is not a sequence.

    An exact ``tuple`` is immutable and an exact ``list`` is copied, so both
    skip the ``typing.Sequence`` check (#837); every other type, subclasses
    included, takes the full check.
    """
    cls = type(parameters)
    if cls is tuple:
        return parameters
    if cls is list:
        return list(parameters)
    # typing.Sequence checks type(obj), not __class__; collections.abc.Sequence
    # would accept an object that only claims a sequence type through __class__.
    if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
        return list(parameters)
    raise ProgrammingError("parameters must be a sequence")


def _render_parameters(parts: list[str], values: Sequence[Any], no_backslash_escapes: bool) -> str:
    """Join the placeholder-split *parts* with the rendered *values*."""
    if len(parts) - 1 != len(values):
        raise ProgrammingError("wrong number of parameters")
    result = [parts[0]]
    for index, value in enumerate(values, start=1):
        literal = format_parameter(value, no_backslash_escapes=no_backslash_escapes)
        # A negative number placed right after a "-" in the SQL would form
        # "--", a line comment that drops the rest of the statement (#893).
        # A space keeps the two minus signs separate tokens. result[-1] is
        # always the SQL text just before this placeholder, and no rendered
        # value ends with "-", so an empty part needs no check.
        if literal[:1] == "-" and result[-1][-1:] == "-":
            result.append(" ")
        result.append(literal)
        result.append(parts[index])
    return "".join(result)


# ---- result description ----------------------------------------------------


class _ColumnMetadata(Protocol):
    """The ``protocol.ColumnMetaData`` fields a description reads.

    A structural type keeps this module free of a ``protocol`` import, which
    imports this module for the shared value checks (#840).
    """

    name: str
    column_type: int
    precision: int
    scale: int
    is_nullable: bool


def build_description(
    columns: Sequence[_ColumnMetadata],
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
    _description: tuple[DescriptionItem, ...] | None
    _columns: list[Any]
    _rows: list[tuple[Any, ...]]
    _row_index: int
    _fetched_count: int
    _page_error: DataError | None
    _total_tuple_count: int
    _query_handle: int | None
    _rowcount: int
    _lastrowid: int | None
    _batch_row_results: tuple[tuple[int, int, str], ...] | None

    def _clear_result_for_batch(self) -> None:
        """Forget the previous result before a batch request is sent (#375, #839).

        A failed transport, parse or server error then leaves no stale rows,
        ``description=None``, ``rowcount=-1`` and ``lastrowid=None``.
        """
        self._description = None
        self._columns = []
        self._rows = []
        self._row_index = 0
        self._fetched_count = 0
        self._page_error = None
        self._total_tuple_count = 0
        self._query_handle = None
        self._rowcount = -1
        self._lastrowid = None
        self._batch_row_results = None

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

    def _bind_parameter_sets(
        self,
        operation: str,
        seq_of_parameters: Iterable[Sequence[Any]],
    ) -> list[str]:
        """Render *operation* once per parameter set, splitting it only once (#837).

        The result is byte-identical to ``_bind_parameters`` per set: on the
        first set the escape mode is resolved, the set is checked and the SQL
        is split, in that order; every set is then checked and rendered in
        turn, so the first bad set raises what it raised before. The escape
        mode is read once per call, before the first set is checked, instead
        of once per set; the two differ only if iterating the sets reconnects
        the connection, and the send-time generation check rejects that SQL.
        """
        rendered: list[str] = []
        parts: list[str] | None = None
        mode: bool
        for parameters in seq_of_parameters:
            if parts is None:
                mode = self._resolve_escape_mode()
                values = _parameter_values(parameters)
                parts = split_on_placeholders(operation, no_backslash_escapes=mode)
            else:
                values = _parameter_values(parameters)
            rendered.append(_render_parameters(parts, values, mode))
        return rendered

    def _format_parameter(self, value: Any) -> str:
        return format_parameter(value, no_backslash_escapes=self._resolve_escape_mode())

    @staticmethod
    def _escape_string(value: str, *, no_backslash_escapes: bool = True) -> str:
        return escape_string(value, no_backslash_escapes=no_backslash_escapes)

    def _build_description(
        self,
        columns: Sequence[_ColumnMetadata],
    ) -> tuple[DescriptionItem, ...] | None:
        return build_description(columns)


def _batch_exception(err: dict[str, Any]) -> DatabaseError:
    """Return the PEP 249 exception for one failed batch statement.

    Uses CAS error code dispatch (same mapping as protocol._raise_error)
    to select the correct exception class. Falls back to DatabaseError
    for unknown codes.
    """
    code = err.get("code", -1)
    message = err.get("message", "batch execute statement failed")
    exc_name = CAS_ERROR_TO_EXCEPTION.get(code, "DatabaseError")
    sqlstate = get_sqlstate(code) or _DEFAULT_SQLSTATE.get(exc_name, "HY000")
    exc_map = {
        "DataError": DataError,
        "IntegrityError": IntegrityError,
        "InternalError": InternalError,
        "OperationalError": OperationalError,
        "ProgrammingError": ProgrammingError,
        "DatabaseError": DatabaseError,
    }
    exc_cls = exc_map.get(exc_name, DatabaseError)
    return exc_cls(message, code=code, errno=code, sqlstate=sqlstate)


def _raise_batch_error(err: dict[str, Any]) -> None:
    """Raise the appropriate PEP 249 exception for a batch statement failure."""
    raise _batch_exception(err)


def _batch_failure(
    row_results: Sequence[tuple[int, int, str]],
    errors: Sequence[dict[str, Any]],
    raised: dict[str, Any],
    protocol_version: int | None = None,
) -> DatabaseError:
    """Return the exception for *raised* with the batch's per-row outcome (#841).

    *row_results* holds one ``(count, error code, message)`` per executed
    row, in order; *errors* holds the failed rows as ``{"code", "message",
    "index"}``. The exception is the one ``_batch_exception`` builds;
    ``batch_errors`` holds a separate instance per failed row. The caller
    raises it, so no frame of this helper is left holding it.

    From protocol 3 the reply puts ``DBMS_ERROR_INDICATOR`` before each row's
    code, so with *protocol_version* above 2 every exception has
    ``error_source == "server"``; otherwise it is ``None`` (#903).
    """
    source = "server" if type(protocol_version) is int and protocol_version > 2 else None
    exc = _batch_exception(raised)
    exc.batch_index = raised["index"]
    exc._error_source = source
    exc.batch_errors = tuple(_batch_entry(err, source) for err in errors)
    exc.batch_rowcounts = tuple(count for count, _code, _message in row_results)
    return exc


def _batch_entry(err: dict[str, Any], source: str | None = None) -> tuple[int, DatabaseError]:
    """Return ``(index, exception)`` for one failed batch row (#841)."""
    entry = _batch_exception(err)
    entry.batch_index = err["index"]
    entry._error_source = source
    return err["index"], entry
