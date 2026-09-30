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

from .exceptions import DataError, InterfaceError, ProgrammingError
from .error_codes import CAS_ERROR_TO_EXCEPTION, _DEFAULT_SQLSTATE, get_sqlstate
from .types import Multiset, Set, _Collection
from .types import Sequence as SequenceParam

# The C implementation of Decimal, or None when only _pydecimal is available.
_CDecimal: type[Decimal] | None
try:
    import _decimal

    _CDecimal = _decimal.Decimal
except ImportError:  # pragma: no cover - CPython builds without the C module
    _CDecimal = None

if TYPE_CHECKING:
    from .protocol import ColumnMetaData

# ---- constants -------------------------------------------------------------

DescriptionItem = tuple[str, int, None, None, int, int, bool]

# DML verbs eligible for batch execution in executemany().
DML_BATCH_VERBS = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE"})

# Regex to strip leading SQL comments: block /* ... */, ANSI line -- ... , and
# CUBRID C++-style line // ... (each to EOL/EOF).  Kept consistent with the
# comment styles skipped by split_on_placeholders().
_RE_LEADING_COMMENTS = re.compile(r"^(\s*(/\*.*?\*/|--[^\n]*(\n|$)|//[^\n]*(\n|$)))*\s*", re.DOTALL)

# IANA-style time zone name accepted from ``tzinfo.key`` in DATETIMETZ literals.
_RE_TZ_KEY = re.compile(r"[A-Za-z0-9_+/-]+")

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


def _format_tz(value: datetime.datetime, tzinfo: datetime.tzinfo) -> str | None:
    """Return the DATETIMETZ zone for an aware *value*, or ``None`` if naive."""
    # The unbound call bypasses a subclass utcoffset(); the C implementation
    # guarantees the tzinfo returns None or a timedelta, whose fields are read
    # through the base-class descriptors.
    offset = datetime.datetime.utcoffset(value)
    if offset is None:
        return None
    tz_key = getattr(tzinfo, "key", None)
    if tz_key is not None and not (type(tz_key) is str and tz_key == ""):
        if type(tz_key) is not str or not _RE_TZ_KEY.fullmatch(tz_key):
            raise ProgrammingError("time zone key must be an IANA name matching [A-Za-z0-9_+/-]+")
        return tz_key
    total_us = (_TD_DAYS(offset) * 86400 + _TD_SECONDS(offset)) * 1000000 + _TD_MICROSECONDS(offset)
    # Truncate toward zero, as int(offset.total_seconds()) did.
    total_seconds = abs(total_us) // 1000000
    sign = "-" if total_us < 0 and total_seconds else "+"
    hours, remainder = divmod(total_seconds, 3600)
    return "%s%02d:%02d" % (sign, hours, remainder // 60)


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
        return format(value, "f")
    if issubclass(cls, int):
        return int.__repr__(value)
    if issubclass(cls, float):
        if math.isnan(value) or math.isinf(value):
            raise ProgrammingError("nan and inf are not supported by CUBRID")
        return float.__repr__(value)
    # Typed collections (#567). The classes cannot be subclassed; they are
    # matched by identity and each element goes through this same renderer.
    if cls is Set or cls is Multiset or cls is SequenceParam:
        elements = _COLLECTION_ELEMENTS(value)
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
    def _escape_string(value: str, *, no_backslash_escapes: bool = True) -> str:
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
    sqlstate = get_sqlstate(code) or _DEFAULT_SQLSTATE.get(exc_name, "HY000")
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
    raise exc_cls(message, code=code, errno=code, sqlstate=sqlstate)
