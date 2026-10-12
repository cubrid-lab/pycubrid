"""Prepared host-variable bindings for the CAS FC3 and FC21 requests.

Validated binding types and the encoders that turn Python values into typed
bind arguments, with no SQL rendering and no I/O. ``pycubrid.protocol``
re-exports every name defined here, and its packet classes write the
bindings with :func:`_write_prepared_binding` (#856).
"""

from __future__ import annotations

import datetime
import math
import re
import struct
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Sequence

from ._cursor_common import (
    _COLLECTION_ELEMENTS,
    _DATE_DAY,
    _DATE_MONTH,
    _DATE_YEAR,
    _DT_HOUR,
    _DT_MICROSECOND,
    _DT_MINUTE,
    _DT_SECOND,
    _DT_TZINFO,
    _TIME_HOUR,
    _TIME_MINUTE,
    _TIME_SECOND,
    _check_temporal_subclass,
    _checked_decimal,
    _format_tz,
)
from .constants import CUBRIDDataType
from .exceptions import DataError, InterfaceError, NotSupportedError, ProgrammingError
from .packet import PacketWriter, _codec_label, _decode_text, _encode_text
from .types import Multiset, Set, _Collection
from .types import Sequence as SequenceParam


# Every DATE/TIME/DATETIME bind uses the seven-short DATETIME layout: the
# broker reads TIME from offset 6 and does not check the length (#840).
_TEMPORAL = struct.Struct(">7h")
_TEMPORAL_SIZE = _TEMPORAL.size
_TEMPORAL_TYPE_CODES = frozenset(
    {CUBRIDDataType.DATE, CUBRIDDataType.TIME, CUBRIDDataType.DATETIME}
)
_NUMERIC_LIMIT = 10**38
_DIGITS = frozenset(b"0123456789")
_NUMERIC_PAYLOAD = re.compile(rb"-?(?=\.?[0-9])[0-9]*(?:\.[0-9]*)?\x00")
# The CAS limit is 63 bytes (CCI_TZ_SIZE); a region with "fold" abbreviation
# or a +hh:mm offset, never a NUL.
_TIME_ZONE_TEXT = re.compile(rb"[A-Za-z0-9_+/: -]{1,63}")


@dataclass(frozen=True, slots=True)
class _PreparedScalar:
    """One validated type/value pair for a prepared FC3 request."""

    type_code: int
    payload: bytes
    encoding: str = "utf-8"

    def __post_init__(self) -> None:
        if isinstance(self.type_code, bool) or not isinstance(self.type_code, int):
            raise ProgrammingError("invalid prepared parameter type code")
        if not isinstance(self.payload, bytes):
            raise ProgrammingError("invalid prepared parameter payload")
        if self.type_code == CUBRIDDataType.NULL:
            if self.payload:
                raise ProgrammingError("SQL NULL must have an empty payload")
        elif self.type_code == CUBRIDDataType.INT:
            if len(self.payload) != 4:
                raise ProgrammingError("prepared INT must have four value bytes")
        elif self.type_code == CUBRIDDataType.CHAR:
            if not self.payload.endswith(b"\x00") or b"\x00" in self.payload[:-1]:
                raise ProgrammingError("prepared CHAR must have one terminal NUL")
            try:
                self.payload[:-1].decode(self.encoding)
            except UnicodeDecodeError:
                raise DataError(
                    f"prepared CHAR is not valid {_codec_label(self.encoding)}"
                ) from None
        elif self.type_code == CUBRIDDataType.BIGINT:
            if len(self.payload) != 8:
                raise ProgrammingError("prepared BIGINT must have eight value bytes")
        elif self.type_code == CUBRIDDataType.DOUBLE:
            if len(self.payload) != 8:
                raise ProgrammingError("prepared DOUBLE must have eight value bytes")
            # The broker stores NaN silently; the literal path rejects it.
            if not math.isfinite(struct.unpack(">d", self.payload)[0]):
                raise ProgrammingError("nan and inf are not supported by CUBRID")
        elif self.type_code == CUBRIDDataType.NUMERIC:
            if not _NUMERIC_PAYLOAD.fullmatch(self.payload) or (
                sum(byte in _DIGITS for byte in self.payload) > 38
            ):
                raise ProgrammingError("prepared NUMERIC must be plain decimal text and one NUL")
        elif self.type_code in _TEMPORAL_TYPE_CODES:
            if len(self.payload) != _TEMPORAL_SIZE:
                raise ProgrammingError("prepared DATE/TIME/DATETIME must have fourteen bytes")
            if self.type_code == CUBRIDDataType.DATE and any(self.payload[6:]):
                raise ProgrammingError("prepared DATE must have zero time fields")
            if self.type_code == CUBRIDDataType.TIME and (
                any(self.payload[:6]) or any(self.payload[12:])
            ):
                raise ProgrammingError("prepared TIME must have zero date and millisecond fields")
        elif self.type_code == CUBRIDDataType.DATETIMETZ:
            if len(self.payload) <= _TEMPORAL_SIZE or not _TIME_ZONE_TEXT.fullmatch(
                self.payload[_TEMPORAL_SIZE:]
            ):
                raise ProgrammingError("prepared DATETIMETZ must end with a time zone name")
        elif self.type_code == CUBRIDDataType.VARBIT:
            if not self.payload:
                raise ProgrammingError("prepared VARBIT must not be empty")
        else:
            raise ProgrammingError("unsupported prepared parameter type code")


def _encode_prepared_scalar(value: Any, encoding: str = "utf-8") -> _PreparedScalar:
    """Encode one Python value as a typed prepared bind, without SQL rendering.

    The #439 subset (``None``, a 32-bit ``int`` and ``str``) keeps its exact
    bytes. #840 adds ``bool`` (INT 1/0, as the literal path), 64-bit ``int``
    (BIGINT), ``float`` (DOUBLE), ``Decimal`` (NUMERIC text), ``date``,
    ``time`` and ``datetime`` (DATE, TIME, DATETIME, or DATETIMETZ when
    aware) and ``bytes``/``bytearray`` (VARBIT). The checks are the literal
    path's: nan and inf, more than 38 Decimal digits and forged subclasses
    are rejected the same way. Strings use ``encoding``, the connection
    charset (#86). Messages never echo the value.
    """
    if value is None:
        return _PreparedScalar(CUBRIDDataType.NULL, b"")
    # Dispatch on type(value), as the literal path does: isinstance() also
    # trusts an overridden __class__. Values are read through the base-class
    # methods, so a subclass cannot change the bytes (#518, #528).
    cls = type(value)
    if cls is bool:
        # The literal path renders True/False as the INTEGER literals 1/0.
        return _PreparedScalar(CUBRIDDataType.INT, struct.pack(">i", 1 if value else 0))
    if issubclass(cls, int):
        number = int.__index__(value)
        if -(2**31) <= number < 2**31:
            return _PreparedScalar(CUBRIDDataType.INT, struct.pack(">i", number))
        if -(2**63) <= number < 2**63:
            return _PreparedScalar(CUBRIDDataType.BIGINT, struct.pack(">q", number))
        # Past BIGINT the literal is a NUMERIC; check before int.__repr__,
        # which has a digit limit.
        if not -_NUMERIC_LIMIT < number < _NUMERIC_LIMIT:
            raise DataError("prepared integer needs more than 38 digits")
        return _PreparedScalar(CUBRIDDataType.NUMERIC, int.__repr__(number).encode() + b"\x00")
    if issubclass(cls, str):
        # A plain copy: a subclass cannot override __contains__() or encode().
        value = str.__str__(value)
        if "\x00" in value:
            raise ProgrammingError("prepared string contains NUL")
        encoded, _position = _encode_text(value, encoding)
        if encoded is None:
            raise DataError(f"prepared string cannot be encoded as {_codec_label(encoding)}")
        return _PreparedScalar(CUBRIDDataType.CHAR, encoded + b"\x00", encoding)
    if issubclass(cls, float):
        if math.isnan(value) or math.isinf(value):
            raise ProgrammingError("nan and inf are not supported by CUBRID")
        return _PreparedScalar(CUBRIDDataType.DOUBLE, struct.pack(">d", value))
    if issubclass(cls, Decimal):
        text = format(_checked_decimal(cls, value), "f")
        # The broker counts every character but the sign and the point as
        # precision, so "0.5" would need one digit more than the literal.
        if text.startswith("0."):
            text = text[1:]
        elif text.startswith("-0."):
            text = "-" + text[2:]
        return _PreparedScalar(CUBRIDDataType.NUMERIC, text.encode("ascii") + b"\x00")
    if issubclass(cls, (bytes, bytearray)):
        # The unbound slice copies the stored bytes; no overridden method
        # (__bytes__, __buffer__) runs (#528).
        if issubclass(cls, bytes):
            data = bytes.__getitem__(value, slice(None))
        else:
            data = bytes(bytearray.__getitem__(value, slice(None)))
        if not data:
            # X'' is valid SQL, but the broker reads a zero-length bind as NULL.
            raise NotSupportedError("an empty bytes value cannot be sent as a prepared bind")
        return _PreparedScalar(CUBRIDDataType.VARBIT, data)
    _check_temporal_subclass(cls)
    if issubclass(cls, datetime.datetime):
        fields = _TEMPORAL.pack(
            _DATE_YEAR(value),
            _DATE_MONTH(value),
            _DATE_DAY(value),
            _DT_HOUR(value),
            _DT_MINUTE(value),
            _DT_SECOND(value),
            _DT_MICROSECOND(value) // 1000,
        )
        tzinfo = _DT_TZINFO(value)
        zone = None if tzinfo is None else _format_tz(value, tzinfo)
        if zone is None:
            return _PreparedScalar(CUBRIDDataType.DATETIME, fields)
        # The broker reads the zone by length; a terminal NUL breaks a region.
        return _PreparedScalar(CUBRIDDataType.DATETIMETZ, fields + zone.encode("ascii"))
    if issubclass(cls, datetime.date):
        return _PreparedScalar(
            CUBRIDDataType.DATE,
            _TEMPORAL.pack(_DATE_YEAR(value), _DATE_MONTH(value), _DATE_DAY(value), 0, 0, 0, 0),
        )
    if issubclass(cls, datetime.time):
        # The broker reads TIME from the DATETIME layout's time fields.
        return _PreparedScalar(
            CUBRIDDataType.TIME,
            _TEMPORAL.pack(0, 0, 0, _TIME_HOUR(value), _TIME_MINUTE(value), _TIME_SECOND(value), 0),
        )
    raise ProgrammingError("unsupported prepared parameter type")


_COLLECTION_TYPE_CODES = frozenset(
    {CUBRIDDataType.SET, CUBRIDDataType.MULTISET, CUBRIDDataType.SEQUENCE}
)
_COLLECTION_ELEMENT_TYPES = frozenset({CUBRIDDataType.INT, CUBRIDDataType.STRING})


@dataclass(frozen=True, slots=True)
class _PreparedCollection:
    """One validated typed collection for a prepared FC3 request (#482).

    The value argument is ``[element type byte]`` followed by one
    ``int32 length + payload`` per element, with no element count. A ``None``
    element is a NULL element (length 0). Whole SQL NULL is a
    ``_PreparedScalar``, never this type. The broker keeps a partial
    collection when an element length overruns the argument, so every
    element is checked here and the framing is always exact.
    """

    type_code: int
    element_type: int
    elements: tuple[bytes | None, ...]
    encoding: str = "utf-8"

    def __post_init__(self) -> None:
        if (
            isinstance(self.type_code, bool)
            or not isinstance(self.type_code, int)
            or self.type_code not in _COLLECTION_TYPE_CODES
        ):
            raise ProgrammingError("unsupported prepared collection type code")
        if (
            isinstance(self.element_type, bool)
            or not isinstance(self.element_type, int)
            or self.element_type not in _COLLECTION_ELEMENT_TYPES
        ):
            raise ProgrammingError("unsupported prepared collection element type")
        if type(self.elements) is not tuple:
            raise ProgrammingError("prepared collection elements must be a tuple")
        for element in self.elements:
            if element is None:
                continue
            if type(element) is not bytes:
                raise ProgrammingError("invalid prepared collection element payload")
            if self.element_type == CUBRIDDataType.INT:
                if len(element) != 4:
                    raise ProgrammingError("prepared INT element must have four value bytes")
            else:
                if not element.endswith(b"\x00") or b"\x00" in element[:-1]:
                    raise ProgrammingError("prepared string element must have one terminal NUL")
                try:
                    # Server-compatible decoding: rejects EUC-KR makeup
                    # sequences that _encode_text() never produces.
                    _decode_text(element[:-1], self.encoding)
                except UnicodeDecodeError:
                    raise DataError(
                        f"prepared string element is not valid {_codec_label(self.encoding)}"
                    ) from None

    @property
    def payload(self) -> bytes:
        """Return the exact FC3 value argument (without its own length prefix)."""
        parts = [bytes((self.element_type,))]
        for element in self.elements:
            if element is None:
                parts.append(b"\x00\x00\x00\x00")
            else:
                parts.append(struct.pack(">i", len(element)))
                parts.append(element)
        return b"".join(parts)


def _collection_int(value: int | str) -> int:
    """Return an INT element from an int or a canonical ASCII decimal string."""
    if isinstance(value, str):
        digits = value[1:] if value.startswith("-") else value
        if (
            not digits
            or not digits.isascii()
            or not digits.isdigit()
            or (digits[0] == "0" and value != "0")
        ):
            raise ProgrammingError("prepared INT element string is not a canonical integer")
        # Longer strings are out of range; this also stays below int()'s
        # digit limit.
        number = int(value) if len(digits) <= 10 else 2**31
    else:
        number = value
    if not -(2**31) <= number < 2**31:
        raise DataError("prepared INT element is outside signed 32-bit range")
    return number


def _encode_prepared_collection(
    values: Any, type_code: int, element_type: int, encoding: str = "utf-8"
) -> _PreparedCollection:
    """Encode a flat tuple as one typed SET/MULTISET/SEQUENCE bind (#482).

    INT elements accept ``int`` (not ``bool``), or canonical decimal strings
    as the official call shape ``('1', '2')`` sends; one collection must not
    mix the two. STRING elements accept ``str`` only and use ``encoding``, the
    connection charset. ``None`` is a NULL element and an empty tuple is an
    empty collection. Everything else, including nested containers, is
    rejected before any bytes are built; messages never echo the value.
    """
    if type(values) is not tuple:
        raise ProgrammingError("prepared collection must be a tuple")
    # Validate the codes before the elements so a bad code is not reported
    # as an element error.
    _PreparedCollection(type_code, element_type, ())
    encoded: list[bytes | None] = []
    int_kind: type | None = None
    for value in values:
        if value is None:
            encoded.append(None)
            continue
        if isinstance(value, (tuple, list, set, frozenset, dict)):
            raise ProgrammingError("nested prepared collections are not supported")
        if element_type == CUBRIDDataType.INT:
            if isinstance(value, bool) or not isinstance(value, (int, str)):
                raise ProgrammingError("unsupported prepared INT element type")
            kind = str if isinstance(value, str) else int
            if int_kind is None:
                int_kind = kind
            elif int_kind is not kind:
                raise ProgrammingError("prepared INT collection mixes int and string elements")
            encoded.append(struct.pack(">i", _collection_int(value)))
            continue
        if not isinstance(value, str):
            raise ProgrammingError("unsupported prepared string element type")
        if "\x00" in value:
            raise ProgrammingError("prepared string element contains NUL")
        text, _position = _encode_text(value, encoding)
        if text is None:
            raise DataError(
                f"prepared string element cannot be encoded as {_codec_label(encoding)}"
            )
        encoded.append(text + b"\x00")
    return _PreparedCollection(type_code, element_type, tuple(encoded), encoding)


# Packed LOB handle: [int32 db_type][int64 size][int32 locator length][locator NUL].
# db_type is the server DB_TYPE (BLOB 33, CLOB 34), not the CCI u_type.
_LOB_HANDLE_HEADER = struct.Struct(">iqi")
_LOB_DB_TYPES: dict[int, int] = {CUBRIDDataType.BLOB: 33, CUBRIDDataType.CLOB: 34}


def _packed_lob_size(packed_handle: bytes) -> int:
    """Return the size field (bytes 4..12) of a packed LOB handle."""
    _db_type, size, _locator_size = _LOB_HANDLE_HEADER.unpack_from(packed_handle)
    return int(size)


def _packed_lob_handle_after_write(packed_handle: bytes, end: int) -> bytes:
    """Return the handle with its size raised to ``end`` after a LOB_WRITE.

    CCI does the same after every LOB_WRITE (cci_query_execute.c qe_lob_write):
    the size never shrinks. The broker trusts this field when the handle is
    bound (cas_execute.c caslob_to_dblob), so a stale size would store a wrong
    length. A handle that does not parse is returned unchanged.
    """
    header = _LOB_HANDLE_HEADER.size
    if len(packed_handle) < header:
        return packed_handle
    db_type, size, locator_size = _LOB_HANDLE_HEADER.unpack_from(packed_handle)
    if locator_size != len(packed_handle) - header or end <= size:
        return packed_handle
    return _LOB_HANDLE_HEADER.pack(db_type, end, locator_size) + packed_handle[header:]


@dataclass(frozen=True, slots=True)
class _PreparedLob:
    """One BLOB/CLOB handle bound in a prepared FC3 request (#441).

    The type argument is the CCI u_type (BLOB ``23``, CLOB ``24``) and the
    value argument is the packed handle as the server sent it, the same
    bytes CCI sends for ``CCI_A_TYPE_BLOB``/``CLOB``. The broker builds the
    stored value from the handle's own type, size and locator, so the
    framing is checked exactly. ``owner`` (the driver connection, compared
    by identity) and ``generation`` (its physical-session counter) together
    name the one physical session the binding was made for; generation
    numbers alone repeat across connections. The binding must not be sent
    on any other session.
    """

    type_code: int
    packed_handle: bytes
    owner: object
    generation: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.type_code, bool)
            or not isinstance(self.type_code, int)
            or self.type_code not in _LOB_DB_TYPES
        ):
            raise ProgrammingError("unsupported prepared LOB type code")
        if type(self.packed_handle) is not bytes:
            raise ProgrammingError("invalid prepared LOB handle")
        header = _LOB_HANDLE_HEADER.size
        if len(self.packed_handle) <= header:
            raise ProgrammingError("invalid prepared LOB handle")
        db_type, size, locator_size = _LOB_HANDLE_HEADER.unpack_from(self.packed_handle)
        locator = self.packed_handle[header:]
        if (
            db_type != _LOB_DB_TYPES[self.type_code]
            or size < 0
            or locator_size != len(locator)
            or not locator.endswith(b"\x00")
            or b"\x00" in locator[:-1]
        ):
            raise ProgrammingError("invalid prepared LOB handle")
        if self.owner is None or type(self.generation) is not int:
            raise ProgrammingError("invalid prepared LOB session owner")

    @property
    def payload(self) -> bytes:
        """Return the exact FC3 value argument (without its own length prefix)."""
        return self.packed_handle


_PreparedBinding = _PreparedScalar | _PreparedCollection | _PreparedLob

# The 10.2-11.4 brokers reject the MULTISET bind kind (-454: cas_execute.c
# wraps it with db_make_set()) and a SET kind drops duplicates, so a Multiset
# is sent as SEQUENCE, as compat.native does; the server stores it into a
# MULTISET column with its duplicates.
_COLLECTION_PARAM_TYPE_CODES: dict[type, int] = {
    Set: CUBRIDDataType.SET,
    Multiset: CUBRIDDataType.SEQUENCE,
    SequenceParam: CUBRIDDataType.SEQUENCE,
}


def _write_prepared_binding(writer: PacketWriter, binding: Any, encoding: str) -> None:
    """Write one (type byte, value) argument pair of an FC3 or FC21 request."""
    text_encoding: str | None = None
    if isinstance(binding, _PreparedScalar):
        if binding.type_code == CUBRIDDataType.CHAR:
            text_encoding = binding.encoding
    elif isinstance(binding, _PreparedCollection):
        if binding.element_type == CUBRIDDataType.STRING:
            text_encoding = binding.encoding
    elif not isinstance(binding, _PreparedLob):  # a LOB handle is not text
        raise ProgrammingError("invalid prepared parameter encoding")
    if text_encoding is not None and text_encoding != encoding:
        raise ProgrammingError("prepared string was encoded for a different charset")
    writer.add_byte(binding.type_code)
    writer.add_bytes(binding.payload)


def _encode_prepared_value(value: Any, encoding: str = "utf-8") -> _PreparedBinding:
    """Encode one ``executemany(prepared=True)`` value as a typed bind (#839).

    A ``pycubrid.types`` ``Set``, ``Multiset`` (sent as SEQUENCE) or
    ``Sequence`` becomes a collection of STRING elements that the server converts to the column's
    element type, as ``compat.native`` sends imported sets: ``str`` elements
    as they are, ``int`` elements (not ``bool``) as decimal text and ``None``
    as a NULL element. Every other value goes through
    :func:`_encode_prepared_scalar`.
    """
    cls = type(value)
    type_code = _COLLECTION_PARAM_TYPE_CODES.get(cls)
    if type_code is None:
        if issubclass(cls, (list, tuple, set, frozenset, dict)):
            raise ProgrammingError(
                "cannot bind a list/tuple/set/frozenset/dict as a single parameter; "
                "wrap the elements in pycubrid.types.Set, Multiset or Sequence"
            )
        return _encode_prepared_scalar(value, encoding)
    try:
        elements = _COLLECTION_ELEMENTS(value)
    except AttributeError:
        raise ProgrammingError(
            f"{cls.__name__} parameter is missing its elements; it was not "
            f"constructed through {cls.__name__}(...)"
        ) from None
    if type(elements) is not tuple:
        raise ProgrammingError("collection parameter elements must be a tuple")
    texts: list[str | None] = []
    for element in elements:
        element_cls = type(element)
        if element is None:
            texts.append(None)
        elif issubclass(element_cls, _Collection):
            raise ProgrammingError("nested collection parameters are not supported")
        elif issubclass(element_cls, str):
            texts.append(str.__str__(element))
        elif issubclass(element_cls, int) and element_cls is not bool:
            number = int.__index__(element)
            if not -_NUMERIC_LIMIT < number < _NUMERIC_LIMIT:
                raise DataError("prepared integer element needs more than 38 digits")
            texts.append(int.__repr__(number))
        else:
            raise ProgrammingError("unsupported prepared collection element type")
    return _encode_prepared_collection(tuple(texts), type_code, CUBRIDDataType.STRING, encoding)


def _encode_prepared_rows(
    seq_of_parameters: Any,
    encoding: str,
    bind_other: Callable[[Any], _PreparedBinding | None] | None = None,
) -> tuple[tuple[_PreparedBinding, ...], ...]:
    """Encode every row of ``executemany(prepared=True)`` before any I/O (#839).

    ``seq_of_parameters`` may be any iterable (a generator is consumed once).
    Each row must be a sequence (not ``str``/``bytes``) and every row must
    have the same length, at least one. ``bind_other`` is tried first for
    each value (the cursors pass their ``Lob`` handling); it returns ``None``
    for values it does not handle. A rejected value names its position as
    ``rows[i][j]`` in the message and never echoes the value.
    """
    if isinstance(seq_of_parameters, (str, bytes, bytearray)):
        raise ProgrammingError("seq_of_parameters must be an iterable of parameter sequences")
    try:
        iterator = iter(seq_of_parameters)
    except TypeError:
        raise ProgrammingError(
            "seq_of_parameters must be an iterable of parameter sequences"
        ) from None
    encoded_rows: list[tuple[_PreparedBinding, ...]] = []
    width: int | None = None
    for i, row in enumerate(iterator):
        if not isinstance(row, Sequence) or isinstance(row, (str, bytes, bytearray)):
            raise ProgrammingError(f"rows[{i}]: parameters must be a sequence")
        values = list(row)
        if width is None:
            if not values:
                raise NotSupportedError(
                    "executemany(prepared=True) needs at least one ? parameter marker"
                )
            width = len(values)
        elif len(values) != width:
            raise ProgrammingError(f"rows[{i}]: wrong number of parameters")
        bindings: list[_PreparedBinding] = []
        for j, value in enumerate(values):
            try:
                binding = None if bind_other is None else bind_other(value)
                if binding is None:
                    binding = _encode_prepared_value(value, encoding)
            except (DataError, InterfaceError, NotSupportedError, ProgrammingError) as exc:
                raise type(exc)(f"rows[{i}][{j}]: {exc.msg}", code=exc.code) from None
            bindings.append(binding)
        encoded_rows.append(tuple(bindings))
    return tuple(encoded_rows)
