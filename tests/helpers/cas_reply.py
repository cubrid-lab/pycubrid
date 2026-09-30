"""Build well-formed CAS broker replies for protocol fuzzing (issue #523).

The fuzz targets in ``tests/test_protocol_fuzz.py`` mutate *realistic* replies:
execute replies with column metadata for the common CUBRID types, FETCH replies
with several rows of populated cells, and schema, batch and LOB replies. Each
reply is built here together with

* the exact values the driver must decode from it when it is left unmutated
  (so the unmutated seed is an exact round-trip oracle, not "no exception"), and
* the offsets of its length words, count words, collection element-type bytes
  and field boundaries, so a mutation strategy can aim at the places where
  framing bugs hide (truncation at a cell edge, a length that disagrees with its
  payload, a collection whose count or element type is wrong).

Replies start at CAS_INFO, i.e. after the 4-byte DATA_LENGTH prefix, exactly as
``packet.parse()`` receives them from ``_send_and_receive``.
"""

from __future__ import annotations

import datetime
import struct
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from pycubrid.constants import CUBRIDDataType as T
from pycubrid.constants import CUBRIDStatementType
from pycubrid.protocol import ColumnMetaData, ResultInfo, _SchemaColumn

CAS_INFO = b"\x01\x00\x00\x00"  # IN_TRAN
OID = b"\x00\x00\x00\x2a\x00\x03\x00\x00"

_COLLECTION_KIND_BITS = {T.SET: 0x20, T.MULTISET: 0x40, T.SEQUENCE: 0x60}


# ---------------------------------------------------------------------------
# Byte builder that remembers where the interesting fields are
# ---------------------------------------------------------------------------


@dataclass
class Wire:
    """A growing reply plus the offsets of the fields a mutator should target."""

    buf: bytearray = field(default_factory=bytearray)
    lengths: list[int] = field(default_factory=list)  # int32 length/size words
    counts: list[int] = field(default_factory=list)  # int32 count words
    element_types: list[int] = field(default_factory=list)  # collection element-type bytes
    boundaries: list[int] = field(default_factory=list)  # field / cell / row starts

    def mark(self) -> None:
        self.boundaries.append(len(self.buf))

    def byte(self, value: int) -> None:
        self.buf += struct.pack(">B", value)

    def i16(self, value: int) -> None:
        self.buf += struct.pack(">h", value)

    def i32(self, value: int) -> None:
        self.buf += struct.pack(">i", value)

    def i64(self, value: int) -> None:
        self.buf += struct.pack(">q", value)

    def raw(self, value: bytes) -> None:
        self.buf += value

    def length(self, value: int) -> None:
        self.lengths.append(len(self.buf))
        self.i32(value)

    def count(self, value: int) -> None:
        self.counts.append(len(self.buf))
        self.i32(value)

    def element_type(self, value: int) -> None:
        self.element_types.append(len(self.buf))
        self.byte(value)

    def text(self, value: str) -> None:
        """A length-prefixed, NUL-terminated string (metadata names, messages)."""
        encoded = value.encode("utf-8") + b"\x00"
        self.length(len(encoded))
        self.raw(encoded)

    def embed(self, other: Wire) -> None:
        """Append ``other``, shifting its recorded offsets to their new position."""
        base = len(self.buf)
        self.buf += other.buf
        self.lengths += [base + offset for offset in other.lengths]
        self.counts += [base + offset for offset in other.counts]
        self.element_types += [base + offset for offset in other.element_types]
        self.boundaries += [base + offset for offset in other.boundaries]

    def seed(self, name: str) -> Seed:
        return Seed(
            name=name,
            data=bytes(self.buf),
            lengths=tuple(self.lengths),
            counts=tuple(self.counts),
            element_types=tuple(self.element_types),
            boundaries=tuple(sorted(set(self.boundaries))),
        )


@dataclass(frozen=True)
class Seed:
    """An unmutated reply and the offsets a mutation strategy may target."""

    name: str
    data: bytes
    lengths: tuple[int, ...] = ()
    counts: tuple[int, ...] = ()
    element_types: tuple[int, ...] = ()
    boundaries: tuple[int, ...] = ()


# ---------------------------------------------------------------------------
# Cell values: wire payload + the value the driver must decode from it
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Value:
    """One non-NULL cell: how to write its payload and what it must decode to.

    ``expected`` is the decoded value with ``decode_collections=True`` and no
    JSON deserializer; ``decoded_json`` is a JSON cell's value under
    ``json.loads``. With ``decode_collections=False`` a collection decodes to
    its raw payload bytes.
    """

    column_type: int
    write: Callable[[Wire], None]
    expected: Any
    decoded_json: Any = None

    def payload(self) -> Wire:
        sub = Wire()
        self.write(sub)
        return sub

    def expected_for(self, *, decode_collections: bool, json_loads: bool) -> Any:
        if self.column_type in _COLLECTION_KIND_BITS and not decode_collections:
            return bytes(self.payload().buf)
        if self.column_type == T.JSON and json_loads:
            return self.decoded_json
        return self.expected


def _fixed(column_type: int, fmt: str, value: Any, expected: Any = None) -> Value:
    packed = struct.pack(fmt, value)
    return Value(column_type, lambda w: w.raw(packed), value if expected is None else expected)


def text(value: str, column_type: int = T.STRING) -> Value:
    encoded = value.encode("utf-8") + b"\x00"
    return Value(column_type, lambda w: w.raw(encoded), value)


def short(value: int) -> Value:
    return _fixed(T.SHORT, ">h", value)


def int_(value: int) -> Value:
    return _fixed(T.INT, ">i", value)


def bigint(value: int) -> Value:
    return _fixed(T.BIGINT, ">q", value)


def float_(value: float) -> Value:
    return _fixed(T.FLOAT, ">f", value)


def double(value: float, column_type: int = T.DOUBLE) -> Value:
    return _fixed(column_type, ">d", value)


def numeric(value: str) -> Value:
    encoded = value.encode("ascii") + b"\x00"
    return Value(T.NUMERIC, lambda w: w.raw(encoded), Decimal(value))


def date(y: int, m: int, d: int) -> Value:
    packed = struct.pack(">3h", y, m, d)
    return Value(T.DATE, lambda w: w.raw(packed), datetime.date(y, m, d))


def time(h: int, mi: int, s: int) -> Value:
    packed = struct.pack(">3h", h, mi, s)
    return Value(T.TIME, lambda w: w.raw(packed), datetime.time(h, mi, s))


def datetime_(y: int, mo: int, d: int, h: int, mi: int, s: int, ms: int) -> Value:
    packed = struct.pack(">7h", y, mo, d, h, mi, s, ms)
    expected = datetime.datetime(y, mo, d, h, mi, s, ms * 1000)
    return Value(T.DATETIME, lambda w: w.raw(packed), expected)


def timestamp(y: int, mo: int, d: int, h: int, mi: int, s: int) -> Value:
    packed = struct.pack(">6h", y, mo, d, h, mi, s)
    return Value(T.TIMESTAMP, lambda w: w.raw(packed), datetime.datetime(y, mo, d, h, mi, s))


def _tzinfo(zone: str) -> datetime.tzinfo:
    token = zone.split()[0]
    if token[0] in "+-":
        sign = 1 if token[0] == "+" else -1
        hours, minutes = int(token[1:3]), int(token[4:6] or "0")
        return datetime.timezone(sign * datetime.timedelta(hours=hours, minutes=minutes))
    return ZoneInfo(token)


def timestamptz(column_type: int, fields: tuple[int, int, int, int, int, int], zone: str) -> Value:
    """TIMESTAMPTZ / TIMESTAMPLTZ: 6 shorts then a NUL-terminated zone string."""
    packed = struct.pack(">6h", *fields) + zone.encode("ascii") + b"\x00"
    expected = datetime.datetime(*fields, tzinfo=_tzinfo(zone))
    return Value(column_type, lambda w: w.raw(packed), expected)


def datetimetz(
    column_type: int, fields: tuple[int, int, int, int, int, int, int], zone: str
) -> Value:
    """DATETIMETZ / DATETIMELTZ: 7 shorts (with milliseconds) then the zone string."""
    packed = struct.pack(">7h", *fields) + zone.encode("ascii") + b"\x00"
    y, mo, d, h, mi, s, ms = fields
    expected = datetime.datetime(y, mo, d, h, mi, s, ms * 1000, tzinfo=_tzinfo(zone))
    return Value(column_type, lambda w: w.raw(packed), expected)


def bits(value: bytes, column_type: int = T.VARBIT) -> Value:
    return Value(column_type, lambda w: w.raw(value), value)


def oid(page: int, slot: int, volume: int) -> Value:
    packed = struct.pack(">ihh", page, slot, volume)
    return Value(T.OBJECT, lambda w: w.raw(packed), f"OID:@{page}|{slot}|{volume}")


def json_(document: str, decoded: Any) -> Value:
    encoded = document.encode("utf-8") + b"\x00"
    return Value(T.JSON, lambda w: w.raw(encoded), document, decoded_json=decoded)


def lob(column_type: int, lob_length: int, locator: str) -> Value:
    """A packed BLOB/CLOB handle: type, length, locator size and locator."""
    handle = Wire()
    handle.i32(column_type)
    handle.i64(lob_length)
    handle.text(locator)
    packed = bytes(handle.buf)
    expected = {
        "lob_type": column_type,
        "lob_length": lob_length,
        "file_locator": locator,
        "packed_lob_handle": packed,
    }

    def write(w: Wire) -> None:
        w.embed(handle)

    return Value(column_type, write, expected)


def collection(kind: int, element_type: int, elements: Sequence[Value | None]) -> Value:
    """SET / MULTISET / SEQUENCE: element type, count, then sized elements."""

    def write(w: Wire) -> None:
        w.element_type(element_type)
        w.count(len(elements))
        for element in elements:
            w.mark()
            if element is None:
                w.length(-1)
                continue
            payload = element.payload()
            w.length(len(payload.buf))
            w.embed(payload)

    decoded = [None if e is None else e.expected for e in elements]
    expected: Any = frozenset(decoded) if kind == T.SET else decoded
    return Value(kind, write, expected)


def null_collection(kind: int, count: int) -> Value:
    """An empty or all-NULL collection: element type NULL and ``count`` -1 lengths (#483)."""
    return collection(kind, T.NULL, [None] * count)


# ---------------------------------------------------------------------------
# Columns and rows
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Column:
    name: str
    column_type: int  # the type the driver reports (SET/MULTISET/SEQUENCE for collections)
    element_type: int = 0  # element type of a collection column
    scale: int = 0
    precision: int = 0
    table: str = "fuzz_t"
    nullable: bool = True
    default: str = ""
    primary_key: bool = False
    # Protocol 7+ layout: 0x80 (plus collection bits) then a full type byte. The
    # legacy single byte cannot carry type codes >= 0x20 (they collide with the
    # collection bits), so only low scalar types may use it.
    legacy_type_byte: bool = False

    def metadata(self) -> ColumnMetaData:
        return ColumnMetaData(
            column_type=self.column_type,
            scale=self.scale,
            precision=self.precision,
            name=self.name,
            real_name=self.name,
            table_name=self.table,
            is_nullable=self.nullable,
            default_value=self.default,
            is_unique_key=self.primary_key,
            is_primary_key=self.primary_key,
        )

    def schema(self) -> _SchemaColumn:
        return _SchemaColumn(self.column_type, self.scale, self.precision, self.name)

    def write_type(self, w: Wire) -> None:
        kind = _COLLECTION_KIND_BITS.get(self.column_type)
        if kind is not None:
            w.byte(0x80 | kind)
            w.byte(self.element_type)
        elif self.legacy_type_byte:
            assert self.column_type < 0x20, "legacy type byte collides with collection bits"
            w.byte(self.column_type)
        else:
            w.byte(0x80)
            w.byte(self.column_type)


def write_column_metadata(w: Wire, columns: Sequence[Column]) -> None:
    for col in columns:
        w.mark()
        col.write_type(w)
        w.i16(col.scale)
        w.i32(col.precision)
        w.text(col.name)
        w.text(col.name)  # real name
        w.text(col.table)
        w.byte(0 if col.nullable else 1)  # is_non_null
        w.text(col.default)
        pk = 1 if col.primary_key else 0
        for flag in (0, pk, pk, 0, 0, 0, 0):  # auto_inc, unique, pk, rev idx/uniq, fk, shared
            w.byte(flag)


def write_schema_columns(w: Wire, columns: Sequence[Column]) -> None:
    for col in columns:
        w.mark()
        col.write_type(w)
        w.i16(col.scale)
        w.i32(col.precision)
        w.text(col.name)


Row = Sequence[Value | None]


def write_rows(w: Wire, rows: Sequence[Row], typed: Sequence[bool]) -> None:
    """Rows: index, OID, then one sized cell per column (``-1`` is SQL NULL).

    ``typed[i]`` selects the CALL / NULL-typed-column layout for column ``i``:
    the cell starts with its own type byte and its size includes that byte.
    """
    for index, row in enumerate(rows, start=1):
        w.mark()
        w.i32(index)
        w.raw(OID)
        for cell, typed_cell in zip(row, typed):
            w.mark()
            if cell is None:
                w.length(-1)
                continue
            payload = cell.payload()
            if typed_cell:
                w.length(len(payload.buf) + 1)
                w.byte(cell.column_type)
            else:
                w.length(len(payload.buf))
            w.embed(payload)
    w.mark()


def expected_rows(
    rows: Sequence[Row], *, decode_collections: bool, json_loads: bool
) -> list[tuple[Any, ...]]:
    return [
        tuple(
            None
            if cell is None
            else cell.expected_for(decode_collections=decode_collections, json_loads=json_loads)
            for cell in row
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Result sets used by the seeds
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResultSet:
    name: str
    columns: tuple[Column, ...]
    rows: tuple[tuple[Value | None, ...], ...]
    statement_type: int = CUBRIDStatementType.SELECT

    def typed(self) -> list[bool]:
        """Which columns carry a per-cell type byte (CALL results, NULL-typed columns)."""
        call = self.statement_type in (
            CUBRIDStatementType.CALL,
            CUBRIDStatementType.EVALUATE,
            CUBRIDStatementType.CALL_SP,
        )
        return [call or c.column_type == T.NULL for c in self.columns]

    def metadata(self) -> list[ColumnMetaData]:
        return [c.metadata() for c in self.columns]

    def expected(self, *, decode_collections: bool, json_loads: bool) -> list[tuple[Any, ...]]:
        return expected_rows(
            self.rows, decode_collections=decode_collections, json_loads=json_loads
        )


_SEOUL = (2026, 3, 4, 5, 6, 7)

STRINGS = ResultSet(
    "strings",
    (
        Column("c_char", T.CHAR, precision=4, legacy_type_byte=True),
        Column("c_varchar", T.STRING, precision=64),
        Column("c_nchar", T.NCHAR, precision=4),
        Column("c_varnchar", T.VARNCHAR, precision=32),
        Column("c_enum", T.ENUM, nullable=False, default="red"),
    ),
    (
        (
            text("abcd", T.CHAR),
            text("héllo, 세계"),
            text("xy  ", T.NCHAR),
            text("ü", T.VARNCHAR),
            text("red", T.ENUM),
        ),
        (text("", T.CHAR), None, None, text("", T.VARNCHAR), text("blue", T.ENUM)),
        (None, text("line\nbreak\ttab"), text("z   ", T.NCHAR), None, text("green", T.ENUM)),
    ),
)

NUMBERS = ResultSet(
    "numbers",
    (
        Column("c_short", T.SHORT, precision=5),
        Column("c_int", T.INT, precision=10, nullable=False, primary_key=True),
        Column("c_bigint", T.BIGINT, precision=19),
        Column("c_float", T.FLOAT, precision=7),
        Column("c_double", T.DOUBLE, precision=15),
        Column("c_monetary", T.MONETARY, precision=15, scale=2),
        Column("c_numeric", T.NUMERIC, precision=15, scale=4),
    ),
    (
        (
            short(-32768),
            int_(2**31 - 1),
            bigint(-(2**63)),
            float_(1.5),
            double(3.141592653589793),
            double(12.25, T.MONETARY),
            numeric("12345.6789"),
        ),
        (short(0), int_(-1), bigint(2**63 - 1), None, double(-0.0), None, numeric("-0.0001")),
        (None, int_(0), None, float_(-2.25), None, double(0.0, T.MONETARY), None),
    ),
)

TEMPORAL = ResultSet(
    "temporal",
    (
        Column("c_date", T.DATE, legacy_type_byte=True),
        Column("c_time", T.TIME),
        Column("c_datetime", T.DATETIME, scale=3),
        Column("c_timestamp", T.TIMESTAMP),
        Column("c_tstz", T.TIMESTAMPTZ),
        Column("c_tsltz", T.TIMESTAMPLTZ),
        Column("c_dttz", T.DATETIMETZ, scale=3),
        Column("c_dtltz", T.DATETIMELTZ, scale=3),
    ),
    (
        (
            date(2026, 9, 30),
            time(23, 59, 58),
            datetime_(2026, 9, 30, 12, 34, 56, 789),
            timestamp(2026, 1, 2, 3, 4, 5),
            timestamptz(T.TIMESTAMPTZ, _SEOUL, "Asia/Seoul KST"),
            timestamptz(T.TIMESTAMPLTZ, _SEOUL, "+09:00"),
            datetimetz(T.DATETIMETZ, (2026, 1, 15, 8, 30, 0, 125), "America/New_York EST"),
            datetimetz(T.DATETIMELTZ, (2026, 1, 15, 8, 30, 0, 999), "-05:30"),
        ),
        (
            date(1, 1, 1),
            time(0, 0, 0),
            datetime_(9999, 12, 31, 23, 59, 59, 999),
            None,
            timestamptz(T.TIMESTAMPTZ, (2026, 7, 1, 0, 0, 0), "UTC"),
            None,
            datetimetz(T.DATETIMETZ, (2026, 7, 1, 0, 0, 0, 0), "+00:00"),
            None,
        ),
    ),
)

BITS_AND_OIDS = ResultSet(
    "bits_oids",
    (
        Column("c_bit", T.BIT, precision=8, legacy_type_byte=True),
        Column("c_varbit", T.VARBIT, precision=64),
        Column("c_oid", T.OBJECT),
    ),
    (
        (bits(b"\xa5", T.BIT), bits(b"\x01\x02\x03\xff"), oid(100, 2, 0)),
        (bits(b"\x00", T.BIT), bits(b"\x80\x00"), None),
        (None, bits(bytes(range(32))), oid(-1, -1, -1)),
    ),
)

COLLECTIONS = ResultSet(
    "collections",
    (
        Column("c_set", T.SET, element_type=T.INT),
        Column("c_multiset", T.MULTISET, element_type=T.STRING),
        Column("c_seq", T.SEQUENCE, element_type=T.DATE),
        Column("c_seq_num", T.SEQUENCE, element_type=T.NUMERIC),
    ),
    (
        (
            collection(T.SET, T.INT, [int_(1), int_(2), int_(3)]),
            collection(T.MULTISET, T.STRING, [text("a"), text("a"), text("bé")]),
            collection(T.SEQUENCE, T.DATE, [date(2026, 1, 1), None, date(2026, 12, 31)]),
            collection(T.SEQUENCE, T.NUMERIC, [numeric("1.50"), numeric("-2")]),
        ),
        (
            null_collection(T.SET, 0),
            null_collection(T.MULTISET, 2),
            collection(T.SEQUENCE, T.DATE, []),
            None,
        ),
    ),
)

LOBS_AND_JSON = ResultSet(
    "lobs_json",
    (
        Column("c_blob", T.BLOB),
        Column("c_clob", T.CLOB),
        Column("c_json", T.JSON),
    ),
    (
        (
            lob(T.BLOB, 4096, "file:/cubrid/lob/ces_001/fuzz_t.00001234567890_1234"),
            lob(T.CLOB, 0, "file:/cubrid/lob/ces_002/fuzz_t.00001234567890_5678"),
            json_('{"k": [1, 2, null], "s": "é"}', {"k": [1, 2, None], "s": "é"}),
        ),
        (None, None, json_("[]", [])),
        (
            lob(T.BLOB, 2**40, "file:/cubrid/lob/ces_003/fuzz_t.00009999999999_0001"),
            None,
            json_('"x"', "x"),
        ),
    ),
)

# A CALL result: every cell carries its own type byte, sized with it. This is
# the single-byte layout the driver decodes; protocol 8 brokers send a two-byte
# header that it does not decode yet (#542).
CALL_RESULT = ResultSet(
    "call",
    (Column("ret", T.NULL),),
    ((int_(42),), (text("out"),), (None,), (numeric("3.25"),)),
    statement_type=CUBRIDStatementType.CALL,
)

# ``SELECT NULL, x``: a NULL-typed column switches the whole row to typed cells.
NULL_TYPED = ResultSet(
    "null_typed",
    (Column("n", T.NULL), Column("x", T.INT)),
    ((None, int_(7)), (text("late"), int_(8))),
)

WIDE = ResultSet(
    "wide",
    tuple(
        col
        for rs in (STRINGS, NUMBERS, TEMPORAL, BITS_AND_OIDS, COLLECTIONS, LOBS_AND_JSON)
        for col in rs.columns
    ),
    (
        tuple(
            cell
            for rs in (STRINGS, NUMBERS, TEMPORAL, BITS_AND_OIDS, COLLECTIONS, LOBS_AND_JSON)
            for cell in rs.rows[0]
        ),
    ),
)

RESULT_SETS: tuple[ResultSet, ...] = (
    STRINGS,
    NUMBERS,
    TEMPORAL,
    BITS_AND_OIDS,
    COLLECTIONS,
    LOBS_AND_JSON,
    CALL_RESULT,
    NULL_TYPED,
    WIDE,
)


# ---------------------------------------------------------------------------
# Whole replies
# ---------------------------------------------------------------------------


def fetch_reply(rs: ResultSet) -> Seed:
    """FC8 FETCH: CAS_INFO, response code, tuple count, rows."""
    w = Wire()
    w.raw(CAS_INFO)
    w.mark()
    w.i32(0)  # response code
    w.count(len(rs.rows))
    write_rows(w, rs.rows, rs.typed())
    return w.seed(f"fetch/{rs.name}")


def _result_info(w: Wire, statement_type: int, count: int) -> None:
    w.mark()
    w.byte(statement_type)
    w.i32(count)
    w.raw(OID)
    w.i32(0)  # cache seconds
    w.i32(0)  # cache microseconds


def prepare_and_execute_reply(rs: ResultSet, *, query_handle: int = 7) -> Seed:
    """FC41 PREPARE_AND_EXECUTE with column metadata and the first page inline."""
    w = Wire()
    w.raw(CAS_INFO)
    w.i32(query_handle)
    w.i32(0)  # result cache lifetime
    w.byte(rs.statement_type)
    w.i32(0)  # bind count
    w.byte(0)  # is_updatable
    w.count(len(rs.columns))
    write_column_metadata(w, rs.columns)
    w.mark()
    w.i32(len(rs.rows))  # total tuple count
    w.byte(0)  # cache reusable
    w.count(1)  # result count
    _result_info(w, rs.statement_type, len(rs.rows))
    w.byte(0)  # includes_column_info
    w.i32(0)  # shard id
    w.mark()
    w.i32(0)  # fetch code
    w.count(len(rs.rows))
    write_rows(w, rs.rows, rs.typed())
    return w.seed(f"prepare_and_execute/{rs.name}")


def prepare_info(w: Wire, rs: ResultSet, *, bind_count: int = 0) -> None:
    """The FC2 tail, also sent by FC3 when it refreshes column info."""
    w.mark()
    w.i32(0)  # result cache lifetime
    w.byte(rs.statement_type)
    w.count(bind_count)
    w.byte(0)  # is_updatable
    w.count(len(rs.columns))
    write_column_metadata(w, rs.columns)


def prepare_reply(rs: ResultSet, *, query_handle: int = 5, bind_count: int = 2) -> Seed:
    """FC2 PREPARE: handle then the prepared column metadata."""
    w = Wire()
    w.raw(CAS_INFO)
    w.i32(query_handle)
    prepare_info(w, rs, bind_count=bind_count)
    w.mark()
    return w.seed(f"prepare/{rs.name}")


def execute_reply(rs: ResultSet, *, refresh_columns: bool) -> Seed:
    """FC3 EXECUTE, optionally carrying refreshed column info, with inline rows."""
    w = Wire()
    w.raw(CAS_INFO)
    w.mark()
    w.i32(len(rs.rows))  # response code = total tuple count
    w.byte(0)  # cache reusable
    w.count(1)
    _result_info(w, rs.statement_type, len(rs.rows))
    w.mark()
    w.byte(1 if refresh_columns else 0)
    if refresh_columns:
        prepare_info(w, rs)
    w.i32(0)  # shard id
    w.mark()
    w.i32(0)  # fetch code
    w.count(len(rs.rows))
    write_rows(w, rs.rows, rs.typed())
    suffix = "refreshed" if refresh_columns else "cached"
    return w.seed(f"execute_{suffix}/{rs.name}")


# Schema (FC9) SCH_CLASS-style result: condensed column metadata, rows via FETCH.
SCHEMA_RESULT = ResultSet(
    "schema_class",
    (
        Column("NAME", T.STRING, precision=255),
        Column("TYPE", T.SHORT, legacy_type_byte=True),
        Column("REMARKS", T.STRING, precision=2048),
    ),
    (
        (text("fuzz_t"), short(2), text("")),
        (text("db_user"), short(0), None),
        (text("사용자"), short(2), text("remark é")),
    ),
)


def schema_reply(rs: ResultSet, *, query_handle: int = 11) -> Seed:
    """FC9 SCHEMA_INFO: handle, tuple count, condensed column metadata."""
    w = Wire()
    w.raw(CAS_INFO)
    w.i32(query_handle)
    w.count(len(rs.rows))
    w.count(len(rs.columns))
    write_schema_columns(w, rs.columns)
    w.mark()
    return w.seed(f"schema/{rs.name}")


@dataclass(frozen=True)
class BatchStatement:
    statement_type: int
    result: int
    error_code: int = 0
    message: str = ""


BATCH_STATEMENTS: tuple[BatchStatement, ...] = (
    BatchStatement(CUBRIDStatementType.INSERT, 1),
    BatchStatement(CUBRIDStatementType.UPDATE, 3),
    BatchStatement(
        CUBRIDStatementType.INSERT,
        -1,
        -670,
        "Operation would have caused one or more unique constraint violations.",
    ),
    BatchStatement(CUBRIDStatementType.DELETE, 0),
    BatchStatement(
        CUBRIDStatementType.INSERT, -1, -494, "Semantic: cannot coerce 'hé' to type integer"
    ),
)


def batch_reply(statements: Sequence[BatchStatement] = BATCH_STATEMENTS) -> Seed:
    """FC20 EXECUTE_BATCH (protocol 8): per-statement result or error, then shard id."""
    w = Wire()
    w.raw(CAS_INFO)
    w.i32(0)  # response code
    w.count(len(statements))
    for stmt in statements:
        w.mark()
        w.byte(stmt.statement_type)
        w.i32(stmt.result)
        if stmt.result < 0:
            w.i32(stmt.error_code)
            w.text(stmt.message)
        else:
            w.i32(0)
            w.i16(0)
            w.i16(0)
    w.mark()
    w.i32(0)  # last shard id
    return w.seed("batch")


LOB_HANDLE = lob(T.BLOB, 10, "file:/cubrid/lob/ces_001/fuzz_t.00001234567890_1234")


def lob_new_reply() -> Seed:
    w = Wire()
    w.raw(CAS_INFO)
    w.i32(0)
    w.mark()
    w.embed(LOB_HANDLE.payload())
    return w.seed("lob_new")


LOB_BYTES = b"\x00\x01binary\xff\xfe"


def lob_read_reply(payload: bytes = LOB_BYTES) -> Seed:
    w = Wire()
    w.raw(CAS_INFO)
    w.length(len(payload))  # response code = bytes read
    w.mark()
    w.raw(payload)
    w.mark()
    return w.seed("lob_read")


def lob_write_reply(written: int = 4096) -> Seed:
    w = Wire()
    w.raw(CAS_INFO)
    w.length(written)
    return w.seed("lob_write")


def expected_result_infos(rs: ResultSet) -> list[ResultInfo]:
    return [ResultInfo(stmt_type=rs.statement_type, result_count=len(rs.rows), oid=OID)]


# ---------------------------------------------------------------------------
# Independent framing walk (the #383 / #512 oracle)
# ---------------------------------------------------------------------------


def fetch_rows_fit(reply: bytes, ncols: int) -> bool:
    """Walk a FETCH reply's rows by their declared sizes, independently of the driver.

    True when every declared row and cell lies inside the reply. The driver may
    only report ``DataError`` (session kept) for such a complete reply, and must
    not return rows from a reply whose declared sizes overrun it (#383, #512).
    """
    if len(reply) < 12:
        return False
    tuple_count = struct.unpack_from(">i", reply, 8)[0]
    if tuple_count < 0:
        return False
    offset = 12
    for _ in range(tuple_count):
        offset += 4 + len(OID)
        for _ in range(ncols):
            if offset + 4 > len(reply):
                return False
            size = struct.unpack_from(">i", reply, offset)[0]
            offset += 4 + max(size, 0)
        if offset > len(reply):
            return False
    return offset <= len(reply)
