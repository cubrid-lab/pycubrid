"""Tests for pycubrid.types — PEP 249 type objects and constructors."""

from __future__ import annotations

import datetime

import pytest

from pycubrid.types import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Binary,
    DBAPIType,
    Date,
    DateFromTicks,
    Time,
    Timestamp,
    TimestampFromTicks,
    TimeFromTicks,
)


# ---------------------------------------------------------------------------
# Type object existence and identity
# ---------------------------------------------------------------------------


class TestTypeObjects:
    """Verify PEP 249 type objects exist and have correct identity."""

    def test_string_exists(self) -> None:
        assert isinstance(STRING, DBAPIType)
        assert STRING.name == "STRING"

    def test_binary_exists(self) -> None:
        assert isinstance(BINARY, DBAPIType)
        assert BINARY.name == "BINARY"

    def test_number_exists(self) -> None:
        assert isinstance(NUMBER, DBAPIType)
        assert NUMBER.name == "NUMBER"

    def test_datetime_exists(self) -> None:
        assert isinstance(DATETIME, DBAPIType)
        assert DATETIME.name == "DATETIME"

    def test_rowid_exists(self) -> None:
        assert isinstance(ROWID, DBAPIType)
        assert ROWID.name == "ROWID"


# ---------------------------------------------------------------------------
# Type object equality with CUBRID type codes
# ---------------------------------------------------------------------------


class TestTypeEquality:
    """Verify type objects compare equal to their CUBRID CCI_U_TYPE codes."""

    @pytest.mark.parametrize(
        ("type_obj", "code"),
        [
            (STRING, 1),  # CHAR
            (STRING, 2),  # STRING
            (STRING, 3),  # NCHAR
            (STRING, 4),  # VARNCHAR
            (STRING, 25),  # ENUM
            (STRING, 24),  # CLOB
        ],
    )
    def test_string_matches(self, type_obj: DBAPIType, code: int) -> None:
        assert type_obj == code

    @pytest.mark.parametrize(
        ("type_obj", "code"),
        [
            (BINARY, 5),  # BIT
            (BINARY, 6),  # VARBIT
            (BINARY, 23),  # BLOB
        ],
    )
    def test_binary_matches(self, type_obj: DBAPIType, code: int) -> None:
        assert type_obj == code

    @pytest.mark.parametrize(
        ("type_obj", "code"),
        [
            (NUMBER, 7),  # NUMERIC
            (NUMBER, 8),  # INT
            (NUMBER, 9),  # SHORT
            (NUMBER, 10),  # MONETARY
            (NUMBER, 11),  # FLOAT
            (NUMBER, 12),  # DOUBLE
            (NUMBER, 21),  # BIGINT
        ],
    )
    def test_number_matches(self, type_obj: DBAPIType, code: int) -> None:
        assert type_obj == code

    @pytest.mark.parametrize(
        ("type_obj", "code"),
        [
            (DATETIME, 13),  # DATE
            (DATETIME, 14),  # TIME
            (DATETIME, 15),  # TIMESTAMP
            (DATETIME, 22),  # DATETIME
            (DATETIME, 29),  # TIMESTAMPTZ
            (DATETIME, 30),  # TIMESTAMPLTZ
            (DATETIME, 31),  # DATETIMETZ
            (DATETIME, 32),  # DATETIMELTZ
        ],
    )
    def test_datetime_matches(self, type_obj: DBAPIType, code: int) -> None:
        assert type_obj == code

    def test_rowid_matches_object(self) -> None:
        assert ROWID == 19  # OBJECT

    def test_no_cross_match(self) -> None:
        assert STRING != 8  # INT is NUMBER, not STRING
        assert NUMBER != 2  # STRING is STRING, not NUMBER
        assert BINARY != 13  # DATE is DATETIME, not BINARY

    def test_type_self_equality(self) -> None:
        assert STRING == STRING
        assert BINARY == BINARY

    def test_type_inequality(self) -> None:
        assert STRING != NUMBER
        assert BINARY != DATETIME


# ---------------------------------------------------------------------------
# DBAPIType semantics
# ---------------------------------------------------------------------------


class TestDBAPITypeSemantics:
    """Verify DBAPIType hash and comparison edge cases."""

    def test_hash_same_for_equal(self) -> None:
        t1 = DBAPIType("A", frozenset({1, 2}))
        t2 = DBAPIType("B", frozenset({1, 2}))
        assert hash(t1) == hash(t2)

    def test_ne_non_int(self) -> None:
        assert STRING != "not an int"

    def test_eq_returns_not_implemented_for_non_int(self) -> None:
        # __eq__ with unsupported type should not raise
        assert STRING.__eq__("hello") is NotImplemented

    def test_ne_returns_not_implemented_for_non_int(self) -> None:
        assert STRING.__ne__("hello") is NotImplemented

    def test_repr(self) -> None:
        assert repr(STRING) == "DBAPIType('STRING')"
        assert repr(NUMBER) == "DBAPIType('NUMBER')"


# ---------------------------------------------------------------------------
# Constructor tests
# ---------------------------------------------------------------------------


class TestConstructors:
    """Verify PEP 249 constructor functions."""

    def test_date_returns_date(self) -> None:
        d = Date(2026, 3, 12)
        assert isinstance(d, datetime.date)
        assert d == datetime.date(2026, 3, 12)

    def test_time_returns_time(self) -> None:
        t = Time(14, 30, 0)
        assert isinstance(t, datetime.time)
        assert t == datetime.time(14, 30, 0)

    def test_timestamp_returns_datetime(self) -> None:
        ts = Timestamp(2026, 3, 12, 14, 30, 0)
        assert isinstance(ts, datetime.datetime)
        assert ts == datetime.datetime(2026, 3, 12, 14, 30, 0)

    def test_date_from_ticks(self) -> None:
        ticks = 1741766400.0  # 2025-03-12 in some timezone
        d = DateFromTicks(ticks)
        assert isinstance(d, datetime.date)

    def test_time_from_ticks(self) -> None:
        ticks = 1741766400.0
        t = TimeFromTicks(ticks)
        assert isinstance(t, datetime.time)

    def test_timestamp_from_ticks(self) -> None:
        ticks = 1741766400.0
        ts = TimestampFromTicks(ticks)
        assert isinstance(ts, datetime.datetime)

    def test_binary_from_bytes(self) -> None:
        b = Binary(b"\x00\x01\x02")
        assert isinstance(b, bytes)
        assert b == b"\x00\x01\x02"

    def test_binary_from_bytearray(self) -> None:
        b = Binary(bytearray(b"\xab\xcd"))
        assert isinstance(b, bytes)
        assert b == b"\xab\xcd"

    def test_binary_from_str(self) -> None:
        b = Binary("hello")
        assert isinstance(b, bytes)
        assert b == b"hello"

    def test_binary_invalid_type(self) -> None:
        with pytest.raises(TypeError, match="must be bytes"):
            Binary(12345)  # type: ignore[arg-type]

    def test_date_from_ticks_consistency(self) -> None:
        """DateFromTicks and TimestampFromTicks should agree on date."""
        ticks = 1741766400.0
        d = DateFromTicks(ticks)
        ts = TimestampFromTicks(ticks)
        assert d == ts.date()
