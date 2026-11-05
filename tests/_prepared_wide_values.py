"""Wide prepared-bind columns and rows shared by the live prepared tests (#840, #839)."""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

_NEW_YORK = ZoneInfo("America/New_York")
_COLUMNS = (
    ("b", "INTEGER"),
    ("bi", "BIGINT"),
    ("big", "NUMERIC(38,0)"),
    ("d", "DOUBLE"),
    ("n", "NUMERIC(38,10)"),
    ("frac", "NUMERIC(38,38)"),
    ("dt", "DATE"),
    ("tm", "TIME"),
    ("dtm", "DATETIME"),
    ("dtz", "DATETIMETZ"),
    ("vb", "BIT VARYING(64)"),
)
# One row per case; every column is bound in each row.
_ROWS: tuple[tuple[Any, ...], ...] = (
    (
        True,
        2**63 - 1,
        10**38 - 1,
        0.1,
        Decimal("-123.4500000000"),
        Decimal("0." + "1" * 38),
        datetime.date(1, 1, 1),
        datetime.time(0, 0, 0),
        datetime.datetime(2024, 2, 29, 23, 59, 59, 999999),
        datetime.datetime(2024, 1, 2, 3, 4, 5, 6000, tzinfo=ZoneInfo("Asia/Seoul")),
        b"\x00\xff",
    ),
    (
        False,
        -(2**63),
        -(2**63) - 1,
        -1e300,
        Decimal("0.5"),
        Decimal("-0.00000000000000000000000000000000000001"),
        datetime.date(9999, 12, 31),
        datetime.time(23, 59, 59, 999999),
        datetime.datetime(1, 1, 1, 0, 0, 0),
        # The second 01:30 of the repeated hour (EST), not the first (EDT).
        datetime.datetime(2024, 11, 3, 1, 30, tzinfo=_NEW_YORK, fold=1),
        b"\x01",
    ),
    (
        True,
        2**31,
        2**64,
        -0.0,
        Decimal("1E+5"),
        Decimal("0E-38"),
        datetime.date(2024, 2, 29),
        datetime.time(12, 0, 1),
        datetime.datetime(2024, 1, 1, 0, 0, 0, 1000),
        datetime.datetime(
            2024, 6, 1, 12, 0, tzinfo=datetime.timezone(-datetime.timedelta(hours=5, minutes=30))
        ),
        bytearray(b"pycubrid"),
    ),
    (None,) * 11,
)
