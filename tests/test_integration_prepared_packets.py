"""Internal FC2/FC3 scalar wire smoke; not a public prepared API (#475)."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

import pycubrid
from pycubrid.constants import CCIPrepareOption
from pycubrid.protocol import (
    CloseQueryPacket,
    ExecutePacket,
    PreparePacket,
    _encode_prepared_scalar,
)

from ._parity_helpers import connect_kwargs, table_name

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("autocommit", [False, True], ids=["manual", "auto"])
@pytest.mark.parametrize(
    ("sql", "values", "expected"),
    [
        ("SELECT CAST(? AS INTEGER)", (11, 12, None), ((11,), (12,), (None,))),
        (
            "SELECT CAST(? AS VARCHAR(30))",
            ("a'\\한", "", None),
            (("a'\\한",), ("",), (None,)),
        ),
    ],
    ids=["int-null", "char-null"],
)
def test_internal_scalar_packets_repeat_on_one_handle(
    autocommit: bool,
    sql: str,
    values: Sequence[object],
    expected: Sequence[tuple[object, ...]],
) -> None:
    conn = pycubrid.connect(**connect_kwargs(), autocommit=autocommit)
    with conn:
        prep = PreparePacket(sql, auto_commit=autocommit, prepare_flag=CCIPrepareOption.HOLDABLE)
        conn._send_and_receive(prep)
        generation = conn._physical_generation
        try:
            assert prep.bind_count == 1
            for value, row in zip(values, expected):
                packet = ExecutePacket(
                    prep.query_handle,
                    prep.statement_type,
                    auto_commit=autocommit,
                    protocol_version=conn._protocol_version,
                    bindings=(_encode_prepared_scalar(value),),
                    bind_count=prep.bind_count,
                )
                packet.columns = prep.columns
                conn._send_and_receive(packet)
                assert packet.rows == [row]
                assert conn._physical_generation == generation
        finally:
            conn._send_and_receive(CloseQueryPacket(prep.query_handle))


def test_internal_dml_packets_repeat_without_implicit_manual_commit() -> None:
    with pycubrid.connect(**connect_kwargs(), autocommit=False) as conn:
        with pycubrid.connect(**connect_kwargs(), autocommit=True) as observer:
            table = table_name("p475")
            created = False
            try:
                setup = conn.cursor()
                try:
                    setup.execute(f"CREATE TABLE {table} (id INTEGER)")
                    created = True
                finally:
                    setup.close()
                conn.commit()

                def count_rows() -> int:
                    cursor = observer.cursor()
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        row = cursor.fetchone()
                        assert row is not None
                        return int(row[0])
                    finally:
                        cursor.close()

                prep = PreparePacket(
                    f"INSERT INTO {table} VALUES (?)",
                    auto_commit=False,
                    prepare_flag=CCIPrepareOption.HOLDABLE,
                )
                conn._send_and_receive(prep)
                try:
                    for value in (11, 12):
                        packet = ExecutePacket(
                            prep.query_handle,
                            prep.statement_type,
                            auto_commit=False,
                            protocol_version=conn._protocol_version,
                            bindings=(_encode_prepared_scalar(value),),
                            bind_count=prep.bind_count,
                        )
                        conn._send_and_receive(packet)
                        assert packet.total_tuple_count == 1
                    assert count_rows() == 0
                finally:
                    conn._send_and_receive(CloseQueryPacket(prep.query_handle))
                assert count_rows() == 0
                conn.commit()
                assert count_rows() == 2
            finally:
                if created:
                    conn.rollback()
                    cleanup = conn.cursor()
                    try:
                        cleanup.execute(f"DROP TABLE {table}")
                    finally:
                        cleanup.close()
                    conn.commit()
