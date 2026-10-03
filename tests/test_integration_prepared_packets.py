"""Internal FC2/FC3 scalar wire smoke; not a public prepared API (#475)."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

import pycubrid
from pycubrid.constants import CCIPrepareOption
from pycubrid.exceptions import OperationalError
from pycubrid.protocol import (
    CloseQueryPacket,
    ExecutePacket,
    PreparePacket,
    _encode_prepared_scalar,
)

from ._parity_helpers import connect_kwargs, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]


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
        generation = conn._physical_generation
        prep = PreparePacket(sql, auto_commit=autocommit, prepare_flag=CCIPrepareOption.HOLDABLE)
        conn._send_and_receive(prep, expected_generation=generation)
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
                conn._send_and_receive(packet, expected_generation=generation)
                assert packet.rows == [row]
                assert conn._physical_generation == generation
        finally:
            conn._send_and_receive(
                CloseQueryPacket(prep.query_handle), expected_generation=generation
            )


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
                generation = conn._physical_generation
                conn._send_and_receive(prep, expected_generation=generation)
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
                        conn._send_and_receive(packet, expected_generation=generation)
                        assert packet.total_tuple_count == 1
                    assert count_rows() == 0
                finally:
                    conn._send_and_receive(
                        CloseQueryPacket(prep.query_handle), expected_generation=generation
                    )
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


def test_prepared_generation_rejects_old_handle_after_live_reconnect() -> None:
    with pycubrid.connect(**connect_kwargs()) as conn:
        generation = conn._physical_generation
        prep = PreparePacket("SELECT CAST(? AS INTEGER)", prepare_flag=CCIPrepareOption.HOLDABLE)
        conn._send_and_receive(prep, expected_generation=generation)
        packet = ExecutePacket(
            prep.query_handle,
            prep.statement_type,
            protocol_version=conn._protocol_version,
            bindings=(_encode_prepared_scalar(42),),
            bind_count=prep.bind_count,
        )
        packet.columns = prep.columns
        conn._send_and_receive(packet, expected_generation=generation)
        assert packet.rows == [(42,)]

        conn._drop_connection()
        healthy = conn.ping(reconnect=True)
        assert healthy is True
        assert conn._physical_generation == generation + 1
        with pytest.raises(OperationalError, match="earlier physical session"):
            conn._send_and_receive(
                CloseQueryPacket(prep.query_handle), expected_generation=generation
            )

        cursor = conn.cursor()
        try:
            cursor.execute("SELECT 1")
            row = cursor.fetchone()
            assert row == (1,)
        finally:
            cursor.close()
