"""Cached native column information without row movement or wire requests (#445)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from pycubrid.compat import native
from pycubrid.exceptions import DatabaseError, InterfaceError, OperationalError, ProgrammingError
from pycubrid.protocol import ColumnMetaData, ExecutePacket

from .test_compat_prepared import DSN, FakeDriver


@pytest.fixture
def owned(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[native.connection, FakeDriver]]:
    monkeypatch.setattr(native, "_DriverConnection", FakeDriver)
    conn = native.connect(DSN)
    yield conn, conn._driver
    conn.close()


def _columns() -> list[ColumnMetaData]:
    first = ColumnMetaData(
        column_type=16,
        scale=2,
        precision=17,
        name="별칭",
        real_name="",
        table_name="실제",
        default_value="NULL",
        is_nullable=False,
        is_auto_increment=True,
        is_primary_key=True,
        is_foreign_key=True,
        is_reverse_unique=True,
    )
    first._cci_type = 40
    second = ColumnMetaData(column_type=34, name="json", is_nullable=True, is_shared=True)
    second._cci_type = 130
    return [first, second]


def _executed(conn: native.connection, *, rows: bool = True) -> native.cursor:
    cur = conn.cursor()
    cur.prepare("SELECT 1")
    cur._columns = _columns()
    cur.execute()
    if not rows:
        cur._rows = []
        cur._fetched_count = cur._total_tuple_count = 0
    return cur


def _snapshot(cur: native.cursor, driver: FakeDriver) -> tuple[object, ...]:
    return (
        tuple(driver.requests),
        tuple(cur._rows),
        cur._row_index,
        cur._fetched_count,
        cur._total_tuple_count,
        cur._has_result,
        cur._result_invalidated,
        cur._handle,
        cur._generation,
    )


def test_all_and_one_are_exact_immutable_metadata_and_preserve_rows(
    owned: tuple[native.connection, FakeDriver],
) -> None:
    conn, driver = owned
    cur = _executed(conn)
    before = _snapshot(cur, driver)
    expected = (
        (40, 1, 2, 17, "별칭", "", "실제", "NULL", 1, 0, 1, 1, 0, 1, 0),
        (130, 0, -1, -1, "json", "", "", "", 0, 0, 0, 0, 0, 0, 1),
    )
    assert cur.result_info() == expected
    assert cur.result_info(0) == expected
    assert cur.result_info(1) == expected[:1]
    assert cur.result_info(2) == expected[1:]
    for column in cur.result_info():
        assert type(column) is tuple
        assert all(type(column[i]) is int for i in (0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14))
    assert _snapshot(cur, driver) == before
    assert cur.fetch_row() == (None,)


@pytest.mark.parametrize("prepared", [False, True])
def test_fresh_and_prepare_only_are_invalid(owned, prepared: bool) -> None:
    conn, driver = owned
    cur = conn.cursor()
    if prepared:
        cur.prepare("SELECT 1")
        cur._columns = _columns()
    before = _snapshot(cur, driver)
    with pytest.raises(InterfaceError) as raised:
        cur.result_info()
    assert raised.value.code == -30006
    assert _snapshot(cur, driver) == before


def test_zero_rows_keep_columns_and_zero_native_type_is_valid(owned) -> None:
    conn, _ = owned
    cur = _executed(conn, rows=False)
    cur._columns[0]._cci_type = 0
    assert cur.result_info(1)[0][0] == 0
    assert cur.result_info(1)[0][5] == ""


@pytest.mark.parametrize("selector", [0, -1, 99, -(2**31), 2**31 - 1])
def test_successful_dml_returns_none_before_index_bounds(owned, selector: int) -> None:
    conn, _ = owned
    cur = conn.cursor()
    cur.prepare("INSERT INTO t VALUES (1)")
    cur.execute()
    assert cur.result_info(selector) is None


@pytest.mark.parametrize("selector", [-1, 3])
def test_column_range_errors_keep_local_state(owned, selector: int) -> None:
    conn, driver = owned
    cur = _executed(conn)
    before = _snapshot(cur, driver)
    with pytest.raises(InterfaceError) as raised:
        cur.result_info(selector)
    assert raised.value.code == -30006
    assert _snapshot(cur, driver) == before


@pytest.mark.parametrize("selector", [None, "1", 1.0, object()])
def test_nonindex_arguments_fail_even_without_metadata(owned, selector: object) -> None:
    conn, _ = owned
    with pytest.raises(TypeError):
        conn.cursor().result_info(selector)


@pytest.mark.parametrize("selector", [-(2**31) - 1, 2**31])
def test_c_int_overflow_precedes_metadata_validation(owned, selector: int) -> None:
    conn, _ = owned
    with pytest.raises(OverflowError):
        conn.cursor().result_info(selector)


def test_boolean_and_index_conversion_are_supported_but_int_only_is_not(owned) -> None:
    conn, _ = owned
    cur = _executed(conn)

    class Index:
        def __index__(self) -> int:
            return 2

    class IntOnly:
        def __int__(self) -> int:
            return 1

    assert cur.result_info(False) == cur.result_info()
    assert cur.result_info(True) == cur.result_info(1)
    assert cur.result_info(Index()) == cur.result_info(2)
    with pytest.raises(TypeError):
        cur.result_info(IntOnly())


def test_arity_keywords_and_closed_precedence(owned) -> None:
    conn, _ = owned
    cur = conn.cursor()
    with pytest.raises(TypeError):
        cur.result_info(0, 1)
    with pytest.raises(TypeError):
        cur.result_info(n=0)
    cur.close()
    for args in [(), (None,), (0, 1)]:
        with pytest.raises(InterfaceError) as raised:
            cur.result_info(*args)
        assert raised.value.code == -30019
        assert raised.value.args == (raised.value.msg,)
    with pytest.raises(TypeError):
        cur.result_info(n=0)


@pytest.mark.parametrize("boundary", ["foreign", "generation", "disconnected"])
def test_invalid_owner_rejection_does_not_invalidate_rows(owned, boundary: str) -> None:
    conn, driver = owned
    cur = _executed(conn)
    replacement = None
    if boundary == "foreign":
        replacement = FakeDriver(**driver.options)
        conn._driver = replacement
    elif boundary == "generation":
        driver._physical_generation += 1
    else:
        driver._connected = False
    before = _snapshot(cur, driver)
    try:
        with pytest.raises(InterfaceError):
            cur.result_info()
        assert _snapshot(cur, driver) == before
        if replacement is not None:
            assert replacement.requests == []
    finally:
        conn._driver = driver
        driver._physical_generation = 1
        driver._connected = True


@pytest.mark.parametrize("effect", ["close", "foreign", "generation", "prepare"])
def test_index_callback_rechecks_closed_and_owner_state(owned, effect: str) -> None:
    conn, driver = owned
    cur = _executed(conn)

    class MutatingIndex:
        def __index__(self) -> int:
            if effect == "close":
                cur.close()
            elif effect == "foreign":
                conn._driver = FakeDriver(**driver.options)
            elif effect == "generation":
                driver._physical_generation += 1
            else:
                cur.prepare("SELECT 2")
            return 0

    try:
        with pytest.raises(InterfaceError):
            cur.result_info(MutatingIndex())
    finally:
        conn._driver = driver
        driver._physical_generation = 1


def test_index_callback_exception_is_propagated(owned) -> None:
    conn, _ = owned
    cur = _executed(conn)

    class BrokenIndex:
        def __index__(self) -> int:
            raise RuntimeError("index failed")

    with pytest.raises(RuntimeError, match="index failed"):
        cur.result_info(BrokenIndex())


def test_unknown_type_is_not_inferred_from_normalized_type(owned) -> None:
    conn, driver = owned
    cur = _executed(conn)
    cur._columns[0]._cci_type = None
    before = _snapshot(cur, driver)
    with pytest.raises(InterfaceError):
        cur.result_info(1)
    assert _snapshot(cur, driver) == before


@pytest.mark.parametrize("boundary", ["commit", "rollback", "eof"])
def test_row_only_boundaries_keep_same_owner_metadata(owned, boundary: str) -> None:
    conn, _ = owned
    cur = _executed(conn)
    before = cur.result_info()
    if boundary == "eof":
        while cur.fetch_row() is not None:
            pass
    else:
        getattr(conn, boundary)()
    assert cur.result_info() == before
    if boundary == "rollback":
        with pytest.raises(InterfaceError):
            cur.fetch_row()


def test_local_preflight_preserves_but_attempted_execute_failure_hides_metadata(owned) -> None:
    conn, driver = owned
    cur = _executed(conn)
    before = cur.result_info()
    with pytest.raises(ProgrammingError):
        cur.execute(option=1)
    assert cur.result_info() == before
    error = DatabaseError("server failure", code=-493)
    setattr(error, "_cas_server_error", True)
    driver.fail_packet_type, driver.fail_packet_error = ExecutePacket, error
    with pytest.raises(DatabaseError):
        cur.execute()
    with pytest.raises(InterfaceError):
        cur.result_info()
    driver.fail_packet_type = driver.fail_packet_error = None
    cur.execute()
    # Deferred reprepare adopts a new statement; fake driver has no columns there.
    assert cur.result_info() is None


def test_failed_result_adoption_never_marks_metadata_successful(owned) -> None:
    conn, driver = owned
    cur = _executed(conn)
    original = driver._send_and_receive

    def bad_reply(packet, **kwargs):
        result = original(packet, **kwargs)
        if isinstance(packet, ExecutePacket):
            packet.total_tuple_count = 0
        return result

    driver._send_and_receive = bad_reply
    with pytest.raises(OperationalError):
        cur.execute()
    with pytest.raises(InterfaceError):
        cur.result_info()


def test_explicit_reprepare_hides_old_metadata_until_success(owned) -> None:
    conn, _ = owned
    cur = _executed(conn)
    cur.prepare("SELECT 2")
    with pytest.raises(InterfaceError):
        cur.result_info()
    cur._columns = _columns()[1:]
    cur.execute()
    assert cur.result_info()[0][4] == "json"
