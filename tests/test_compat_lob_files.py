"""Staged native LOB file transfers over the existing byte-valued broker fake."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from pycubrid.compat import native
from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import DatabaseError, InterfaceError
from pycubrid.protocol import LOBReadPacket, LOBWritePacket

from .test_compat_prepared import DSN
from .test_compat_lob_stream import StreamDriver


@pytest.fixture
def conn(monkeypatch: pytest.MonkeyPatch) -> Iterator[native.connection]:
    monkeypatch.setattr(native, "_DriverConnection", StreamDriver)
    owner = native.connect(DSN)
    owner._driver.read_cap = 64 * 1024
    yield owner
    owner.close()


@pytest.mark.parametrize(
    ("kind", "data", "lob_type"),
    [
        ("B", bytes(range(256)) * 513, CUBRIDDataType.BLOB),
        ("c", ("A한éB" * 19000).encode("utf-8"), CUBRIDDataType.CLOB),
    ],
    ids=["all-raw-bytes", "split-utf8-clob"],
)
def test_raw_multichunk_replacement_preserves_position_and_old_binding(
    conn: native.connection,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    data: bytes,
    lob_type: int,
) -> None:
    source = tmp_path / "input-한.bin"
    output = tmp_path / "output.bin"
    source.write_bytes(data)
    output.write_bytes(b"old output")
    holder = conn.lob()
    holder.write(b"old")
    old_handle = holder._handle
    cur = conn.cursor()
    cur.prepare("INSERT INTO owned_lob_fixture VALUES (?)")
    cur.bind_lob(1, holder)
    holder.seek(17, native.SEEK_SET)
    driver = conn._driver
    driver.requests.clear()
    monkeypatch.setattr(
        driver, "_check_reconnect", lambda: pytest.fail("transfer must not reconnect")
    )

    assert holder.imports(str(source), kind) is None
    assert holder._lob_type == lob_type and holder._handle != old_handle
    assert holder.seek(0) == 17
    assert cur._bindings[0].packed_handle == old_handle
    assert driver.values[holder._handle[16:]] == data
    assert holder.export(str(output)) is None
    assert output.read_bytes() == data and holder.seek(0) == 17
    assert set(tmp_path.iterdir()) == {source, output}
    writes = [packet for packet, _ in driver.requests if isinstance(packet, LOBWritePacket)]
    reads = [packet for packet, _ in driver.requests if isinstance(packet, LOBReadPacket)]
    assert len(writes) >= 3 and reads
    assert b"".join(packet.data for packet in writes) == data
    assert max(len(packet.data) for packet in writes) <= 64 * 1024
    assert reads[0].offset == 0 and max(packet.length for packet in reads) <= 64 * 1024


def test_empty_file_creates_a_value_and_exports_without_read_packets(
    conn: native.connection, tmp_path: Path
) -> None:
    source, output = tmp_path / "empty", tmp_path / "out"
    source.write_bytes(b"")
    output.write_bytes(b"replace me")
    holder = conn.lob()
    conn._driver.requests.clear()
    assert holder.imports(str(source)) is None
    assert holder._handle is not None
    holder.seek(99, native.SEEK_SET)
    assert holder.export(str(output)) is None
    assert output.read_bytes() == b"" and holder.seek(0) == 99
    assert not any(isinstance(p, (LOBWritePacket, LOBReadPacket)) for p, _ in conn._driver.requests)


def test_late_broker_import_failure_does_not_adopt_partial_replacement(
    conn: native.connection, tmp_path: Path
) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"x" * (128 * 1024 + 1))
    holder = conn.lob()
    holder.write(b"original")
    holder.seek(3, native.SEEK_SET)
    state = holder._state
    conn._driver.fail_write_call = conn._driver.write_calls + 2
    with pytest.raises(DatabaseError) as raised:
        holder.imports(str(source))
    assert raised.value.errno == -1016
    assert holder._state is state and holder.seek(0) == 3
    assert conn._driver.values[holder._handle[16:]] == b"original"


def test_missing_input_has_fixed_open_error_and_no_server_effects(
    conn: native.connection, tmp_path: Path
) -> None:
    holder = conn.lob()
    holder.write(b"old")
    before = holder._state, holder.seek(0), list(conn._driver.requests)
    with pytest.raises(InterfaceError) as raised:
        holder.imports(str(tmp_path / "missing"))
    assert raised.value.code == -30009 and raised.value.msg == "lob file open failed"
    assert raised.value.args == (raised.value.msg,)
    assert isinstance(raised.value.__cause__, OSError)
    assert (holder._state, holder.seek(0), conn._driver.requests) == before


def test_failed_replace_preserves_old_output_and_removes_owned_temp(
    conn: native.connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    holder = conn.lob()
    holder.write(b"replacement")
    holder.seek(4, native.SEEK_SET)
    output = tmp_path / "output"
    output.write_bytes(b"original output")
    state = holder._state
    error = OSError("replace refused")

    def fail_replace(_source: str, _destination: str) -> None:
        raise error

    monkeypatch.setattr(native.os, "replace", fail_replace)
    with pytest.raises(InterfaceError) as raised:
        holder.export(str(output))
    assert raised.value.code == -30017 and raised.value.msg == "lob file write failed"
    assert raised.value.__cause__ is error
    assert output.read_bytes() == b"original output"
    assert set(tmp_path.iterdir()) == {output}
    assert holder._state is state and holder.seek(0) == 4


@pytest.mark.parametrize("boundary", ["closed", "stale", "disconnected"])
@pytest.mark.parametrize("operation", ["imports", "export"])
def test_existing_value_session_fence_precedes_file_effects(
    conn: native.connection, tmp_path: Path, boundary: str, operation: str
) -> None:
    holder = conn.lob()
    holder.write(b"old")
    before = list(conn._driver.requests)
    if boundary == "closed":
        holder.close()
    elif boundary == "stale":
        conn._driver._physical_generation += 1
    else:
        conn._driver._connected = False
    target = tmp_path / "absent-parent" / "file"
    with pytest.raises(InterfaceError) as raised:
        getattr(holder, operation)(str(target))
    assert raised.value.code == 0
    assert conn._driver.requests == before and not target.exists()
    conn._driver._physical_generation = 1
    conn._driver._connected = True


def test_empty_export_fails_before_file_creation(conn: native.connection, tmp_path: Path) -> None:
    holder = conn.lob()
    output = tmp_path / "output"
    with pytest.raises(InterfaceError) as raised:
        holder.export(str(output))
    assert raised.value.code == -30018 and raised.value.msg == "lob has no value"
    assert not output.exists()


@pytest.mark.parametrize("operation", ["imports", "export"])
@pytest.mark.parametrize("path", [None, b"file", 1])
def test_non_string_paths_fail_without_file_or_server_effects(
    conn: native.connection, operation: str, path: object
) -> None:
    before = list(conn._driver.requests)
    with pytest.raises(TypeError):
        getattr(conn.lob(), operation)(path)
    assert conn._driver.requests == before
