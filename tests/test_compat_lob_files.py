"""Staged native LOB file transfers over the existing byte-valued broker fake."""

from __future__ import annotations

import io
from collections.abc import Iterator
from pathlib import Path

import pytest

from pycubrid.compat import native
from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import DatabaseError, InterfaceError, OperationalError
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

    result = holder.imports(str(source), kind)
    assert result is None
    assert holder._lob_type == lob_type and holder._handle != old_handle
    position = holder.seek(0)
    assert position == 17
    assert cur._bindings[0].packed_handle == old_handle
    assert driver.values[holder._handle[16:]] == data
    result = holder.export(str(output))
    assert result is None
    position = holder.seek(0)
    assert output.read_bytes() == data and position == 17
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
    result = holder.imports(str(source))
    assert result is None
    assert holder._handle is not None
    holder.seek(99, native.SEEK_SET)
    result = holder.export(str(output))
    assert result is None
    position = holder.seek(0)
    assert output.read_bytes() == b"" and position == 99
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
    position = holder.seek(0)
    assert holder._state is state and position == 3
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
    position = holder.seek(0)
    assert (holder._state, position, conn._driver.requests) == before


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
    position = holder.seek(0)
    assert holder._state is state and position == 4


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


@pytest.mark.parametrize("phase", ["read", "close"])
def test_late_input_os_failure_closes_input_without_adopting(
    conn: native.connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    holder = conn.lob()
    holder.write(b"old")
    state, position = holder._state, holder._position
    error = OSError("owned input failure")

    class Input(io.BytesIO):
        def read(self, size: int = -1) -> bytes:
            assert 0 < size <= 64 * 1024
            if phase == "read" and self.tell() > 0:
                raise error
            return super().read(size)

        def close(self) -> None:
            super().close()
            if phase == "close":
                raise error

    reader = Input(b"x" * (64 * 1024 + 1))
    monkeypatch.setattr(native, "open", lambda *_args: reader, raising=False)
    with pytest.raises(InterfaceError) as raised:
        holder.imports(str(tmp_path / "source"))
    assert raised.value.code == -30016 and raised.value.__cause__ is error
    assert reader.closed and holder._state is state and holder._position == position


@pytest.mark.parametrize("phase", ["short", "write", "flush", "close"])
def test_output_failure_preserves_destination_and_closes_temp(
    conn: native.connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    holder = conn.lob()
    holder.write(b"raw\x00\xff")
    output = tmp_path / "out"
    output.write_bytes(b"old")
    error = OSError("owned output failure")
    real_fdopen = native.os.fdopen
    streams: list[object] = []

    class Output:
        def __init__(self, fd: int, mode: str) -> None:
            self.inner = real_fdopen(fd, mode)
            streams.append(self.inner)

        def write(self, chunk: bytes) -> int:
            if phase == "write":
                raise error
            return self.inner.write(chunk[:1] if phase == "short" else chunk)

        def flush(self) -> None:
            if phase == "flush":
                raise error
            self.inner.flush()

        def close(self) -> None:
            self.inner.close()
            if phase == "close":
                raise error

    monkeypatch.setattr(native.os, "fdopen", Output)
    with pytest.raises(InterfaceError) as raised:
        holder.export(str(output))
    assert raised.value.code == -30017
    if phase != "short":
        assert raised.value.__cause__ is error
    assert output.read_bytes() == b"old" and set(tmp_path.iterdir()) == {output}
    assert streams and all(stream.closed for stream in streams)


@pytest.mark.parametrize("reply", [(0, b""), (2, b"x"), (99, b"x" * 99), (True, b"x")])
def test_invalid_broker_progress_never_publishes(
    conn: native.connection, tmp_path: Path, reply: tuple[int, bytes]
) -> None:
    holder = conn.lob()
    holder.write(b"abc")
    conn._driver.read_plan = [reply]
    output = tmp_path / "out"
    output.write_bytes(b"old")
    with pytest.raises(OperationalError):
        holder.export(str(output))
    assert output.read_bytes() == b"old" and set(tmp_path.iterdir()) == {output}


def test_positive_short_reads_are_progress_not_eof(conn: native.connection, tmp_path: Path) -> None:
    holder = conn.lob()
    holder.write(b"abc")
    holder.seek(9, native.SEEK_SET)
    conn._driver.read_plan = [(1, b"a"), (1, b"b"), (1, b"c")]
    output = tmp_path / "out"
    holder.export(str(output))
    assert output.read_bytes() == b"abc" and holder._position == 9


@pytest.mark.parametrize("effect", ["position", "close", "generation"])
def test_input_close_reentry_is_rechecked_before_adoption(
    conn: native.connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, effect: str
) -> None:
    holder = conn.lob()
    holder.write(b"old")
    state = holder._state

    class Input(io.BytesIO):
        def close(self) -> None:
            if not self.closed:
                if effect == "position":
                    holder.seek(1, native.SEEK_SET)
                elif effect == "close":
                    holder.close()
                else:
                    conn._driver._physical_generation += 1
            super().close()

    reader = Input(b"new")
    monkeypatch.setattr(native, "open", lambda *_args: reader, raising=False)
    with pytest.raises(InterfaceError):
        holder.imports(str(tmp_path / "source"))
    assert reader.closed
    if effect != "close":
        assert holder._state is state
    if effect == "position":
        assert holder._position == 1  # callback's own change is not rolled back
    conn._driver._physical_generation = 1


def test_fdopen_failure_closes_owned_descriptor_and_removes_temp(
    conn: native.connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    holder = conn.lob()
    holder.write(b"data")
    descriptors: list[int] = []
    error = OSError("fdopen refused")

    def fail_fdopen(fd: int, _mode: str) -> None:
        descriptors.append(fd)
        raise error

    monkeypatch.setattr(native.os, "fdopen", fail_fdopen)
    with pytest.raises(InterfaceError) as raised:
        holder.export(str(tmp_path / "out"))
    assert raised.value.code == -30009 and raised.value.__cause__ is error
    assert len(descriptors) == 1
    with pytest.raises(OSError):
        native.os.fstat(descriptors[0])
    assert not list(tmp_path.iterdir())


def test_cleanup_failure_does_not_mask_primary_and_reports_owned_path(
    conn: native.connection,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    holder = conn.lob()
    holder.write(b"data")
    primary = OSError("replace blocked")
    real_unlink = native.os.unlink

    def fail_replace(_source: str, _destination: str) -> None:
        raise primary

    def fail_unlink(_path: str) -> None:
        raise OSError("owned unlink blocked")

    monkeypatch.setattr(native.os, "replace", fail_replace)
    monkeypatch.setattr(native.os, "unlink", fail_unlink)
    with pytest.raises(InterfaceError) as raised:
        holder.export(str(tmp_path / "out"))
    assert raised.value.__cause__ is primary
    leftovers = list(tmp_path.iterdir())
    assert len(leftovers) == 1 and str(leftovers[0]) in caplog.text
    real_unlink(leftovers[0])  # clean only the acknowledged test-owned leftover


@pytest.mark.parametrize("phase", ["flush", "close"])
def test_output_reentry_prevents_replace(
    conn: native.connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    holder = conn.lob()
    holder.write(b"new")
    output = tmp_path / "out"
    output.write_bytes(b"old")
    real_fdopen = native.os.fdopen

    class Output:
        def __init__(self, fd: int, mode: str) -> None:
            self.inner = real_fdopen(fd, mode)

        def write(self, chunk: bytes) -> int:
            return self.inner.write(chunk)

        def flush(self) -> None:
            self.inner.flush()
            if phase == "flush":
                holder.seek(1, native.SEEK_SET)

        def close(self) -> None:
            self.inner.close()
            if phase == "close":
                holder.seek(1, native.SEEK_SET)

    monkeypatch.setattr(native.os, "fdopen", Output)
    with pytest.raises(InterfaceError, match="lob changed"):
        holder.export(str(output))
    assert output.read_bytes() == b"old" and set(tmp_path.iterdir()) == {output}
    assert holder._position == 1


def test_missing_destination_parent_is_not_created(conn: native.connection, tmp_path: Path) -> None:
    holder = conn.lob()
    holder.write(b"new")
    before = list(conn._driver.requests)
    target = tmp_path / "missing-parent" / "out"
    with pytest.raises(InterfaceError) as raised:
        holder.export(str(target))
    assert raised.value.code == -30009 and not target.parent.exists()
    assert conn._driver.requests == before


def test_invalid_new_handle_retires_session_without_adopting(
    conn: native.connection, tmp_path: Path
) -> None:
    holder = conn.lob()
    holder.write(b"old")
    state = holder._state
    source = tmp_path / "source"
    source.write_bytes(b"new")
    conn._driver.bad_new_handle = True
    with pytest.raises(OperationalError, match="malformed"):
        holder.imports(str(source))
    assert holder._state is state and conn._driver.discarded


def test_relative_export_path_is_frozen_before_close_changes_cwd(
    conn: native.connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    holder = conn.lob()
    holder.write(b"new")
    other = tmp_path / "other"
    other.mkdir()
    monkeypatch.chdir(tmp_path)
    real_fdopen = native.os.fdopen

    class Output:
        def __init__(self, fd: int, mode: str) -> None:
            self.inner = real_fdopen(fd, mode)

        def write(self, chunk: bytes) -> int:
            return self.inner.write(chunk)

        def flush(self) -> None:
            self.inner.flush()

        def close(self) -> None:
            self.inner.close()
            monkeypatch.chdir(other)

    monkeypatch.setattr(native.os, "fdopen", Output)
    holder.export("out")
    assert (tmp_path / "out").read_bytes() == b"new" and not (other / "out").exists()


@pytest.mark.parametrize("kind", ["", "BC", "β", None, b"B"])
def test_type_validation_precedes_input_open(
    conn: native.connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: object
) -> None:
    holder = conn.lob()
    before = list(conn._driver.requests)
    monkeypatch.setattr(native, "open", lambda *_args: pytest.fail("must not open"), raising=False)
    if type(kind) is str:
        with pytest.raises(InterfaceError) as raised:
            holder.imports(str(tmp_path / "source"), kind)
        assert raised.value.code == -30006
    else:
        with pytest.raises(TypeError):
            holder.imports(str(tmp_path / "source"), kind)
    assert conn._driver.requests == before


@pytest.mark.parametrize("operation", ["imports", "export"])
def test_path_subclass_pathlike_and_nul_are_rejected_before_io(
    conn: native.connection, tmp_path: Path, operation: str
) -> None:
    class String(str):
        def __fspath__(self) -> str:
            pytest.fail("path coercion must not run")

    holder = conn.lob()
    before = list(conn._driver.requests)
    for path in (String("file"), tmp_path / "file"):
        with pytest.raises(TypeError):
            getattr(holder, operation)(path)
    with pytest.raises(ValueError):
        getattr(holder, operation)("file\x00")
    assert conn._driver.requests == before


def test_receiver_stream_overrides_are_not_transfer_hooks(
    conn: native.connection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"raw\x00\xff")
    output = tmp_path / "out"
    holder = conn.lob()
    holder.seek(13, native.SEEK_SET)
    for name in ("write", "read", "seek", "close"):
        monkeypatch.setattr(holder, name, lambda *_args: pytest.fail("receiver hook must not run"))
    holder.imports(str(source))
    holder.export(str(output))
    assert output.read_bytes() == b"raw\x00\xff" and holder._position == 13
