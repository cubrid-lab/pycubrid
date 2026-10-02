"""Deferred CLOSE flushes retain generation and failure boundaries (#585)."""

from __future__ import annotations

import asyncio
import gc
import logging
from typing import Any

import pytest

from pycubrid.exceptions import OperationalError
from pycubrid.protocol import CloseQueryPacket

from .helpers.replay_broker import OUT_TRAN, Reply, Request, Session, error_body, ok_body
from .test_deferred_close import ASYNC, _Driver, _run

BOUNDARY = pytest.mark.parametrize("boundary", ["commit", "rollback"])


@ASYNC
@BOUNDARY
@pytest.mark.parametrize("phase", ["before-second-send", "after-first-reply"])
@pytest.mark.parametrize("followup", ["boundary", "fc41"])
def test_interrupt_preserves_only_unsent_deferred_closes(
    asynchronous: bool, boundary: str, phase: str, followup: str
) -> None:
    def steps(d: _Driver) -> list[int]:
        handles = _queue_three(d)
        generation = d.conn._physical_generation
        if phase == "before-second-send":
            if asynchronous:
                original = d.conn._close_handle_at_boundary_locked
                calls = 0

                async def interrupt_before_second(handle: int) -> None:
                    nonlocal calls
                    calls += 1
                    if calls == 2:
                        raise asyncio.CancelledError
                    await original(handle)

                d.conn._close_handle_at_boundary_locked = interrupt_before_second
            else:
                original = d.conn._close_handle_at_boundary
                calls = 0

                def interrupt_before_second(handle: int) -> None:
                    nonlocal calls
                    calls += 1
                    if calls == 2:
                        raise KeyboardInterrupt
                    original(handle)

                d.conn._close_handle_at_boundary = interrupt_before_second
            restore_name = (
                "_close_handle_at_boundary_locked" if asynchronous else "_close_handle_at_boundary"
            )
        elif asynchronous:
            original = d.conn._send_and_receive_locked
            interrupted = False

            async def interrupt_after_reply(packet: Any, **kwargs: Any) -> Any:
                nonlocal interrupted
                result = await original(packet, **kwargs)
                if isinstance(packet, CloseQueryPacket) and not interrupted:
                    interrupted = True
                    raise asyncio.CancelledError
                return result

            d.conn._send_and_receive_locked = interrupt_after_reply
            restore_name = "_send_and_receive_locked"
        else:
            original = d.conn._send_and_receive
            interrupted = False

            def interrupt_after_reply(packet: Any, **kwargs: Any) -> Any:
                nonlocal interrupted
                result = original(packet, **kwargs)
                if isinstance(packet, CloseQueryPacket) and not interrupted:
                    interrupted = True
                    raise KeyboardInterrupt
                return result

            d.conn._send_and_receive = interrupt_after_reply
            restore_name = "_send_and_receive"

        try:
            with pytest.raises(asyncio.CancelledError if asynchronous else KeyboardInterrupt):
                d.run(getattr(d.conn, boundary)())
        finally:
            setattr(d.conn, restore_name, original)
        assert d.conn._connected
        assert d.conn._physical_generation == generation
        assert d.conn._deferred_closes == [(generation, handles[1]), (generation, handles[2])]
        if followup == "boundary":
            d.run(getattr(d.conn, boundary)())
        else:
            fresh = d.cursor()
            d.select(fresh)  # FC41 carries the same two queued IDs.
        assert d.conn._deferred_closes == []
        return handles

    requests, handles = _run(asynchronous, steps)
    closes = [r for r in requests if r.function == "CLOSE_REQ_HANDLE"]
    if followup == "boundary":
        assert [r.int_arg(0) for r in closes] == handles
        assert {r.session for r in closes} == {0}
        assert len([r for r in requests if r.function == "END_TRAN"]) == 1
        _assert_boundary(requests, boundary)
    else:
        followup_index = max(
            index
            for index, request in enumerate(requests)
            if request.function == "PREPARE_AND_EXECUTE"
        )
        followup_request = requests[followup_index]
        assert followup_request.deferred_closes == tuple(handles[1:])
        assert [
            r.int_arg(0) for r in requests[:followup_index] if r.function == "CLOSE_REQ_HANDLE"
        ] == [handles[0]]
        assert all(r.function != "END_TRAN" for r in requests[: followup_index + 1])


@ASYNC
@BOUNDARY
def test_gc_append_during_flush_stays_behind_original_batch(
    asynchronous: bool, boundary: str
) -> None:
    def steps(d: _Driver) -> tuple[list[int], int]:
        late_cursor = d.cursor()
        d.select(late_cursor)
        late_handle = int(late_cursor._query_handle)
        # Model collection after the active-cursor snapshot but during the
        # queued flush; the weak registry entry disappears before __del__.
        d.conn._cursors.discard(late_cursor)
        late_cursor._cycle = late_cursor
        held = [late_cursor]
        del late_cursor
        handles = _queue_three(d)
        generation = d.conn._physical_generation
        calls = 0
        if asynchronous:
            original = d.conn._close_handle_at_boundary_locked

            async def append_after_first(handle: int) -> None:
                nonlocal calls
                await original(handle)
                calls += 1
                if calls == 1:
                    held.clear()
                    gc.collect()

            d.conn._close_handle_at_boundary_locked = append_after_first
            restore_name = "_close_handle_at_boundary_locked"
        else:
            original = d.conn._close_handle_at_boundary

            def append_after_first(handle: int) -> None:
                nonlocal calls
                original(handle)
                calls += 1
                if calls == 1:
                    held.clear()
                    gc.collect()

            d.conn._close_handle_at_boundary = append_after_first
            restore_name = "_close_handle_at_boundary"
        try:
            d.run(getattr(d.conn, boundary)())
        finally:
            setattr(d.conn, restore_name, original)
        assert d.conn._deferred_closes == [(generation, late_handle)]
        d.run(getattr(d.conn, boundary)())
        assert d.conn._deferred_closes == []
        return handles, late_handle

    requests, (handles, late_handle) = _run(asynchronous, steps)
    closes = [r.int_arg(0) for r in requests if r.function == "CLOSE_REQ_HANDLE"]
    assert closes == [*handles, late_handle]
    assert len([r for r in requests if r.function == "END_TRAN"]) == 2


@ASYNC
@BOUNDARY
def test_same_generation_queue_clear_during_flush_stops_cleanly(
    asynchronous: bool, boundary: str
) -> None:
    def steps(d: _Driver) -> list[int]:
        handles = _queue_three(d)
        generation = d.conn._physical_generation
        calls = 0
        if asynchronous:
            original = d.conn._close_handle_at_boundary_locked

            async def clear_after_first(handle: int) -> None:
                nonlocal calls
                await original(handle)
                calls += 1
                if calls == 1:
                    d.conn._deferred_closes.clear()

            d.conn._close_handle_at_boundary_locked = clear_after_first
        else:
            original = d.conn._close_handle_at_boundary

            def clear_after_first(handle: int) -> None:
                nonlocal calls
                original(handle)
                calls += 1
                if calls == 1:
                    d.conn._deferred_closes.clear()

            d.conn._close_handle_at_boundary = clear_after_first
        try:
            d.run(getattr(d.conn, boundary)())
        finally:
            if asynchronous:
                d.conn._close_handle_at_boundary_locked = original
            else:
                d.conn._close_handle_at_boundary = original
        assert d.conn._physical_generation == generation
        assert d.conn._deferred_closes == []
        return handles

    requests, handles = _run(asynchronous, steps)
    closes = [r.int_arg(0) for r in requests if r.function == "CLOSE_REQ_HANDLE"]
    assert closes == handles[:1]
    assert len([r for r in requests if r.function == "END_TRAN"]) == 1


def _queue_three(d: _Driver) -> list[int]:
    cursors = [d.cursor() for _ in range(3)]
    for cursor in cursors:
        d.select(cursor)
    handles = [int(cursor._query_handle) for cursor in cursors]
    for cursor in cursors:
        d.close(cursor)
    assert len(d.conn._deferred_closes) == 3
    return handles


def _assert_boundary(requests: list[Request], boundary: str) -> None:
    assert requests[-1].function == "END_TRAN"
    assert requests[-1].args == (b"\x01" if boundary == "commit" else b"\x02",)


@ASYNC
@BOUNDARY
def test_boundary_flush_filters_collected_handles_from_an_old_generation(
    asynchronous: bool,
    boundary: str,
) -> None:
    def steps(d: _Driver) -> tuple[int, int]:
        old = d.cursor()
        d.select(old)
        old_handle = int(old._query_handle)
        old_generation = d.conn._physical_generation
        # Cycle collection removes weak registry entries before __del__; a
        # reconnect in between cannot clear this cursor's old local handle.
        d.conn._cursors.discard(old)
        d.conn._drop_connection()
        d.connect()
        live, queued = d.cursor(), d.cursor()
        d.select(live)
        d.select(queued)
        assert live._query_handle == old_handle  # lowest-free ID reused
        queued_handle = int(queued._query_handle)
        d.close(queued)
        del old
        gc.collect()
        assert (old_generation, old_handle) in d.conn._deferred_closes
        assert old_generation + 1 == d.conn._physical_generation
        d.run(getattr(d.conn, boundary)())
        assert d.conn._connected
        assert d.conn._deferred_closes == []
        return old_handle, queued_handle

    requests, handles = _run(asynchronous, steps)
    closes = [r for r in requests if r.function == "CLOSE_REQ_HANDLE"]
    assert [r.int_arg(0) for r in closes] == list(handles)
    assert all(r.session == 1 for r in closes)
    _assert_boundary(requests, boundary)


@ASYNC
@BOUNDARY
def test_boundary_flush_stops_old_ids_after_mid_flush_reconnect(
    asynchronous: bool,
    boundary: str,
) -> None:
    def recycle(request: Request, session: Session) -> Reply | None:
        if session.number == 0 and request.function == "CLOSE_REQ_HANDLE":
            session.results.pop(request.int_arg(0), None)
            # The first close completes, but the next close's liveness probe
            # discovers that CAS recycled. Its old ID is skipped; the third
            # queued ID must also never reach the replacement session.
            return Reply(ok_body(OUT_TRAN), close=True)
        return None

    def steps(d: _Driver) -> list[int]:
        handles = _queue_three(d)
        generation = d.conn._physical_generation
        d.run(getattr(d.conn, boundary)())
        assert d.conn._connected
        assert d.conn._physical_generation == generation + 1
        assert d.conn._deferred_closes == []
        return handles

    requests, handles = _run(asynchronous, steps, extra=recycle)
    assert {r.session for r in requests} == {0, 1}
    closes = [r for r in requests if r.function == "CLOSE_REQ_HANDLE"]
    assert [(r.session, r.int_arg(0)) for r in closes] == [(0, handles[0])]
    replacement = [r for r in requests if r.session == 1]
    assert all(r.function != "PREPARE_AND_EXECUTE" for r in replacement)  # no SQL replay
    _assert_boundary(replacement, boundary)


@ASYNC
@BOUNDARY
def test_server_error_mid_flush_is_logged_and_remaining_closes_continue(
    asynchronous: bool,
    boundary: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def native_error(request: Request, session: Session) -> Reply | None:
        if (
            request.function == "CLOSE_REQ_HANDLE"
            and session.functions.count(request.function) == 2
        ):
            session.results.pop(request.int_arg(0), None)
            return Reply(error_body(OUT_TRAN, -10004, "invalid query handle"))
        return None

    def steps(d: _Driver) -> list[int]:
        handles = _queue_three(d)
        generation = d.conn._physical_generation
        d.run(getattr(d.conn, boundary)())
        assert d.conn._connected
        assert d.conn._physical_generation == generation
        assert d.conn._deferred_closes == []
        return handles

    with caplog.at_level(logging.DEBUG, logger="pycubrid"):
        requests, handles = _run(asynchronous, steps, extra=native_error)
    closes = [r for r in requests if r.function == "CLOSE_REQ_HANDLE"]
    assert [r.int_arg(0) for r in closes] == handles
    assert {r.session for r in requests} == {0}
    assert any(f"CLOSE_REQ for handle {handles[1]} failed" in r.message for r in caplog.records)
    _assert_boundary(requests, boundary)


@ASYNC
@BOUNDARY
def test_transport_error_mid_flush_aborts_without_end_or_replacement(
    asynchronous: bool,
    boundary: str,
) -> None:
    def lost_reply(request: Request, session: Session) -> Reply | None:
        if (
            request.function == "CLOSE_REQ_HANDLE"
            and session.functions.count(request.function) == 2
        ):
            return Reply(close=True)
        return None

    def steps(d: _Driver) -> list[int]:
        handles = _queue_three(d)
        generation = d.conn._physical_generation
        with pytest.raises(OperationalError, match="connection lost during receive"):
            d.run(getattr(d.conn, boundary)())
        assert not d.conn._connected
        assert d.conn._physical_generation == generation
        assert d.conn._deferred_closes == []
        return handles

    requests, handles = _run(asynchronous, steps, extra=lost_reply)
    closes = [r for r in requests if r.function == "CLOSE_REQ_HANDLE"]
    assert [r.int_arg(0) for r in closes] == handles[:2]
    assert all(r.function != "END_TRAN" for r in requests)
    assert {r.session for r in requests} == {0}
