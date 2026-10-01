"""Typed collection parameters for ordinary cursors (#567).

``pycubrid.types.Set``, ``Multiset`` and ``Sequence`` render as ``SET{...}``,
``MULTISET{...}`` and ``SEQUENCE{...}`` literals. Every element goes through
the same override-proof renderer as a scalar parameter (#518, #528), so a
hostile element subclass cannot change the SQL text.
"""

from __future__ import annotations

import copy
import datetime
import pickle
from decimal import Decimal
from typing import Any, cast

import pytest

import pycubrid
from pycubrid._cursor_common import bind_parameters, format_parameter
from pycubrid.exceptions import ProgrammingError
from pycubrid.types import Multiset, Sequence, Set

from .test_param_security import (
    _HostileByteArray,
    _HostileBytes,
    _HostileDate,
    _HostileDateTime,
    _HostileDecimal,
    _HostileFloat,
    _HostileInt,
    _HostileStr,
    _HostileTime,
    _spoof,
)

_KINDS = [(Set, "SET"), (Multiset, "MULTISET"), (Sequence, "SEQUENCE")]


class TestConstruction:
    def test_exported(self) -> None:
        assert pycubrid.Set is Set
        assert pycubrid.Multiset is Multiset
        assert pycubrid.Sequence is Sequence
        assert {"Set", "Multiset", "Sequence"} <= set(pycubrid.__all__)
        assert Sequence.__module__ == "pycubrid.types"

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    def test_wraps_a_tuple(self, kind: Any) -> None:
        source = [3, 1, 1]
        value = kind(source)
        source.append(9)
        assert value.elements == (3, 1, 1)
        assert type(value.elements) is tuple
        assert list(value) == [3, 1, 1]
        assert len(value) == 3
        assert kind().elements == ()
        assert kind(x for x in (1, 2)).elements == (1, 2)

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    def test_immutable(self, kind: Any) -> None:
        value = kind([1])
        with pytest.raises(AttributeError):
            value._elements = (2,)
        with pytest.raises(AttributeError):
            value.elements = (2,)
        with pytest.raises(AttributeError):
            del value._elements
        with pytest.raises(AttributeError):
            value.other = 1
        assert value.elements == (1,)

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    def test_reinvoking_init_does_not_mutate(self, kind: Any) -> None:
        # Construction happens in __new__; __init__ is a no-op, so calling it
        # again on a live instance cannot replace its elements or its hash
        # (#568 review).
        value = kind([1])
        original_hash = hash(value)
        value.__init__([9, 9, 9])
        assert value.elements == (1,)
        assert hash(value) == original_hash

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    @pytest.mark.parametrize("single", ["ab", b"ab", bytearray(b"ab")])
    def test_rejects_a_single_string_or_bytes(self, kind: Any, single: object) -> None:
        with pytest.raises(TypeError, match="iterable of elements"):
            kind(single)

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    def test_rejects_a_dict(self, kind: Any) -> None:
        # Iterating a dict yields only its keys; the values would otherwise
        # be silently dropped (#568 review).
        with pytest.raises(TypeError, match="does not accept a dict"):
            kind({"a": 1, "b": 2})

    @pytest.mark.parametrize("kind", [Set, Multiset])
    @pytest.mark.parametrize("unordered", [{1, 2}, frozenset({1, 2})])
    def test_set_and_multiset_accept_set_or_frozenset(self, kind: Any, unordered: object) -> None:
        assert set(kind(unordered).elements) == {1, 2}

    @pytest.mark.parametrize("unordered", [{1, 2}, frozenset({1, 2})])
    def test_sequence_rejects_a_set_or_frozenset(self, unordered: object) -> None:
        # A set/frozenset's iteration order is not guaranteed, which would
        # make Sequence's element order nondeterministic (#568 review).
        with pytest.raises(TypeError, match="does not accept a set/frozenset"):
            Sequence(unordered)

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    def test_cannot_be_subclassed(self, kind: Any) -> None:
        with pytest.raises(TypeError, match="cannot be subclassed"):
            type("Sub", (kind,), {})

    def test_equality_hash_and_repr(self) -> None:
        assert Set([1, 2]) == Set((1, 2))
        assert Set([1, 2]) != Set([2, 1])
        assert Set([1]) != Multiset([1])
        assert Multiset([1]) != Sequence([1])
        assert Set([1]) != (1,)
        assert hash(Set([1, 2])) == hash(Set((1, 2)))
        assert len({Set([1]), Set([1]), Sequence([1])}) == 2
        assert repr(Multiset(["a", 1])) == "Multiset(['a', 1])"

    def test_is_not_a_parameter_sequence(self) -> None:
        # A typed collection is one parameter, never the parameters sequence.
        with pytest.raises(ProgrammingError, match="parameters must be a sequence"):
            bind_parameters("SELECT ?", cast(Any, Sequence([1])))


class TestRendering:
    @pytest.mark.parametrize("kind, keyword", _KINDS)
    def test_keyword_and_elements(self, kind: Any, keyword: str) -> None:
        assert format_parameter(kind([3, 1, 1])) == "%s{3, 1, 1}" % keyword
        assert format_parameter(kind()) == "%s{}" % keyword

    @pytest.mark.parametrize("kind, keyword", _KINDS)
    @pytest.mark.parametrize("no_backslash_escapes", [True, False])
    def test_each_element_renders_like_a_scalar(
        self, kind: Any, keyword: str, no_backslash_escapes: bool
    ) -> None:
        elements = [
            None,
            True,
            7,
            -(10**20),
            1.5,
            1e-07,
            Decimal("12.50"),
            "it's \\ here",
            b"\x00\xff",
            bytearray(b"\xca\xfe"),
            datetime.date(99, 1, 2),
            datetime.time(1, 2, 3),
            datetime.datetime(2024, 1, 2, 3, 4, 5, 123456),
            datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone(datetime.timedelta(hours=9))),
        ]
        expected = ", ".join(
            format_parameter(e, no_backslash_escapes=no_backslash_escapes) for e in elements
        )
        assert format_parameter(
            kind(elements), no_backslash_escapes=no_backslash_escapes
        ) == "%s{%s}" % (keyword, expected)

    def test_backslash_mode_reaches_elements(self) -> None:
        value = Sequence(["a\\b"])
        assert format_parameter(value, no_backslash_escapes=True) == "SEQUENCE{'a\\b'}"
        assert format_parameter(value, no_backslash_escapes=False) == "SEQUENCE{'a\\\\b'}"

    def test_bind_parameters(self) -> None:
        sql = bind_parameters(
            "INSERT INTO t VALUES (?, ?, ?, ?)",
            (Set([1, 2]), Multiset(["a", "a"]), Sequence([3, 1, 2]), 5),
        )
        assert sql == ("INSERT INTO t VALUES (SET{1, 2}, MULTISET{'a', 'a'}, SEQUENCE{3, 1, 2}, 5)")

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    @pytest.mark.parametrize("inner", [Set([1]), Multiset([1]), Sequence([1])])
    def test_rejects_nested_typed_collections(self, kind: Any, inner: object) -> None:
        with pytest.raises(ProgrammingError, match="nested collection"):
            format_parameter(kind([1, inner]))

    @pytest.mark.parametrize("inner", [[1], (1,), {1}, frozenset({1}), {"a": 1}])
    def test_rejects_plain_containers_as_elements(self, inner: object) -> None:
        with pytest.raises(ProgrammingError, match="cannot bind a collection"):
            format_parameter(Set([inner]))

    @pytest.mark.parametrize("bad", [object(), float("nan"), Decimal("Infinity"), "a\x00b"])
    def test_rejects_unsupported_elements(self, bad: object) -> None:
        with pytest.raises(ProgrammingError):
            format_parameter(Sequence([1, bad]))

    @pytest.mark.parametrize("plain", [[1, 2], (1, 2), {1, 2}, frozenset({1, 2})])
    def test_plain_containers_stay_rejected(self, plain: object) -> None:
        with pytest.raises(ProgrammingError, match="pycubrid.types.Set, Multiset or Sequence"):
            format_parameter(plain)


class TestHostileElements:
    """Elements render from their stored values, never from overridden methods."""

    @pytest.mark.parametrize("kind, keyword", _KINDS)
    @pytest.mark.parametrize("no_backslash_escapes", [True, False])
    def test_hostile_subclass_elements(
        self, kind: Any, keyword: str, no_backslash_escapes: bool
    ) -> None:
        elements = [
            _HostileInt(1),
            _HostileFloat(2.5),
            _HostileDecimal("3.25"),
            _HostileStr("it's"),
            _HostileBytes(b"\x01"),
            _HostileByteArray(b"\x02"),
            _HostileDate(2024, 1, 2),
            _HostileTime(3, 4, 5),
            _HostileDateTime(2024, 1, 2, 3, 4, 5),
        ]
        rendered = format_parameter(kind(elements), no_backslash_escapes=no_backslash_escapes)
        assert rendered == (
            "%s{1, 2.5, 3.25, 'it''s', X'01', X'02', DATE'2024-01-02', TIME'03:04:05', "
            "DATETIME'2024-01-02 03:04:05.000'}" % keyword
        )
        assert "DROP" not in rendered

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    def test_spoofed_class_is_not_a_collection(self, kind: Any) -> None:
        with pytest.raises(ProgrammingError, match="unsupported parameter type"):
            format_parameter(_spoof(kind))

    def test_forged_slot_contents_are_rejected(self) -> None:
        value = Set([1])
        object.__setattr__(value, "_elements", ["'; DROP TABLE t; --"])
        with pytest.raises(ProgrammingError, match="must be a tuple"):
            format_parameter(value)

    def test_hostile_iterable_is_copied_once_at_construction(self) -> None:
        class _Shifty(list[Any]):
            calls = 0

            def __iter__(self) -> Any:
                type(self).calls += 1
                return iter([1] if type(self).calls == 1 else ["'; DROP TABLE t; --"])

        value = Sequence(_Shifty())
        assert format_parameter(value) == "SEQUENCE{1}"
        assert format_parameter(value) == "SEQUENCE{1}"

    def test_private_base_subclass_is_not_rendered(self) -> None:
        base = Set.__mro__[1]
        forged = type("Set", (base,), {"__slots__": ()})
        with pytest.raises(ProgrammingError, match="unsupported parameter type"):
            format_parameter(forged([1]))

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    def test_uninitialized_instance_raises_programming_error(self, kind: Any) -> None:
        # object.__new__(kind) bypasses _Collection.__new__ entirely, so the
        # _elements slot is never set. format_parameter() must not leak the
        # resulting AttributeError (#568 review).
        broken = object.__new__(kind)
        with pytest.raises(ProgrammingError, match="missing its elements"):
            format_parameter(broken)


class TestCopyAndPickle:
    """copy.copy, copy.deepcopy and pickle must round-trip (#568 review)."""

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    def test_copy_returns_the_same_immutable_instance(self, kind: Any) -> None:
        value = kind([1, 2])
        assert copy.copy(value) is value

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    def test_deepcopy_with_immutable_elements_returns_the_same_instance(self, kind: Any) -> None:
        # Every element here is itself immutable, so copy.deepcopy() of the
        # elements tuple hands back that same tuple and this is a no-op.
        value = kind([1, 2])
        assert copy.deepcopy(value) is value

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    def test_deepcopy_independently_copies_a_mutable_bytearray_element(self, kind: Any) -> None:
        # bytearray is mutable (#568 review): deepcopy must not alias it,
        # or mutating the copy would silently mutate the "immutable"
        # original's stored element too.
        original_bytes = bytearray(b"ab")
        value = kind([original_bytes])
        restored = copy.deepcopy(value)
        assert restored is not value
        assert restored == value
        assert restored.elements[0] is not value.elements[0]
        restored.elements[0][0] = 0
        assert value.elements[0] == original_bytes

    @pytest.mark.parametrize("kind", [Set, Multiset, Sequence])
    @pytest.mark.parametrize("protocol", range(pickle.HIGHEST_PROTOCOL + 1))
    def test_pickle_round_trips(self, kind: Any, protocol: int) -> None:
        value = kind([3, 1, 1])
        restored = pickle.loads(pickle.dumps(value, protocol=protocol))
        assert restored == value
        assert restored.elements == value.elements
        assert hash(restored) == hash(value)
        assert type(restored) is kind
