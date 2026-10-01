# Parameter Binding

The driver-side parameter-binding contract for pycubrid 1.x.

This document is the **authoritative specification** for how pycubrid converts
Python values into SQL literals before sending them to CUBRID. Every claim in
this document is backed by a citation to the implementation
(`pycubrid/_cursor_common.py`) and to a unit test that pins the behavior. Any
change to the rules below is a contract change governed by
[`RELEASE_POLICY.md`](../RELEASE_POLICY.md).

---

## Table of Contents

- [Overview](#overview)
- [Placeholder Style](#placeholder-style)
- [Type Mapping (Guarantees)](#type-mapping-guarantees)
  - [Decimal parameters](#decimal-parameters)
  - [Typed collection parameters](#typed-collection-parameters)
- [String Escaping](#string-escaping)
  - [Escape-mode negotiation](#escape-mode-negotiation)
  - [Literal mode](#literal-mode-no_backslash_escapestrue)
  - [Escape-processing mode](#escape-processing-mode-no_backslash_escapesfalse)
- [Placeholder Tokenizer](#placeholder-tokenizer)
- [`executemany`](#executemany)
- [Non-Guarantees and Explicit Limits](#non-guarantees-and-explicit-limits)
- [Compatibility Policy (1.x)](#compatibility-policy-1x)
- [Pinned Tests](#pinned-tests)
- [References](#references)

---

## Overview

pycubrid performs **driver-side literal binding**. When you call
`cursor.execute(sql, parameters)`, the driver:

1. Tokenizes `sql` into segments split on unquoted, uncommented `?` placeholders
   (`split_on_placeholders`, `pycubrid/_cursor_common.py:46-119`).
2. Validates that the number of placeholders matches `len(parameters)`
   (`pycubrid/_cursor_common.py:199-203`).
3. Converts each Python value to a SQL literal string via `format_parameter`
   (`pycubrid/_cursor_common.py:141-181`).
4. Concatenates the segments and the rendered literals into a single SQL string
   (`pycubrid/_cursor_common.py:204-208`).
5. Sends the fully-rendered SQL to CUBRID via `PrepareAndExecutePacket`
   (`pycubrid/cursor.py:139-150`, `pycubrid/aio/cursor.py:114-117`).

The binding implementation is **shared verbatim** between the synchronous
(`Cursor`) and asynchronous (`AsyncCursor`) paths via `CursorParamsMixin`
(`pycubrid/_cursor_common.py:237-257`). There is no behavioral divergence
between sync and async binding; parity is enforced by
`tests/test_aio_cursor_parity.py` and `tests/test_split_placeholders.py`.

**This is not server-side prepared-statement binding.** pycubrid does not send
parameter values as a separate typed payload; the broker receives a complete
SQL text per execute. See
[Non-Guarantees and Explicit Limits](#non-guarantees-and-explicit-limits).
The separate, **explicit sync-only** `pycubrid.compat.native` prepared cursor
does send typed INT32, UTF-8 string and SQL NULL values through FC2/FC3. It is
not a replacement for this ordinary 1.x contract; see its bounded
[#418 typed CAS design](PREPARED_BINDING_DESIGN.md) and
[API reference](API_REFERENCE.md#explicit-native-compatibility-subset).

---

## Placeholder Style

- `paramstyle = "qmark"` (`pycubrid/__init__.py:46`), per PEP 249.
- Placeholders are positional `?`. There are **no** named, numeric, or
  pyformat placeholders.
- The `parameters` argument to `execute()` must be a `Sequence` other than
  `str`/`bytes`/`bytearray`. Mappings are rejected with `ProgrammingError`
  (`pycubrid/_cursor_common.py:194-197`). The exact message text is
  informative; see
  [Non-Guarantees and Explicit Limits](#non-guarantees-and-explicit-limits).
- A placeholder count mismatch raises `ProgrammingError`
  (`pycubrid/_cursor_common.py:199-203`). The exact message text is
  informative; see
  [Non-Guarantees and Explicit Limits](#non-guarantees-and-explicit-limits).

---

## Type Mapping (Guarantees)

The following table is the **authoritative type-to-literal mapping** for 1.x.
Every row cites the implementing line in `pycubrid/_cursor_common.py` and the
test that pins the behavior.

> The **exception class** raised for each error case (e.g. `ProgrammingError`)
> is part of the contract; the **message text** shown in the table is
> illustrative only and may be refined within 1.x. See
> [Non-Guarantees and Explicit Limits](#non-guarantees-and-explicit-limits).

| Python type | SQL literal | Implementation | Pinned by |
|---|---|---|---|
| `None` | `NULL` | `_cursor_common.py:254-255` | `tests/test_param_security.py:95-97` |
| `bool` | `1` (True) / `0` (False) | `_cursor_common.py:260-261` | `tests/test_param_security.py:98-102` |
| `int` (and subclasses such as `IntEnum`/`IntFlag`) | `int.__repr__(value)` (decimal digits of the value). See [Numeric subclasses](#numeric-subclasses) | `_cursor_common.py:330-331` | `tests/test_param_security.py::TestFormatParameterTypes::test_int`, `::test_numeric_subclass_renders_by_value` |
| `float` (and subclasses) | `float.__repr__(value)` (shortest round-trip form, identical to `str()` of a plain `float`, e.g. `1e+20`); `nan`/`inf`/`-inf` raise `ProgrammingError` (current message: `"nan and inf are not supported by CUBRID"`) | `_cursor_common.py:332-335` | `tests/test_param_security.py::TestFormatParameterTypes::test_float*`, `::test_numeric_subclass_renders_by_value` |
| `decimal.Decimal` (and subclasses) | Plain fixed-point digits (`format(value, "f")` on the value converted to a plain `Decimal`, unquoted, never E notation); sign, trailing zeros and scale kept; more than 38 literal digits raise `DataError`; `NaN`/`Infinity` raise `ProgrammingError` (current message: `"nan and inf are not supported by CUBRID"`); subclasses raise `ProgrammingError` when the C `decimal` module is unavailable. See [Decimal parameters](#decimal-parameters) and [Numeric subclasses](#numeric-subclasses) | `_cursor_common.py:301-329` | `tests/test_param_security.py::TestFormatParameterTypes::test_decimal*`, `::TestPureDecimalFallback`; `tests/test_parity_integration.py::TestParityDecimalLiterals` |
| `str` (and subclasses) | Single-quoted literal; escaping per [String Escaping](#string-escaping), applied to a plain `str` copy of the value; NUL (`U+0000`) and Ctrl-Z (`U+001A`, `\x1a`) each raise `ProgrammingError` (current messages: `"string parameter contains null byte"`, `"string parameter contains Ctrl-Z (0x1A) byte"`). See [Text, binary and temporal subclasses](#text-binary-and-temporal-subclasses) | `_cursor_common.py:262-263, 194-228` | `tests/test_param_security.py:27-84`, `::TestStrSubclassEscaping` |
| `bytes`, `bytearray` (and subclasses) | `X'<hex>'` (lowercase hex, `bytes.hex(value)` / `bytearray.hex(value)`) | `_cursor_common.py:266-269` | `tests/test_param_security.py:104-106, 144-145`, `::TestBinarySubclassRendering` |
| `datetime.datetime` (naive, and subclasses) | `DATETIME'YYYY-MM-DD HH:MM:SS.mmm'` — year zero-padded to 4 digits; microseconds truncated to milliseconds (`microsecond // 1000`) | `_cursor_common.py:274-288` | `tests/test_param_security.py:124-127`, `::TestTemporalSubclassRendering` |
| `datetime.datetime` (tz-aware, and subclasses) | `DATETIMETZ'YYYY-MM-DD HH:MM:SS.mmm <tz>'` where `<tz>` is `tzinfo.key` when present (e.g. `Asia/Seoul`), otherwise a `±HH:MM` numeric offset. A non-empty `key` must be a plain `str` matching `[A-Za-z0-9_+/-]+`, otherwise `ProgrammingError` (current message: `"time zone key must be an IANA name matching [A-Za-z0-9_+/-]+"`) | `_cursor_common.py:231-249, 274-287` | `tests/test_param_security.py:147-169`, `::TestTzinfoKey` |
| `datetime.date` (and subclasses) | `DATE'YYYY-MM-DD'` — year zero-padded to 4 digits | `_cursor_common.py:289-290` | `tests/test_param_security.py:116-118`, `::TestTemporalSubclassRendering` |
| `datetime.time` (and subclasses) | `TIME'HH:MM:SS'` — microseconds and `tzinfo` dropped | `_cursor_common.py:291-294` | `tests/test_param_security.py:120-122`, `::TestTemporalSubclassRendering` |
| `pycubrid.types.Set` / `Multiset` / `Sequence` | `SET{e1, e2, ...}` / `MULTISET{...}` / `SEQUENCE{...}` (each keyword renders as `KEYWORD{}` when empty, e.g. `SET{}`); each element rendered by the rows of this table with the connection's escape mode. Nested typed collections raise `ProgrammingError` (current message: `"nested collection parameters are not supported"`); plain containers as elements are rejected as below. See [Typed collection parameters](#typed-collection-parameters) | `_cursor_common.py` `format_parameter` typed-collection branch | `tests/test_typed_collections.py`; `tests/test_replay_parity.py::typed_collection_parameters`, `::executemany_typed_collection_parameters`, `::typed_collection_backslash_escape_processing`; `tests/test_integration_collections.py::TestTypedCollectionParameters` |
| anything else, including objects that only claim a supported type through `__class__` | `ProgrammingError` (current message: `"unsupported parameter type"`) | `_cursor_common.py:342` | `tests/test_param_security.py:128-130`, `::TestClassSpoofing`; `tests/test_cursor.py:233-235` |

Integers are converted directly to decimal strings without conversion to `float`,
including values such as `10**1000` and `-(10**1000)` that exceed the float range.
This formatting behavior does not guarantee that CUBRID can store the value;
server numeric limits and Python's integer-to-string conversion limits still apply.
Pinned by `tests/test_param_security.py::TestFormatParameterTypes::test_large_int`
and `::test_bind_large_int`.

### Numeric subclasses

Subclasses of `int`, `float` and `decimal.Decimal` are rendered from their
numeric value through the base-class methods (`int.__repr__`, `float.__repr__`,
and `format(Decimal(value), "f")`), never through `str()`, `repr()` or
`format()` on the object itself. Before #518 they were rendered with
`str(value)`, so a subclass that overrides `__str__` changed the SQL text: on
Python 3.10 `enum.IntEnum` members were sent as `Color.RED` and `enum.IntFlag`
combinations as `Perm.R|W`, and a user subclass whose `__str__` returned
`1; DROP TABLE t` injected that text verbatim. `Color.RED` is now sent as `1`
and `Perm.R | Perm.W` as `6`. A `Decimal` subclass is converted to a plain
`Decimal` first, so overridden `__format__`, `is_nan()` or `as_tuple()` cannot
alter the literal or bypass the `NaN`/`Infinity` and 38-digit checks. `bool` is
checked before `int` and still renders as `1`/`0`; it cannot be subclassed.
The `Decimal` copy is only tamper-proof with CPython's C `decimal` module
(`_decimal`): the pure-Python fallback (`_pydecimal`) copies `_sign`, `_int`
and `_exp` through ordinary attribute reads, which a subclass can forge (#528).
When the C module is unavailable, a `Decimal` subclass raises
`ProgrammingError`; a plain `Decimal` is still accepted.
Pinned by `tests/test_param_security.py::TestFormatParameterTypes::test_numeric_subclass_renders_by_value`,
`::TestPureDecimalFallback` and `tests/test_parity_integration.py::TestParityNumericSubclassLiterals`.

### Text, binary and temporal subclasses

Subclasses of `str`, `bytes`, `bytearray`, `datetime.datetime`,
`datetime.date` and `datetime.time` are rendered from their stored value, never
through methods the subclass can override (#528):

- `str`: the value is first copied to a plain `str` (`str.__str__(value)`
  called on the base class), and the NUL/Ctrl-Z checks and escaping run on that
  copy. An overridden `replace()`, `__contains__()`, `__str__()` or
  `__format__()` is never called. Before #528 a subclass whose `replace()`
  returned `x'; DROP TABLE users; --` had that text sent unescaped.
- `bytes`/`bytearray`: `bytes.hex(value)` / `bytearray.hex(value)` read the
  buffer directly; an overridden `hex()` or `__bytes__()` is never called.
- Dates and times: the literal is built from the integer fields read through
  the base-class descriptors (`datetime.date.year`, `datetime.datetime.hour`,
  ...) with explicit zero padding, instead of `strftime()`. Overriding
  `strftime()`, `isoformat()` or the `year`/`hour`/... properties does not
  change the literal. The UTC offset comes from `datetime.datetime.utcoffset()`
  called on the base class, and its `days`/`seconds`/`microseconds` fields are
  read the same way.
- Years below 1000 are zero-padded to four digits (`date(99, 1, 2)` is sent as
  `DATE'0099-01-02'`). `strftime("%Y")` does not pad them on Linux, and CUBRID
  reads `DATE'99-01-02'` as 1999-01-02, so two-digit years were silently
  stored with the wrong year (#519); one- and three-digit years happened to
  round-trip and are unchanged in value.
- `tzinfo.key`: a non-empty key is spliced into the `DATETIMETZ` literal only
  when it is a plain `str` matching `[A-Za-z0-9_+/-]+` (every IANA name, such
  as `Asia/Seoul`, `Etc/GMT+5` or `America/Port-au-Prince`, qualifies);
  anything else raises `ProgrammingError`. A missing, `None` or empty key still
  falls back to the numeric `±HH:MM` offset.

Dispatch uses `type(value)`, not `isinstance()`, which also trusts an
overridden `__class__`. An object that only claims to be one of the supported
types through `__class__` (for example a transparent proxy) raises
`ProgrammingError("unsupported parameter type")`; unwrap it before binding.
`escape_string()` likewise raises `ProgrammingError` for a non-`str` argument.

Plain values render exactly as before, except the year padding.
Pinned by `tests/test_param_security.py::TestPlainLiteralsUnchanged`,
`::TestStrSubclassEscaping`, `::TestBinarySubclassRendering`,
`::TestTemporalSubclassRendering`, `::TestTzinfoKey`, `::TestClassSpoofing`,
and live on CUBRID 10.2 and 11.4 (sync and async) by
`tests/test_parity_integration.py::TestParityLiteralHardening`.

### Decimal parameters

A finite `decimal.Decimal` is rendered in plain fixed-point notation, never with
an exponent: `Decimal("1E-7")` becomes `0.0000001`, `Decimal("1E+5")` becomes
`100000`. CUBRID parses a numeric literal written with `E` as `DOUBLE`, so the
previous `str(value)` rendering (`1E-7`) silently turned such values into
floating point and lost digits on insert (#517). The sign, trailing zeros and
scale are kept as written: `Decimal("1.10")` is sent as `1.10` (CUBRID types it
`NUMERIC(3,2)`) and `Decimal("-0.00")` as `-0.00`.

CUBRID accepts a plain numeric literal of at most 38 digits (the `NUMERIC`
maximum precision) and rejects a longer one with error `-494` "Invalid
numeric". The digit count is that of the rendered literal: every digit of a
nonzero integer part plus every fractional digit, including leading fractional
zeros (`0.0000001` has 7) and trailing zeros; a lone `0` integer part is not
counted. A `Decimal` whose plain literal would need more than 38 digits raises
`DataError` before anything is sent, instead of falling back to `DOUBLE`. That
covers `Decimal("1E-39")`, 39 significant digits and any huge exponent such as
`Decimal("1E+999999999")`, which is rejected without being expanded. Round or
quantize such values before binding, or bind a `float` if `DOUBLE` semantics
are intended.

A fractional literal is `NUMERIC(p,s)` on the server. An integral value without
fractional digits (`Decimal("42")`, `Decimal("1E+5")`) renders as a plain
integer literal, which CUBRID types by magnitude (`INTEGER`, `BIGINT` or
`NUMERIC(p,0)`); the value stays exact. When the target column has a smaller
scale than the literal, CUBRID rounds on assignment as for any literal (see
[Non-Guarantees](#non-guarantees-and-explicit-limits)). `NaN` and `Infinity`
still raise `ProgrammingError`.

Pinned by `tests/test_param_security.py::TestFormatParameterTypes::test_decimal_plain_notation`,
`::test_decimal_precision_38_accepted`, `::test_decimal_precision_over_38_raises`
and `::test_bind_decimal_plain_notation`, and live on CUBRID 10.2 and 11.4 (sync
and async) by `tests/test_parity_integration.py::TestParityDecimalLiterals`.

### Typed collection parameters

Wrap the elements in `pycubrid.types.Set`, `Multiset` or `Sequence` (also
exported as `pycubrid.Set`, `pycubrid.Multiset`, `pycubrid.Sequence`) to bind a
CUBRID collection through an ordinary sync or async cursor (#567):

```python
from pycubrid.types import Multiset, Sequence, Set

cur.execute(
    "INSERT INTO t (tags, words, steps) VALUES (?, ?, ?)",
    (Set([1, 2, 3]), Multiset(["a", "a"]), Sequence([3, 1, 2])),
)
cur.execute("SELECT id FROM t WHERE tags SUBSETEQ ?", (Set([1, 2, 3, 4]),))
```

- Each type takes one iterable and stores its elements as a `tuple`
  (`.elements`); the objects are immutable, compare equal only to the same type
  with equal elements, and cannot be subclassed. A single `str`, `bytes` or
  `bytearray` argument raises `TypeError` instead of being split into characters.
- The literal keyword follows the type: `SET{...}`, `MULTISET{...}`,
  `SEQUENCE{...}` (CUBRID's `LIST{...}` is the same type). The server applies the
  collection semantics: `SET` drops duplicates, `MULTISET` keeps duplicates but
  not their order, `SEQUENCE` keeps both.
- Every element goes through the same override-proof renderer as a scalar
  parameter, so the element types are exactly the scalar rows of the table above
  (`None`, `bool`, `int`, `float`, `Decimal`, `str`, `bytes`, `bytearray`,
  `date`, `time`, `datetime`), including their subclass hardening. The server
  converts the elements to the column's element type, as for a literal.
- Nested collections are rejected (`ProgrammingError`): a typed collection
  inside another, or a plain `list`/`tuple`/`set`/`frozenset`/`dict` element.
- A `dict` is rejected at construction time (`TypeError`) for all three
  classes: iterating it would silently use only its keys and drop the
  values. `Sequence` additionally rejects a `set`/`frozenset` (`TypeError`):
  their iteration order is not guaranteed, which would make `Sequence`'s
  element order nondeterministic between runs. `Set` and `Multiset` accept a
  `set`/`frozenset` since their own server-side semantics do not depend on
  input order.
- `executemany()` accepts typed collections in each parameter set, including
  through the DML batch path (`EXECUTE_BATCH`).
- The instances are immutable and safe to `copy.copy()` (always returns the
  same object), `copy.deepcopy()` (the same object when every element is
  itself immutable; an independent copy, with independently copied elements,
  when an element such as `bytearray` is mutable) and `pickle`; re-invoking
  `__init__` on an existing instance cannot mutate it either.
- Fetching is unchanged: with `decode_collections=True` a `SET` column still
  decodes to `frozenset` and `MULTISET`/`SEQUENCE` to `list` (raw `bytes`
  otherwise). Decoded values are not wrapped back into these types; wrap them
  again (for example `Set(row[0])`) to bind them.

### Explicitly unsupported as a bound value

- `datetime.timedelta` — no branch; raises `ProgrammingError("unsupported parameter type")`.
- `pycubrid.Lob` — `Lob` instances are not converted to SQL literals. Insert
  raw `bytes` for `BLOB`/`BIT`-typed columns; for large object workflows use
  `Connection.create_lob()` plus the LOB write API
  (`pycubrid/lob.py`, `pycubrid/connection.py:333-339`).
- Collections (`list`, `tuple`, `set`, `frozenset`, `dict`) as a single bound
  value — raise `ProgrammingError` with an actionable message (current text:
  `cannot bind a collection (list/tuple/set/frozenset/dict) as a single parameter; pycubrid does not auto-expand IN (?, ?, ...) — expand the placeholders explicitly in the SQL, or wrap the elements in pycubrid.types.Set, Multiset or Sequence to bind a CUBRID collection`).
  There is **no automatic `IN (?, ?, ?)`** expansion; expand placeholders
  explicitly in the SQL. To bind a CUBRID collection, wrap the elements in a
  [typed collection parameter](#typed-collection-parameters); the message also
  says so.
- Arbitrary Python objects — raises
  `ProgrammingError("unsupported parameter type")`.

---

## String Escaping

String escaping is performed by `escape_string`
(`pycubrid/_cursor_common.py`). The behavior depends on the
`no_backslash_escapes` connection flag
(`pycubrid/_connection_common.py`). By default the flag is **auto-negotiated**
from the live server at connect time (see
[Escape-mode negotiation](#escape-mode-negotiation)); pass it explicitly to
`pycubrid.connect(..., no_backslash_escapes=True|False)` to override detection.
Automatic detection runs for each newly opened physical session, including
explicit `ping(reconnect=True)` recovery, before parameterized SQL can use it.

In every mode:

- The literal is wrapped in single quotes.
- NUL (`U+0000`) in the input raises `ProgrammingError`
  (`pycubrid/_cursor_common.py:130-131`). This is unconditional and applies
  in both modes. The exact message text is informative; see
  [Non-Guarantees and Explicit Limits](#non-guarantees-and-explicit-limits).
- Single quotes are doubled (`'` → `''`).
- Unicode code points (including non-BMP characters that UTF-16 would encode
  as a surrogate pair) are passed through unchanged
  (`tests/test_param_security.py::TestEscapeString::test_unicode_passthrough`,
  `::test_unicode_non_bmp_passthrough`).

### Escape-mode negotiation

CUBRID's `no_backslash_escapes` **system parameter defaults to `yes`** — a
backslash is an ordinary literal character, not an escape marker
([CUBRID manual, literal.rst](https://github.com/CUBRID/cubrid-manual/blob/master/en/sql/literal.rst):
*"An escape using a backslash can be used if you set no_backslash_escapes in
cubrid.conf as no. But this default value is yes."*). If the driver blindly
doubled backslashes against such a server, `C:\temp\file` would be stored as
`C:\\temp\\file` — silent data corruption (issue #255).

To stay correct against whatever the server is actually configured for, when
`no_backslash_escapes` is **not** passed to `connect()`, the driver probes the
live server once at connect time with `SELECT CHAR_LENGTH('\\')` (the SQL
literal `'\\'`, two backslash characters):

- result `2` → the server left both backslashes intact → **literal mode**, so
  the driver pins `no_backslash_escapes=True` (does NOT double backslashes).
- result `1` → the server unescaped the pair → **escape-processing mode**, so
  the driver pins `no_backslash_escapes=False`.
- any other value or a probe error → raises `OperationalError`. The driver
  refuses to guess the escape mode, because a wrong value silently corrupts
  string escaping (and can enable SQL injection). Pass `no_backslash_escapes`
  explicitly to skip detection when the probe cannot run.

Passing `no_backslash_escapes=True` or `False` explicitly skips the probe and
retains that choice across reconnections. Without an explicit value, a newly
opened physical session is probed before use; a healthy same-session
`ping()` does not re-probe. A failed probe prevents direct connection setup;
during `ping(reconnect=True)`, it retires the replacement session and returns
`False`. Neither path guesses a mode or replays interrupted SQL. In the sync and
async paths, parameterized SQL bound before a session replacement is rejected before
send if its session generation changed; the caller must deliberately retry
the operation. Normal `CAS_INFO=OUT_TRAN` responses keep the session; only a
failed pre-request `CHECK_CAS` reconnects (#485). Cursors run that check before
rendering parameters; SQL rendered for a session that is replaced afterwards is
rejected before send in sync and async alike. SQL strings passed directly to
`executemany_batch()` are rendered by the caller and are not generation-fenced.

The [10.2](https://www.cubrid.org/manual/en/10.2/admin/config.html) and
[11.4](https://www.cubrid.org/manual/ko/11.4/admin/config.html) CUBRID manuals
do not classify `no_backslash_escapes` as dynamically changeable. This driver
behavior does not imply support for a per-session `SET` toggle or prove
heterogeneous failover between differently configured servers. An explicit
mode should be used only when every possible target is known to match it.

### Literal mode (`no_backslash_escapes=True`)

This is what auto-negotiation selects against a stock CUBRID server
(`no_backslash_escapes=yes`):

1. Single quotes are doubled (`'` → `''`).
2. Backslashes and control characters are **left untouched** (the server
   treats them as ordinary characters, so they round-trip byte-for-byte).
3. NUL rejection still applies.
4. The result is wrapped in single quotes.

Pinned by `tests/test_aio_cursor_parity.py:99-105`,
`tests/test_backslash_negotiation.py`, and the live round-trip suite
`tests/test_integration.py::TestBackslashRoundTrip`.

### Escape-processing mode (`no_backslash_escapes=False`)

Selected when the server runs `no_backslash_escapes=no`, or when pinned
explicitly. The driver doubles backslashes so the server unescapes them back:

1. Backslashes are doubled (`\` → `\\`).
2. Single quotes are doubled (`'` → `''`).
3. `\r` and `\n` are each prefixed with a backslash (`\n` → `\\n`, etc.).
4. `\x1a` (Ctrl-Z) has no safe CUBRID literal escape and raises
   `ProgrammingError` in **both** escape modes (see [String Escaping](#string-escaping)).
5. The result is wrapped in single quotes.

Pinned by `tests/test_param_security.py:27-55` and
`tests/test_aio_cursor_parity.py:87-96`.

---

## Placeholder Tokenizer

`split_on_placeholders` (`pycubrid/_cursor_common.py:46-119`) parses the SQL
text and returns segments split only on `?` characters that appear in
**executable SQL** — never inside string literals, identifier quotes, or
comments. Specifically the tokenizer recognizes and skips:

- Single-quoted string literals (`'...'`), including doubled-quote escapes
  (`''`).
- Double-quoted identifiers (`"..."`), including doubled-quote escapes (`""`).
- Line comments (`-- ... <EOL>`).
- Block comments (`/* ... */`).

A `?` inside any of the above is part of the SQL text and is **not** treated as
a placeholder. This is pinned by `tests/test_split_placeholders.py` in its
entirety.

The replacement step concatenates segments and rendered literals
(`pycubrid/_cursor_common.py:204-208`); it never performs a naïve
`str.replace("?", ...)`, so a literal `?` in the rendered value of one
parameter cannot inadvertently consume the next placeholder.

---

## `executemany`

For DML verbs (`INSERT`, `UPDATE`, `DELETE`, `MERGE`), `executemany`:

1. Calls `_bind_parameters(sql, params)` once per parameter row, producing one
   fully-rendered SQL string per row.
2. Sends the list of rendered SQL strings in a single `BatchExecutePacket`
   (`pycubrid/cursor.py:252-257`, `pycubrid/aio/cursor.py:206-211`), dispatched
   from `executemany` via `executemany_batch`
   (`pycubrid/cursor.py:217`, `pycubrid/aio/cursor.py:191`).

For non-DML statements, `executemany` falls back to a per-row `execute` loop
(`pycubrid/cursor.py:220-238`, `pycubrid/aio/cursor.py:176-187`).

Each row is bound independently with the same type-mapping rules above.

---

## Non-Guarantees and Explicit Limits

The following behaviors are **explicitly outside the contract** and may change
without a major version bump. They are listed so that callers do not depend on
them implicitly.

- **Ordinary sync/async cursors do not use server-side prepared binding.**
  `pycubrid.Cursor` and `pycubrid.aio.AsyncCursor` render parameters into SQL
  text on the client, so their `execute()` sends a complete SQL string without
  a typed value payload or statement-handle cache. The separate, opt-in
  `pycubrid.compat.native` sync cursor has a narrow typed scalar contract;
  its performance and plan-cache effects are not claimed here.
- **Identifiers are not escaped.** Code paths that interpolate identifiers
  (notably `Cursor.callproc`, `pycubrid/cursor.py:328-336`) embed the
  identifier into the SQL text without quoting. Application code must validate
  any identifier it accepts from untrusted input. Parameter binding (`?`)
  applies only to **values**, never to identifiers.
- **Type-object identity at the binding layer.** PEP 249 type objects
  (`STRING`, `BINARY`, etc.) describe `cursor.description`; they are not
  consulted during parameter binding.
- **Server-side type coercion is not normalized.** The driver renders a SQL
  literal; CUBRID then coerces the literal to the target column type per its
  own rules. The driver does not adjust precision, scale, or charset to match
  the destination column.
- **No driver-enforced length limits.** Strings, byte buffers, and rendered
  SQL are bounded only by Python memory and CUBRID server limits.
- **No automatic `IN`-clause expansion.** A `list`/`tuple`/`set` passed as a
  single bound value raises `ProgrammingError`. Expand placeholders in the SQL
  text yourself (e.g.,
  `f"... WHERE id IN ({','.join('?' * len(ids))})"` with `params=tuple(ids)`).
- **Exception messages.** The exact text of `ProgrammingError` messages
  ("unsupported parameter type", "wrong number of parameters", "string
  parameter contains null byte", "nan and inf are not supported by CUBRID",
  "parameters must be a sequence") is not part of the contract. The
  **exception class** is; the **message text** may be refined.
- **Behavioral subtleties not listed above.** Anything not enumerated in
  [Type Mapping](#type-mapping-guarantees) or [String Escaping](#string-escaping)
  is not a guarantee. The public-API surface gate
  (`scripts/check_public_api.py`) does not detect behavioral drift; it only
  detects structural surface changes. See
  [`RELEASE_POLICY.md`](../RELEASE_POLICY.md) §"What the gate does *not*
  detect".

---

## Compatibility Policy (1.x)

Within the 1.x line, the following changes are governed by
[`RELEASE_POLICY.md`](../RELEASE_POLICY.md):

### Allowed in a minor (`1.y` → `1.y+1`) release

- Adding support for a new Python type (e.g., `uuid.UUID`,
  `datetime.timedelta`) — additive only, with a new row added to the
  [Type Mapping](#type-mapping-guarantees) table.
- Improving an exception **message** while keeping the exception **class**
  unchanged.
- Internal refactoring of `escape_string`, `format_parameter`, or
  `split_on_placeholders` that does not change the produced SQL literal or the
  raised exception class for any input already covered above.
- Adding new connection flags that modify binding behavior, provided the
  default value preserves the rules in this document.

### Requires a major (`1.y` → `2.0`) release

- Removing or renaming any row in the
  [Type Mapping](#type-mapping-guarantees) table.
- Changing the SQL literal produced for any input already in the table (for
  example: switching `bytes` from `X'<hex>'` to another representation,
  changing the default `datetime` precision, or changing the tz suffix
  format).
- Tightening or loosening NUL / NaN / Inf rejection.
- Changing the default value of `no_backslash_escapes`.
- Changing the `paramstyle` from `"qmark"`.
- Changing the rejection of `Mapping` parameters (i.e., reintroducing or
  removing named-parameter support).

### Deprecation flow

Behaviors slated for removal in a future major release are first marked
**deprecated** in `CHANGELOG.md > ### Deprecated`, with a documented
replacement and a migration window of at least one minor release before the
major-version removal.

---

## Pinned Tests

The following test modules pin the behavior documented above. CI failures in
any of these are contract regressions:

- `tests/test_param_security.py` — per-type formatting, NUL rejection,
  NaN/Inf rejection, datetime tz rendering, Decimal handling, bytes hex
  rendering.
- `tests/test_split_placeholders.py` — placeholder tokenizer behavior across
  quoted strings, quoted identifiers, line comments, block comments, and
  integration with `_bind_parameters`.
- `tests/test_cursor.py` (lines 183-235) — end-to-end binding of a
  multi-type parameter sequence, placeholder-count validation, and
  `Mapping`/`str` parameter rejection.
- `tests/test_aio_cursor_parity.py` (lines 76-105) — sync/async parity for
  NUL rejection, default escaping, and `no_backslash_escapes` mode.
- `tests/test_typed_collections.py` — typed `Set`/`Multiset`/`Sequence`
  rendering, element hardening and nested-collection rejection; sync/async
  parity in `tests/test_replay_parity.py::typed_collection_parameters`.

Together these tests provide the executable specification of this contract.

---

## References

- Implementation: [`pycubrid/_cursor_common.py`](../pycubrid/_cursor_common.py)
- Sync entry points: [`pycubrid/cursor.py`](../pycubrid/cursor.py)
- Async entry points: [`pycubrid/aio/cursor.py`](../pycubrid/aio/cursor.py)
- Connection flag: [`pycubrid/_connection_common.py`](../pycubrid/_connection_common.py)
- Release policy: [`RELEASE_POLICY.md`](../RELEASE_POLICY.md)
- Type system (fetch / description side): [`docs/TYPES.md`](TYPES.md)
- API reference: [`docs/API_REFERENCE.md`](API_REFERENCE.md)
