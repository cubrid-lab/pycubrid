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
| `None` | `NULL` | `_cursor_common.py:143-144` | `tests/test_param_security.py:95-97` |
| `bool` | `1` (True) / `0` (False) | `_cursor_common.py:145-146` | `tests/test_param_security.py:98-102` |
| `int` | `str(value)` (decimal digits) | `_cursor_common.py:177-180` | `tests/test_param_security.py:107-109` |
| `float` | `str(value)`; `nan`/`inf`/`-inf` raise `ProgrammingError` (current message: `"nan and inf are not supported by CUBRID"`) | `_cursor_common.py:177-180` | `tests/test_param_security.py:132-142` |
| `decimal.Decimal` | `str(value)` (unquoted) | `_cursor_common.py:175-176` | `tests/test_param_security.py:113-115` |
| `str` | Single-quoted literal; escaping per [String Escaping](#string-escaping); NUL (`U+0000`) raises `ProgrammingError` (current message: `"string parameter contains null byte"`) | `_cursor_common.py:147-148, 124-138` | `tests/test_param_security.py:27-78` |
| `bytes`, `bytearray` | `X'<hex>'` (lowercase hex) | `_cursor_common.py:149-150` | `tests/test_param_security.py:104-106, 144-145` |
| `datetime.datetime` (naive) | `DATETIME'YYYY-MM-DD HH:MM:SS.mmm'` — microseconds truncated to milliseconds (`value.microsecond // 1000`) | `_cursor_common.py:151-152, 170` | `tests/test_param_security.py:124-127` |
| `datetime.datetime` (tz-aware) | `DATETIMETZ'YYYY-MM-DD HH:MM:SS.mmm <tz>'` where `<tz>` is `tzinfo.key` when present (e.g. `Asia/Seoul`), otherwise a `±HH:MM` numeric offset | `_cursor_common.py:151-169` | `tests/test_param_security.py:147-169` |
| `datetime.date` | `DATE'YYYY-MM-DD'` | `_cursor_common.py:171-172` | `tests/test_param_security.py:116-118` |
| `datetime.time` | `TIME'HH:MM:SS'` — microseconds dropped | `_cursor_common.py:173-174` | `tests/test_param_security.py:120-122` |
| anything else | `ProgrammingError` (current message: `"unsupported parameter type"`) | `_cursor_common.py:181` | `tests/test_param_security.py:128-130`; `tests/test_cursor.py:233-235` |

### Explicitly unsupported as a bound value

- `datetime.timedelta` — no branch; raises `ProgrammingError("unsupported parameter type")`.
- `pycubrid.Lob` — `Lob` instances are not converted to SQL literals. Insert
  raw `bytes` for `BLOB`/`BIT`-typed columns; for large object workflows use
  `Connection.create_lob()` plus the LOB write API
  (`pycubrid/lob.py`, `pycubrid/connection.py:333-339`).
- Collections (`list`, `tuple`, `set`, `dict`) as a single bound value — no
  branch; raises `ProgrammingError("unsupported parameter type")`. There is
  **no automatic `IN (?, ?, ?)` expansion**; expand placeholders explicitly in
  the SQL.
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
- any other value or a probe error → falls back to `no_backslash_escapes=False`
  (legacy behavior) and logs a warning, so detection never breaks `connect()`.

Passing `no_backslash_escapes=True` or `False` explicitly skips the probe
entirely. Negotiation happens once per physical connection and is preserved
across transparent reconnects.

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
3. `\r`, `\n`, and `\x1a` (Ctrl-Z) are each prefixed with a backslash
   (`\n` → `\\n`, etc.).
4. The result is wrapped in single quotes.

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

- **No server-side prepared-statement binding.** The driver renders parameters
  into SQL text on the client. CUBRID receives a complete SQL string per
  `execute`. There is no separate typed parameter payload and no client-side
  statement-handle cache. Performance characteristics, query-plan caching, and
  log output reflect this design.
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
