# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added
- **`charset` connection option (#86)** — `pycubrid.connect()`,
  `pycubrid.aio.connect()`, `pycubrid.compat.native.connect()` and
  `cubriddb.Connection(charset=...)` (previously `"utf8"` only) accept
  `charset` (default `"utf-8"`): a Python codec or the CUBRID names `utf8`,
  `euckr`, `iso88591`. It is validated before any socket work (`TypeError` for
  a non-string; `ValueError` for an unknown codec, CUBRID `binary` or a codec
  that is not ASCII-transparent, such as UTF-16/32, Shift_JIS, Big5, GBK or
  CP949; `DataError` for unencodable credentials) and kept across reconnects.
  SQL text with rendered parameters, batch and schema-info arguments, prepared
  strings and `OPEN_DATABASE` credentials are encoded with it before anything
  is sent; an unencodable character raises `DataError` naming the codec and
  position, nothing of that request is sent and the session stays usable.
  Character values, `ENUM` and collection elements, column/table names and
  defaults are decoded strictly (`DataError` naming the codec), error messages
  with replacement. Fetched `JSON` stays UTF-8 (JSON parameters are SQL text); `NUMERIC`, timezone names, version
  strings and LOB contents are unaffected (`CLOB` bytes are in the column
  charset). The broker does no conversion, so the codec must match the
  database charset; a `CHARSET utf8` column in an EUC-KR database raises
  `DataError` under `charset="euckr"` (convert with `CAST(... CHARSET euckr)`).
  With the default UTF-8 codec, request bytes are unchanged except two edge
  cases: a column name that is not valid UTF-8 now raises `DataError` and an
  ordinary cursor keeps the session (previously `OperationalError('malformed response from broker')`
  and a closed connection), and a database/user/password longer than its
  32-byte `OPEN_DATABASE` field is cut on a character boundary instead of
  mid-character. With `euc_kr`, Hangul outside KS X 1001 (such as 똠), which
  Python would send as an 8-byte makeup sequence, is rejected as unencodable,
  and stored Hangul filler (U+3164) and jamo read back as separate characters,
  as CUBRID stores them.
  LOB file locators, which embed the table name, decode with the connection
  codec and `errors="replace"`. `charset=None` means the default, and a CUBRID
  locale such as `"ko_KR.euckr"` is accepted. `get_schema_info()` checks its arguments before sending, so an
  unencodable table or column pattern no longer closes the connection. A new `integration-charset` CI job runs the live round trips
  against CUBRID 11.4 created with `CUBRID_LOCALE=ko_KR.euckr`.

### Documentation
- **`llms.txt` no longer advertises prepared statements, and the two entry points are single-sourced (#414)** — the root `llms.txt` claimed prepared statements and a `Cursor.prepare()` method, which ordinary cursors do not have, listed an incomplete exception hierarchy, hardcoded test and coverage counts and linked to the retired `cubrid-cookbook/python` paths, while `docs/llms.txt` was a separately maintained, differing index. `docs/llms.txt` is now the only maintained index, checked against the code: driver-side literal binding and its documented limits, the opt-in sync-only `pycubrid.compat.native` prepared subset, sync and async (`pycubrid.aio`) feature parity, the full PEP 249 exception list and `cubrid-cookbook-python` links. `scripts/generate_llms_full.py` copies it byte-for-byte to the root `llms.txt`, and the CI `lint` job now fails when either `docs/llms-full.txt` or `llms.txt` is stale. `docs/SUPPORT_MATRIX.md` and `docs/TROUBLESHOOTING.md` (+ Korean) no longer describe `cursor.execute(sql, params)` as server-side `PREPARE_AND_EXECUTE` binding (the section is renamed "Parameterized Query Issues"), and the support matrix notes that `nextset()` raises `NotSupportedError`; the Korean, German, Hindi, Russian and Chinese READMEs now describe driver-side binding like the English README. `CONTRIBUTING.md` documents the workflow.
- **Cookbook smoke-test fallback is now pinned** — `RELEASING.md`'s manual `gh workflow run smoke-test.yml -R cubrid-lab/cubrid-cookbook-python` fallback now passes `-f package=pycubrid -f version=X.Y.Z`, so it verifies the exact published release instead of testing the cookbook's latest releases (cubrid-lab/cubrid-cookbook-python#179).

### Fixed
- **Row cells whose value does not use exactly their declared size are rejected (#523)** —
  the readers for fixed-width values (`SHORT`, `INT`, `BIGINT`, `FLOAT`,
  `DOUBLE`, `MONETARY`, `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`, `OBJECT`, and
  the fixed part of the TZ types) ignored a cell's size word, so a FETCH or
  inline execute row whose cell declared more bytes than the reply held (for
  example an `INT` declaring 1000 bytes at the end of the reply), or a size
  that disagreed with the value's width, was decoded as if it were complete.
  Every row cell must now use exactly its declared size, like the
  length-prefixed values since #383 (checked before the value is read, and
  when a reply is re-walked before `DataError`); otherwise the reply raises
  `OperationalError('malformed response from broker')` and closes the
  connection, sync and async. A normal server always sends the exact size, so
  valid replies, the `DataError` classification of complete replies (#492,
  #512) and SQL `NULL` cells (a non-positive size) are unchanged. A negative
  FETCH tuple count, which read as an empty page and silently ended the result
  set early, is rejected the same way. Documented in `docs/PROTOCOL.md` and
  `docs/TROUBLESHOOTING.md` (+ Korean).
- **Tests: protocol fuzzing seeds realistic replies (#523)** — every
  `tests/test_protocol_fuzz.py` seed used to carry zero columns, so no fuzz
  case reached column metadata or row cells. Seeds built by
  `tests/helpers/cas_reply.py` now cover `PREPARE_AND_EXECUTE`, `PREPARE` and
  `EXECUTE` replies with metadata for string, numeric, `NUMERIC`, temporal and
  TZ, `BIT`/`VARBIT`, OID, collection, LOB and `JSON` columns; multi-row FETCH
  replies (including CALL and `NULL`-typed layouts); and schema, batch and LOB
  replies. Unmutated seeds must decode to exactly their values; mutations aim
  at truncation at field boundaries, length and count words, and collection
  element types, and the oracle admits only structural errors (reported as
  `OperationalError`), server errors and `DataError` for complete replies,
  also through the sync and async connections. Thirteen tests whose only
  assertion was `is not None` now check the expected value, and two unittest
  guards use `self.fail()` instead of a narrowing `assert`.
- **Tests: a configured but unreachable CUBRID now errors instead of skipping (#522, #432)** —
  16 integration modules probed the server at import time and called
  `skipif("CUBRID instance not available")`, so pointing the suite at a dead
  endpoint produced hundreds of silent skips despite `tests/conftest.py`
  promising fail-closed behavior. Those probes (and the TLS module's import-time
  TLS probes) are gone: one gate in `tests/conftest.py` skips integration tests
  when neither `CUBRID_TEST_URL` nor `CUBRID_TEST_HOST` is set, and otherwise
  probes the endpoint once per session and makes every plain integration test
  error with the endpoint and connection error. Every integration module, the
  gate and `scripts/wait_for_cubrid.py` now resolve the endpoint through one
  helper, `tests/_cubrid_endpoint.py`: per-field `CUBRID_TEST_*` variables win,
  then the components of `CUBRID_TEST_URL` (a scheme-less value such as `1`
  stays a pure on/off switch; a malformed URL errors the integration tests
  without breaking offline collection), then `localhost:33000/testdb` as
  `dba` — so a URL naming another host or port is no longer silently ignored in
  favor of whatever listens on `localhost:33000`. CI, which exports both, is
  unchanged. `make integration` waits with `wait_for_cubrid.py` instead of
  `sleep 10`, runs `integration and not tls`, fails when the JUnit audit
  (`check_integration_lanes.py --results`) finds an all-skipped run, always
  removes the container, and accepts `CUBRID_TEST_PORT=<port>` (also used by
  `docker-compose.yml`) to avoid a busy port 33000. `make integration-tls` also
  waits for readiness instead of sleeping, audits its JUnit report and always
  removes the container.
- **Rows fetched before a failing page are no longer lost (#507)** — when a
  later FETCH page raised a data-level `DataError` (invalid text #492, an
  unresolved zone #413, a zero date #512), `fetchall()` and `fetchmany()`
  dropped the rows they had already collected in that call, and because the
  fetch position did not advance, every retry requested the same page again:
  it failed again, or, in autocommit mode once the broker had closed the
  result after its last page, raised `DatabaseError` with CAS error `-1012`.
  The call that reaches the page still raises `DataError` and the whole page is
  withheld, but the rows it had collected stay buffered and the next
  `fetchone()`/`fetchmany()`/`fetchall()` (or iteration) returns them without
  contacting the server. After that every fetch raises the same `DataError`
  again, without requesting the page, until `execute()` or `close()`, so no
  row of or past the failing page is returned and retries do not loop on the
  server. The connection stays usable and the cursor keeps its handle, sync
  and async alike. Documented in `docs/API_REFERENCE.md`, `docs/TYPES.md` and
  `docs/TROUBLESHOOTING.md` (+ Korean); live-tested against CUBRID 11.4 with a
  zero `DATE` several FETCH pages into the result.
- **`pycubrid.aio` no longer logs an asyncio warning whenever a TLS broker closes the connection (#514)** — after the in-place `loop.start_tls()` upgrade the stream protocol still believed it was on a plaintext transport, so every TLS peer close (broker restart, CAS recycle, idle timeout, dropped session before a reconnect) made asyncio log `WARNING returning true from eof_received() has no effect when using ssl`. The upgrade now marks the stream protocol as running over TLS, as `StreamWriter.start_tls()` does on Python 3.11+. Log output only; connection state, errors and the sync driver are unchanged.
- **`pycubrid.aio.connect(..., ssl=...)` no longer hangs forever when the TLS handshake is interrupted (#513)** — if the broker stalled or reset the connection before the TLS handshake completed, `read_timeout` (or the 10-second `ssl_handshake_timeout`) fired as intended, but connect cleanup then awaited `StreamWriter.wait_closed()` on a stream that asyncio never marks closed (its `SSLProtocol` drops `connection_lost` while still handshaking), so the call never returned on Python 3.11+. The failed upgrade now notifies the stream protocol itself after aborting the transport, and connect raises `OperationalError` within `read_timeout` and closes the socket. The sync driver was not affected. `docs/CONNECTION.md` and `docs/TROUBLESHOOTING.md` (+ Korean) now state which timeout bounds the TLS handshake (`read_timeout`; `connect_timeout` covers only the TCP connect).
- **Reads past the end of a broker reply are rejected (#383)** — a length
  field that ran past the end of a reply was cut short by a Python slice and
  returned as if complete: a `BIT`/`VARBIT` cell declaring 8 bytes but carrying
  2 returned those 2 bytes, a `LOB_READ` reply declaring 10 bytes with 3 in
  the payload set `bytes_read = 10`, and strings, `NUMERIC`, `JSON`, raw
  collections and LOB handles and locators behaved the same way. A negative
  length moved the reader backwards. Every length-prefixed read now checks
  `0 <= length <= remaining` before it moves, and a decoded collection's
  elements must fill its declared size exactly (previously elements could run
  into the next column). Such a reply raises
  `OperationalError('malformed response from broker')` and closes the
  connection, sync and async, like other framing damage; `DataError` stays for
  complete replies (#492, #512). A `LOB_READ` count below the requested length
  is still a valid short read (#362), and bytes after the last value a reply
  declares are still ignored. Documented in `docs/PROTOCOL.md` and
  `docs/TROUBLESHOOTING.md` (+ Korean).
- **Zero `DATE`/`DATETIME`/`TIMESTAMP` values no longer close the connection
  (#512)** — CUBRID accepts zero values such as `DATE'0000-00-00'`,
  `DATETIME'0000-00-00 00:00:00'` and zero `TIMESTAMP`, `TIMESTAMPTZ`,
  `TIMESTAMPLTZ`, `DATETIMETZ` and `DATETIMELTZ` values, but Python's
  `datetime` has no year 0. The decoder's raw `ValueError` was treated as a
  framing failure: `OperationalError('malformed response from broker')`, the
  socket closed, and every later call raised `InterfaceError('connection is
  closed')`. The value now raises `DataError` naming the CUBRID type and fields
  (`CUBRID DATE value (0, 0, 0) cannot be represented in Python: year 0 is out
  of range`) and the session stays usable, on `execute()` and on a later fetch
  page, sync and async, with the same cursor state as invalid UTF-8 (#492).
  Any other temporal field Python cannot hold (such as a `TIME` hour of 25)
  in a complete reply is reported the same way.
  A row value that raises `DataError` (#492, #413, #512) is now reported only
  after the rest of the row data is checked against the reply length, so a
  reply cut short still raises `OperationalError` and closes the connection,
  and so does a temporal field whose declared size does not match its type,
  or a collection element that runs past the collection.
  The explicit prepared API (`pycubrid.compat.native`) stays fail-closed.
  There is no option to return zero dates as `None` or text;
  `docs/TYPES.md` and `docs/TROUBLESHOOTING.md` (+ Korean) document SQL
  workarounds (`NULLIF(d, DATE'0000-00-00')`, `CASE`, `TO_CHAR`). Found by
  the CUBRID 10.2-11.4 version differential (#351); the behavior was the same
  on 10.2, 11.0, 11.2 and 11.4.
- **Security: `str`, `bytes`, date and time parameters are rendered without
  calling overridable methods (#528)** — `format_parameter()` escaped `str`
  parameters with `value.replace(...)` and `"\x00" in value`, rendered
  `bytes`/`bytearray` with `value.hex()` and dates and times with
  `value.strftime(...)`, all of which a subclass can override, and spliced a
  `tzinfo.key` into `DATETIMETZ` literals unescaped. A `str` subclass whose
  `replace()` returned `x'; DROP TABLE users; --` had that text sent
  unescaped; an overridden `hex()` or `strftime()`, or a `tzinfo.key`
  containing `'`, injected SQL the same way. A `str` subclass is now copied to
  a plain `str` through the base class before the NUL/Ctrl-Z checks and
  escaping, `bytes`/`bytearray` are rendered with `bytes.hex(value)` /
  `bytearray.hex(value)`, and date/time literals are built from the integer
  fields read through the base-class descriptors (the UTC offset through
  `datetime.datetime.utcoffset()` and the `timedelta` descriptors). A
  non-empty `tzinfo.key` must be a plain `str` matching `[A-Za-z0-9_+/-]+`
  (every IANA name does), otherwise `ProgrammingError`. Parameters are
  dispatched on `type(value)`, so an object that only claims a supported type
  through `__class__` (including transparent proxies) raises
  `ProgrammingError("unsupported parameter type")` instead of a raw
  `TypeError` or being rendered through the proxy; `escape_string()` raises
  `ProgrammingError` for a non-`str` argument. When the C `decimal` module is
  unavailable (pure-Python `_pydecimal` fallback), `Decimal` subclasses raise
  `ProgrammingError`, because that module copies their value through
  attributes a subclass can forge. Output for plain `str`, `bytes`,
  `bytearray`, `date`, `datetime` and `time` values is byte-identical except
  for the year padding below, and sync and async cursors share the change.
- **Years below 1000 are zero-padded in `DATE`/`DATETIME`/`DATETIMETZ`
  literals (#519)** — the year was rendered with `strftime("%Y")`, which does
  not pad on Linux, and CUBRID reads `DATE'99-01-02'` as 1999-01-02, so
  `date(99, 1, 2)` and `datetime(99, ...)` were silently stored and compared as
  year 1999. Years are now always four digits (`DATE'0099-01-02'`); years 1,
  99, 999 and 1000 round-trip on CUBRID 10.2 and 11.4.
- **Security: `int`, `float` and `Decimal` subclasses are bound by value
  (#518)** — `format_parameter()` rendered `int` and `float` parameters with
  `str(value)` and `Decimal` with `format(value, "f")`, which dispatch to
  methods a subclass can override. A subclass with a custom `__str__`/`__format__`
  could therefore inject arbitrary text into the SQL sent to the server (a
  `__str__` returning `1; DROP TABLE t` was sent verbatim), and on Python 3.10
  `enum.IntEnum`/`enum.IntFlag` members were sent as `Color.RED` / `Perm.R|W`
  instead of their values. Values are now rendered through the base-class
  methods (`int.__repr__`, `float.__repr__`, and a plain `Decimal` copy for the
  `NaN`/`Infinity` and 38-digit checks and `format(..., "f")`), so `Color.RED`
  is sent as `1` and `Perm.R | Perm.W` as `6`. Output for plain `int`, `float`
  and `Decimal` values is unchanged, `bool` still renders as `1`/`0`, and
  sync and async cursors share the change.
- `decimal.Decimal` parameters are now rendered in plain fixed-point notation
  instead of `str(value)`, which switched to E notation (`Decimal("1E-7")` was
  sent as `1E-7`). CUBRID parses an E-notation literal as `DOUBLE`, so such
  values silently came back as `float` and lost digits when inserted into
  `NUMERIC` columns; they now stay `NUMERIC` with their sign, trailing zeros
  and scale (`Decimal("0.0000001")` is sent as `0.0000001`, `Decimal("1E+5")`
  as `100000`). A `Decimal` whose plain literal needs more than 38 digits
  (CUBRID's `NUMERIC` maximum precision; leading fractional zeros count), such
  as `Decimal("1E-39")` or a 39-significant-digit value, raises `DataError`
  before anything is sent instead of becoming `DOUBLE`; CUBRID itself rejects
  such plain literals. `NaN`/`Infinity` still raise `ProgrammingError`, and
  integral Decimals written without an exponent (`Decimal("42")`) render as the
  same integer literal as before. Sync and async cursors share the change. (#517)
- With `decode_collections=True`, a nonempty `SET`/`MULTISET`/`SEQUENCE`
  (`LIST`) whose elements are all SQL NULL, such as `{NULL}` or
  `{NULL, NULL}`, now decodes to `[None, ...]` (a `SET` becomes
  `frozenset({None})`) instead of raw `bytes`. CUBRID 10.2 and 11.4 send
  these with element type NULL, the element count and a `-1` length per
  element; only an empty collection was handled before. A NULL-type header
  (including an empty collection) whose count does not match the payload
  size, or whose element lengths are not NULL markers, raises `OperationalError('malformed response from
  broker')`. Default raw-bytes mode, empty and mixed collections, and public
  signatures are unchanged. (#483)
- Invalid UTF-8 in a fully received broker reply no longer raises
  `OperationalError('malformed response from broker')` and closes the
  connection. Server error messages (and batch per-statement error messages)
  are decoded with `errors="replace"`, so the real CUBRID error surfaces with
  its `errno`/`sqlstate`; this happens when CUBRID cuts an echoed value in the
  middle of a multi-byte character. A `CHAR`/`VARCHAR`/`NCHAR`/`ENUM`/`JSON`
  value (including a collection element) that is not valid UTF-8 now raises
  `DataError` and the session stays usable; `execute()` keeps the server
  handle so it is released normally. CUBRID 10.2 can store such a value when
  it truncates an oversized string by bytes. Truncated packets and invalid
  UTF-8 in protocol metadata still raise the connection-level
  `OperationalError`, as does the explicit prepared API (`compat.native`),
  which retires the session on any non-server failure. (#492)
- A `DELETE`/`UPDATE` of a parent row that a foreign key still references
  (native `-924`, `ER_FK_RESTRICT`) and a `TRUNCATE` of a referenced parent
  table (`-1284`, `ER_TRUNCATE_PK_REFERRED` on CUBRID 11.4; 10.2 reports
  `-924`) now raise `IntegrityError` with SQLSTATE `23000` instead of a
  generic `DatabaseError`, for single statements and batch failures alike.
  `IntegrityError` is still a `DatabaseError` subclass. Dropping a referenced
  primary key (`-923`) is a schema error and stays `DatabaseError`. (#493)
- A `TIMESTAMPTZ`/`TIMESTAMPLTZ`/`DATETIMETZ`/`DATETIMELTZ` value whose
  region the client's IANA time zone database cannot resolve now raises
  `DataError` naming the zone, with a hint to install `tzdata`, instead of
  logging a warning per value and returning a naive `datetime`. The session
  stays usable. This mostly affects clients without a time zone database
  (Windows without `tzdata`, minimal container images), where every region
  value, including the LTZ types' `UTC`, silently lost its zone. An offset
  that is malformed or not strictly within ±24 hours also raises `DataError`
  instead of `OperationalError('malformed response from broker')` with a
  closed connection. The explicit prepared API (`pycubrid.compat.native`)
  stays fail-closed, as in #492. Offsets, resolvable regions and an empty
  zone suffix decode as before. pycubrid now
  depends on `tzdata` on Windows only (`tzdata; sys_platform == 'win32'`).
  (#413)
- A region value in the repeated hour when daylight saving time ends now
  honors the abbreviation CUBRID sends: `America/New_York EST` at
  2026-11-01 01:30 decodes with `fold=1` (UTC-05:00) instead of the EDT
  instant an hour earlier. A missing or unknown abbreviation, or one both
  occurrences share, keeps `fold=0`. (#413)

### Changed
- Release workflow unified with the sibling repos: new `RELEASING.md`; `make release`
  replaced by the read-only `make release-check VERSION=x.y.z`; `publish-pypi.yml` is
  manual-dispatch only and now dispatches the cookbook smoke test after a successful
  publish (replacing `notify-cookbook.yml`); CI lints `CHANGELOG.md`.
- **PyPI publish fails closed on duplicate files (#494)** — `publish-pypi.yml` no longer
  passes `skip-existing: true`. The new stdlib-only `scripts/pypi_duplicate_guard.py`
  compares the SHA-256 of every verified file with the file PyPI already serves under the
  same name: an identical file (a partial upload recovered with `gh run rerun --failed`)
  is dropped from the upload, and a different hash or an unreachable PyPI fails the job.
  `RELEASING.md` documents the bounded recovery; offline tests cover the guard.
- **CI: releases happen automatically when a reviewed release PR is merged (#539)** —
  `prepare-release.yml` opens the `chore: release vX.Y.Z` PR (moves `[Unreleased]` into a
  dated section, bumps `__version__`, runs `make release-check`). On every push to `main`,
  the new `release.yml` decides from git facts only (`scripts/release_detect.py`: version
  changed against the first parent, dated CHANGELOG section, tag absent or at the same
  commit) and then runs, pinned to the merge SHA: release check, the full
  `integration-full.yml` matrix (now also a `workflow_call` workflow, no longer run on tag
  pushes), one build with SHA-256 hashes, the annotated tag, a draft GitHub Release with
  SBOM, the PyPI upload through the duplicate guard, and the cookbook verification of that
  exact version (`scripts/cookbook_wait.py`; "incomplete" without
  `COOKBOOK_DISPATCH_TOKEN`), with one run summary. `create-release.yml` and the manual
  `publish-pypi.yml` are removed; a narrow recovery dispatch (`resume`, `verify-only`,
  `dry-run`) remains. The CHANGELOG stays hand-curated.
- Ruff/Mypy pre-commit hooks are now `repo: local` / `language: system` hooks that
  invoke `python3 -m ruff`/`python3 -m mypy` from the active `.[dev]` environment
  instead of separately versioned mirror repos, so there is a single source of
  truth (the `pyproject.toml` dev pin) for each tool's version.
  `scripts/check_quality_tools.py` was updated to match. This fixes Dependabot's
  routine `pip`-ecosystem Ruff/Mypy bumps, which previously left the pre-commit
  hook revision stale and failed the quality-tool consistency gate (#476).

## [1.8.0] - 2026-09-29

### Upgrade notes
Behavior changes you may notice (details in the entries below):
- Native NOT NULL (`-631`) and invalid foreign-key (`-922`) violations now raise
  `IntegrityError` (SQLSTATE `23000`, still a `DatabaseError` subclass) instead
  of a generic `DatabaseError`. (#390)
- Fetching from an unfinished SELECT result after `commit()`/`rollback()`
  invalidated its handle now raises `InterfaceError` instead of silently
  returning a partial result as if exhausted. Already received rows remain
  readable; no replay or holdable-result guarantee is added. (#395)
- `cursor.description` `null_ok` was inverted and is now correct: `True` for
  nullable columns, `False` for NOT NULL/primary-key columns. (#431)
- SET/MULTISET/SEQUENCE columns report their collection type codes and decode
  as collections with `decode_collections=True` (raw bytes when disabled). (#430)
- Normal `commit()`, `rollback()` and autocommit requests keep the same CAS
  session, so isolation level and session variables survive transaction
  boundaries. (#468, #472)
- A session time zone set with `SET TIME ZONE` now also survives
  `commit()`/`rollback()`. On 1.7.x a transaction boundary could transparently
  reconnect and silently fall back to the server default zone, so
  `DATETIMELTZ`/`TIMESTAMPLTZ` values read after a commit came back in that
  zone (often `+00:00`). On 1.8.0 they come back in the session zone you set:
  the same instant with a different UTC offset. Compare instants rather than
  offsets or wall-clock fields if your code or expected output relied on the
  old offset. If the CAS itself closes the socket, the zone is lost like other
  SQL session state (see the next note). (#468, #472)
- If the CAS closed the socket after a transaction boundary (CAS restart,
  CHANGE CLIENT, `cubrid broker reset`), the driver probes with `CHECK_CAS`
  and reconnects once before the next request. Driver-owned settings (escape
  mode unless pinned, explicit autocommit) are restored; session state set with
  SQL (isolation, session variables) is not, so re-apply it. SQL bound for a
  replaced session is never sent to the new one; a retryable `OperationalError`
  is raised instead. Requests after a boundary cost one extra round trip. (#485)
- `commit()`/`rollback()` now close server handles held by unclosed cursors. In
  autocommit mode there is no such boundary: close cursors yourself, or their
  handles stay open until commit/rollback/close. (#485)
- `get_last_insert_id()` returns `None` instead of `""` when no identity is
  available; replace `value == ""` checks with `value is None`. (#381)
- Unknown connection keyword arguments now emit
  `pycubrid.UnknownConnectionOptionWarning` (still ignored otherwise). Use
  `warnings.simplefilter("error", pycubrid.UnknownConnectionOptionWarning)` to
  reject them. (#377)

New explicit, staged APIs (additive; ordinary 1.x connect/cursor behavior is
unchanged):
- `pycubrid.compat.native` and `pycubrid.compat.cubriddb` factories construct
  and close an owned sync connection (#465). `compat.native` also offers a sync
  prepared scalar cursor limited to INT32, UTF-8 CHAR and SQL NULL bindings with
  tuple-only rows (#439). Neither is full official-driver/DB-API parity; there is
  no async preparation, and `compat.cubriddb` provides no cursor execution.
- `Connection.fetch_schema_info()` / `close_schema_info()` (sync and async)
  eagerly fetch and close owned schema rows (#456). Handles are closed at
  transaction boundaries and are never reconnected or replayed. Live
  verification covers CLASS/VCLASS/ATTRIBUTE/CONSTRAINT/PRIMARY_KEY/
  IMPORTED_KEYS/EXPORTED_KEYS on CUBRID 10.2 and 11.4, not all schema codes
  (#457).

### Added
- Explicit `pycubrid.compat.native` sync prepared scalar cursor (#439): one
  physical-session-owned FC2 handle supports repeated typed FC3 execution of
  INT32, UTF-8 CHAR and SQL NULL, tuple-only row fetch, current-generation
  FC6 close, and connection commit/rollback result hooks. Pooling-off/unknown
  sessions fail before FC2. HOLDABLE SELECT results continue across commit
  and invalidate across rollback; ordinary sync/async FC41 remains unchanged.
  Live scalar, DML and 130-row multi-FETCH gates pass on CUBRID 10.2, 11.0,
  11.2 and 11.4. Pinned official-native comparisons on 10.2/11.4 match the
  selected non-NULL scalar/DML results; native `bind_param(None)` crashes and
  is a documented safety deviation, not a NULL parity pass. This is an
  additive MINOR subset, not full native/DB-API
  parity, public async preparation, effective settings, or a release.
- Internal FC2/FC3 scalar packet groundwork (#475) now serializes validated
  INT32, UTF-8 CHAR and SQL NULL bindings, preserves the authoritative FC2
  bind count, and parses refreshed FC3 column metadata before shard/FETCH.
  Error records fail closed. This has no public prepared cursor or owner
  lifecycle yet; ordinary sync/async FC41 literal execution is unchanged and
  #439 remains the public implementation gate.
- Construction-only official-driver compatibility factories (#465): explicit
  `pycubrid.compat.native` and `pycubrid.compat.cubriddb` namespaces validate
  CUBRID/UTF-8 DSNs, preserve the source's public/empty credential defaults and
  start one owned pure-Python sync connection with autocommit enabled. Wrapper
  aliases and close are available; cursor execution, prepared binding, sharing,
  configurable charset and HA are not. Ordinary 1.x defaults and async behavior
  are unchanged. MINOR/additive public surface, protected by the API baseline.
- Owned schema rows (#456): corrected FC9 requests/condensed metadata now ship with sync/async eager `fetch_schema_info(packet)` and idempotent `close_schema_info(packet)`. Existing getter positional arguments and raw packet fields remain; keyword-only `arg2=None` adds the second filter. Immutable original-session ownership prevents forged/retired handle RPCs; explicit transaction boundaries and auto-committing cursor statements/batches and version lookup with connection autocommit enabled close schema handles before the boundary, while connection teardown and I/O failures retire resources. Schema FETCH/CLOSE do not reconnect, replay, implicitly commit or return partial rows as success. Initial live coverage was CLASS/ATTRIBUTE on 10.2/11.4; the #457 live matrix extends it to CLASS/VCLASS/ATTRIBUTE/CONSTRAINT/PRIMARY_KEY/IMPORTED_KEYS/EXPORTED_KEYS, not all schema codes or native parity. MINOR/additive surface; API baseline regenerated.
- **Unknown connection options are now surfaced instead of silently ignored (#377)** — `Connection.__init__`/`AsyncConnection.__init__` read a fixed set of options out of `**kwargs` and discarded everything else without a word, so a typo such as `read_timout=30` or `connectTimeout=5` was accepted, had no effect, and gave the caller no signal. Any keyword outside the supported set now emits a new `pycubrid.UnknownConnectionOptionWarning` (a `UserWarning` subclass, **not** part of the PEP 249 exception hierarchy) naming the offending option, suggesting the closest supported spelling when there is one, and listing the full supported set. Known options behave exactly as before, and the warning is emitted before any socket work so a mis-spelled option is reported even when the connection then fails. It covers `pycubrid.connect()`, `pycubrid.aio.connect()`, and direct `Connection(...)`/`AsyncConnection(...)` construction, and points at the caller's own line rather than pycubrid's internals.

  A warning rather than a hard `TypeError` is deliberate: wrapper layers (connection pools, ORM dialects such as `sqlalchemy-cubrid`) legitimately forward extra keywords, so rejecting them would be a breaking change under `RELEASE_POLICY.md` §3 and cannot land on the 1.x line. Callers choose their own strictness with the standard `warnings` machinery — `warnings.simplefilter("error", pycubrid.UnknownConnectionOptionWarning)` to reject unknown options, `"ignore"` to silence them. Additive surface change (`api-baseline.json` regenerated).

### Documentation
- Define the bounded typed-CAS prepared binding design (#418) for a future
  sync-only compatibility scalar slice (#439): exact FC2/FC3/FC6 framing,
  session-owned handle/result states, explicit pooling-on evidence limits,
  and failing-first test IDs. This is design evidence, not a shipped prepared
  API, ordinary 1.x behavior change, or full native-parity claim.
- Verify owned CLASS/VCLASS/ATTRIBUTE/index/composite PK/FK schema rows on CUBRID 10.2/11.4 in both sync and async modes, with real multi-FETCH/close evidence. Correct schema examples to consume/close results and supply ATTRIBUTE's second filter; document row-order/qualifier/index-family boundaries without claiming native parity. (#457)
- Select a conservative additive compatibility design (#438): separate wrapper/native namespaces, unchanged ordinary 1.x/SQLAlchemy contracts, classified safe deviations and focused migration/acceptance boundaries. This design preceded the construction-only #465 slice and does not authorize a 2.0/default or release migration.
- Record the pinned official-driver source declaration inventory and reviewed assertion subcases in a scenario ledger, keeping unknown/duplicate candidates and execution evidence separate; validate candidate links against ledger declarations. This accounting does not certify functional parity. (#437)
- Private FC9 request/condensed-column groundwork (#455) preceded atomic getter activation and owned row consumption in #456. Its wire fixtures alone were not live-getter or native-parity certification.
- Add a source-referenced official-driver public API inventory and compatibility guide; catalog consistency checks do not certify functional parity. (#436)
- Added a README "First contribution" guide (with Korean translation) pointing newcomers to the right sibling repo for their first PR, and documented the `good first issue` → `status: in progress` label lifecycle in AGENTS.md.
- Acknowledge CUBRID/cubrid-python's reference test scenarios in the README, NOTICE and third-party provenance notes, with source links and explicit licensing-verification limits.
- Clarify contributor and maintainer review/label/translation responsibilities, validate populated standalone docs exceptions with executable event-JSON checks, and pin the two verified shared workflow callers. CI code/security/release gates and security support policy are unchanged.

### Fixed
- Recover when the CAS closes the socket after a transaction boundary, and
  release open query handles at END_TRAN (#485). Since #468 the session survives
  commit/rollback, but the CAS may still close the socket right after an OUT_TRAN
  reply (CAS memory restart at `APPL_SERVER_MAX_SIZE`, `cubrid broker reset`,
  CHANGE CLIENT with more clients than CAS processes), and the next request then
  failed with `OperationalError: connection lost during receive`. Sync and async
  connections now send one `CHECK_CAS` before a request that follows an OUT_TRAN
  reply, like JDBC `checkReconnect`. A live CAS keeps the same session, so
  session variables and isolation level still survive normal boundaries. Only a
  failed probe replaces the session, once per request and before that request is
  first sent: the escape mode is re-probed unless pinned and explicit autocommit is
  restored, and the replacement is verified once more before the request. Requests
  tied to the lost session are not sent to the new one (a CLOSE_REQ is skipped;
  FETCH, last-insert-id, LOB read/write and native prepared requests fail; a
  `lastrowid` lost after an autocommit INSERT is `None` and logged at WARNING),
  and cursors probe before rendering parameters, so SQL rendered for a session
  that is then replaced is rejected before send with the retryable
  `OperationalError`, never sent to the new session (sync and async). Async cursor
  FETCH/CLOSE_REQ requests whose handle another task's boundary released while
  they waited are no longer sent. No SQL is replayed. SQL-level session state
  of the lost CAS is not carried over, so layers that set isolation or session variables with SQL must
  keep re-applying them on a new session (sqlalchemy-cubrid#527). A failed
  replacement raises `OperationalError` and leaves the connection disconnected
  for `ping(reconnect=True)`. Commit and rollback first send `CLOSE_REQ` for
  handles still held by unclosed cursors, so server handles no longer accumulate
  until the CAS exceeds its memory limit; in autocommit mode there is no such
  boundary, so close cursors.
  Already received rows stay readable; unfinished results still raise
  `InterfaceError` (#395). Requests after an OUT_TRAN reply cost one extra round
  trip, including each statement in autocommit mode. This supersedes the #468
  entry's statement that only an explicit `ping(reconnect=True)` may recover:
  normal boundaries keep the session, and only a CAS that fails the probe
  triggers the automatic reconnect.
- Fence future prepared FC3/FC6 requests to their owning physical CAS
  generation inside the synchronous transport boundary (#478). A stale
  handle is rejected before send even when a replacement server reuses its
  number; uncertain post-send failure retires the session without replay.
  This is internal groundwork for #439, not a public prepared API or a
  change to the declared `threadsafety=1` contract.
- Re-probe automatically detected `no_backslash_escapes` on each new physical
  session, including explicit ping recovery (#471). Explicit `True`/`False`
  remains pinned; healthy same-session ping does not probe. Probe failure
  retires the replacement and returns `False` from ping, while direct connect
  raises. Async parameterized SQL bound against an older session generation is
  rejected before send, not silently rebound or replayed. PATCH correction;
  no dynamic `SET` or heterogeneous-failover guarantee. This supersedes the
  historical #264 note that recovery never re-probes.
- Treat `CAS_INFO[0]=0` as OUT_TRAN, not a released CAS session (#468). Normal
  commit, rollback, and autocommit requests keep the physical connection and
  session state; only an explicit `ping(reconnect=True)` may recover from a
  disconnected socket, a negative `CHECK_CAS` response, or a CHECK_CAS
  transport/protocol error. `ping(reconnect=False)` still reports failure without
  reconnecting. Other SQL is never replayed after an uncertain transport
  failure. Connection, API, architecture, and support documentation now
  describe this boundary consistently.
- Async schema FETCH now discards the session without sending CLOSE when
  `KeyboardInterrupt` or `SystemExit` interrupts a pending reply; the original
  interruption is preserved. Ordinary FETCH errors retain their cleanup behavior.
- Empty `bytes` LOB writes return `0` without a broker request after existing object, offset, connection and wire argument checks. BLOB/CLOB data and handles stay unchanged; nonempty ACK checks and existing bool/other-data paths are preserved. No new strict type policy or async LOB feature is introduced. (#394)
- Native syntax (`-493`), semantic (`-494`) and communication (`-671`) errors now carry their verified meanings: generic `ProgrammingError` / `42000` for parser errors, `OperationalError` / `08S01` for communication. Batch dispatch reuses known-code SQLSTATE lookup rather than discarding it in favor of a class default; unknown-code defaults are unchanged. Missing-table inference from `-493` alone is unsupported. (#391)
- Unfinished SELECT results invalidated by commit/rollback no longer silently look exhausted: sync and async fetch methods raise `InterfaceError` when another broker FETCH is required without a valid handle. Already received rows remain readable, fully buffered/exhausted results retain normal EOF, and reconnect-specific `OperationalError` stays distinct. No transparent replay or holdable-result guarantee is added. (#395)
- Native NOT NULL (`-631`) and invalid foreign-key (`-922`) errors now raise `IntegrityError` with SQLSTATE `23000` by code, independent of message language. Single-statement and batch paths preserve the native value in `code` and `errno`; the shared batch error helper no longer drops errno. Sync/async regressions verify constrained inserts and connection reuse after rollback. (#390)
- `cursor.description` now reports `null_ok=True` for nullable columns and `False` for NOT NULL/primary-key columns. The CAS byte is an `is_non_null` flag, previously interpreted backwards. Full per-type metadata and sync/async nullability regressions preserve existing size fields and collection codes. (#431, #398, #408)
- Collection column metadata retains CAS collection-kind flags instead of treating the element type as the column type. SET/MULTISET/SEQUENCE, including empty collections with a NULL element-type header, return their documented containers with `decode_collections=True`, or raw bytes when disabled, in both sync and async queries. Real-header regressions cover initial and subsequent fetches. (#403, #410)
- Development quality checks synchronize Ruff/Mypy hook revisions with the exact dev pins, reject installed-tool/configuration drift, and lint/format maintained scripts and demos through shared local/CI Make targets. The Mypy hook explicitly checks the package instead of running only stub installation. (#416)
- Full integration validation selects current pytest markers instead of filename globs. Normal, TLS, and nightly slow lanes cover the declared integration inventory, including concurrency stress; unknown skips and missing workflow paths fail the lane audit. TLS provisioning runs broker commands as the service owner. (#397)
- Integration CI now uses the shared CUBRID readiness probe with host/port connection fields and fails before running tests when all retries are exhausted. (#411)
- **`Connection.get_last_insert_id()` / `AsyncConnection.get_last_insert_id()` no longer return an ambiguous empty string after `commit()` (#381)** — cache the broker identity captured after INSERT so it survives commit/rollback and SELECT. Successful values remain strings; unavailable identities return `None`. A new INSERT attempt, nonempty batch, or physical connection change clears the cache; failed, empty, or malformed identity retrieval leaves it unavailable. The broker can report an earlier identity after a non-auto-increment INSERT, so an ID does not prove the current statement generated it or that a row exists after rollback. Migration: replace `value == ""` with `value is None` and check for `None` before `int(value)`; `cursor.lastrowid` remains `int | None`. This is a documented bug correction, not an annotation-only change.
- **Empty `executemany()` clears previous results (#376)** — sync and async cursors close any previous query handle and reset result state to `description=None`, `rowcount=0`, and `lastrowid=None`. No SQL is executed; query-close failures propagate without discarding the handle.
- Failed batch execution no longer exposes stale cursor result state (#375): sync and async executemany_batch clear prior result metadata, row counts, and last-insert IDs before the batch request, including per-statement, transport, and response-parse failure paths. If closing the previous query handle fails, no batch is sent and the handle remains tracked.
- **`Cursor.arraysize` now rejects non-integer values in sync and async cursors (#370).** Floats, booleans, and other non-integers raise `ProgrammingError` without changing the previous value; positive integers remain valid.
- **Batch execution closes an existing query handle (#374)** — `executemany_batch()` now releases an active server-side query handle before sending a batch request, matching `execute()` and preventing the prior result-set handle from leaking. Sync and async cursors keep the same behavior.
- Format very large integer parameters as decimal strings without converting them to floats, avoiding `OverflowError`. Float NaN/infinity rejection and boolean formatting are unchanged. (#368)

## [1.7.1] - 2026-09-18

### Fixed
- **`Lob.read(n)` no longer silently truncates large reads (#362)** — the CUBRID broker caps each `LOB_READ` response at a fixed size (~81908 bytes), so a single request returned a short buffer for any LOB larger than that, with no error (silent partial-read data loss). `Lob.read` now loops, advancing the offset by the bytes the broker actually returned, until the full requested length is collected or the broker signals end-of-LOB. As a side benefit, `read(0)` now short-circuits with no server round-trip (previously it triggered a server-side transaction abort).
- **create-release.yml: dropped `--target` from `gh release create`** — with an already-pushed tag (the normal tag-push trigger) `--verify-tag` already guarantees the tag exists, and passing `target_commitish` for an existing tag makes the Releases API return `422 Validation Failed`, so the first tag-triggered run of this workflow always failed. Verified live by the v0.4.0 tag attempt in cubrid-mcp-server.

### Documentation
- **Demo GIF embedded in README** — auto-generated terminal demo showing pip install → connect → query → zero dependencies. Rendered from `demos/pycubrid-demo.json` via `demos/render_gif.py`.
- **한국어 문서 페이지 — 배치 3 완결 (#317)** — TROUBLESHOOTING(1,246줄) 번역으로 13페이지 전체 완성. #317의 배치 작업 종료.
- **한국어 문서 페이지 — 배치 3 (Project 축, #317)** — DEVELOPMENT(개발 가이드) 번역 추가. TROUBLESHOOTING만 남음.
- **한국어 문서 페이지 — 배치 3 (Reference/Ops 축 1차, #317)** — SUPPORT_MATRIX·ARCHITECTURE·PERFORMANCE 번역 추가. TROUBLESHOOTING·DEVELOPMENT는 후속.
- **한국어 문서 페이지 — 배치 2 완결 (#317)** — API 참조(최대 문서, 1,298줄) 번역 추가로 Usage 축 전체(4페이지) 완성.
- **한국어 문서 페이지 — 배치 2 (Usage 축 2차, #317)** — CAS 프로토콜 참조 번역 추가. Usage 축 마지막(API_REFERENCE)은 후속 배치.
- **한국어 문서 페이지 — 배치 2 (Usage 축 1차, #317)** — PARAMETER_BINDING·TYPES의 한국어 번역을 `docs/ko/`에 추가. Usage 축 나머지(PROTOCOL·API_REFERENCE)는 후속 배치.
- **한국어 문서 페이지 — 배치 1/3 (#317)** — Getting Started 축 4페이지(quickstart·CONNECTION·EXAMPLES·faq)의 한국어 번역을 `docs/ko/`에 추가하고 Project → Translations → 한국어 문서로 노출. 나머지 9페이지(Usage/Reference/Operations/Project 축)는 후속 배치. 페이지 번역은 경고 수준 동기화, README.ko 하드 게이트 유지.
- **Korean/multi-language docs governance** — every `docs/README.<lang>.md` translation now carries a sync marker, and docs-sync gained a `translation-sync` job that fails a PR when `README.md` changes without any translation changing (escape hatch: the `translations-deferred` label).
- **Docs site information architecture unified across the ecosystem** — nav reorganized to the shared six-tab skeleton (Home / Getting Started / Usage / Reference / Operations / Project), the five README translations (ko/de/hi/ru/zh) are now reachable via Project → Translations (previously URL-only), palette unified to blue with search-suggest, and the homepage gains an Ecosystem section linking the three sibling sites.
- **CUBRID server license relationship documented; copyright notice unified (#309)** — `docs/ARCHITECTURE.md` gains a section stating the verified upstream licensing (server engine Apache-2.0, APIs/connectors BSD per CUBRID's `COPYING`; the often-cited GPL v2+ no longer applies) and that pycubrid is an independent wire-protocol client with no server code included or linked. `THIRD_PARTY_LICENSES.md` carries the same one-paragraph statement. LICENSE/NOTICE copyright lines now read `Yeongseon Choe, Gyeongjun Paik` (2025-2026), reflecting the two primary authors.

### Documentation
- **Added `THIRD_PARTY_LICENSES.md`** — pip-licenses-generated inventory of the development toolchain's licenses. pycubrid itself has zero runtime dependencies, so nothing in the table ships in the wheel. Documentation only.

## [1.7.0] - 2026-09-02

### Documentation
- **Added an Acknowledgments section and a `NOTICE` file crediting the CUBRID Node.js driver ([node-cubrid](https://github.com/CUBRID/node-cubrid), © 2008–2012 Search Solution Corporation, BSD-3-Clause)** — during pycubrid's initial development, node-cubrid was consulted as a reference implementation to understand CUBRID's CAS wire protocol (packet structure and function codes). This is recorded as an acknowledgment in `README.md`, `docs/README.ko.md`, and the new `NOTICE` file. Documentation only; no code or runtime behavior change.

### Added
- **DB-API 2.0 exception classes are now exposed as attributes on `Connection`/`AsyncConnection` (#282)** — PEP 249's optional extension recommends that the standard exception classes (`Warning`, `Error`, `InterfaceError`, `DatabaseError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`) be accessible as attributes of the `Connection` object, so multi-connection code can catch errors specific to a driver without importing its module. All ten classes are now set as class attributes on the shared `ConnectionCommonMixin`, so both sync `Connection` and `AsyncConnection` (and their instances) expose them with identity preserved — e.g. `conn.IntegrityError is pycubrid.IntegrityError`. Additive, backward-compatible surface change (`api-baseline.json` regenerated).
- **`Lob` now supports client-side lifecycle management — `close()`, context-manager (`with`), and a closed-state guard (#268)** — the CUBRID CAS wire protocol has **no** LOB release/free/close opcode (function codes are contiguous: `LOB_NEW=35`, `LOB_WRITE=36`, `LOB_READ=37`, `END_SESSION=38`), and the reference JDBC driver's `Blob.free()` is likewise purely client-side. LOB handles are therefore connection/session-scoped and reclaimed only when the owning connection/session closes. `Lob.close()` mirrors this: it is idempotent, performs **no** network I/O, and simply invalidates the Python object so subsequent `read()`/`write()` raise `InterfaceError("LOB is closed")`. `Lob` is now usable as a context manager (`__exit__` closes without suppressing exceptions). The `lob_handle`/`lob_type` properties remain readable after close for introspection. The class docstring documents the connection-scoped semantics.
- **`Lob` now rejects async connections instead of silently producing un-awaited coroutines (#266)** — `Lob` drives its connection *synchronously* (`_send_and_receive` with no `await`), but `AsyncConnection._send_and_receive` is a coroutine. A `Lob` bound to an `AsyncConnection` (via direct construction — `Lob` is exported) therefore called an async method without awaiting it, silently getting a coroutine object back and emitting "coroutine was never awaited" warnings. `Lob.__init__` and `Lob.create()` now detect an async connection (`inspect.iscoroutinefunction`) and raise `NotSupportedError` *before* any packet is sent; async LOB support is not implemented. `AsyncConnection.create_lob()` (previously absent, raising `AttributeError`) now exists and raises the same `NotSupportedError` for a clear, discoverable failure.
### Fixed
- **`TIMESTAMPTZ`/`TIMESTAMPLTZ` reads no longer crash with "malformed response from broker" (#289)** — `PacketReader._parse_timestamptz` decoded the timezone-aware timestamp types using the 7-short / 14-byte `DATETIMETZ` layout (which carries a millisecond field). But `TIMESTAMPTZ`/`TIMESTAMPLTZ` are **second-precision**: their wire layout is 6 shorts (12 bytes, no millisecond field) followed by the timezone string. The parser therefore over-read the first 2 bytes of the timezone string as a `ms` value and computed `ms * 1000`, which overflowed `datetime`'s "microsecond must be in 0..999999" range whenever those bytes were non-trivial (e.g. a leading `"As"` from `Asia/Seoul` = 16755 → 16755000 µs). The resulting `ValueError` surfaced as `OperationalError("malformed response from broker")`, making any `SELECT` of a `TIMESTAMPTZ`/`TIMESTAMPLTZ` column unreadable. `_parse_timestamptz` now reads the correct 12-byte second-precision layout (microsecond fixed to 0), and `DATETIMETZ`/`DATETIMELTZ` get their own `_parse_datetimetz` reader that keeps the 14-byte millisecond layout; a shared `_attach_timezone_suffix` helper parses the trailing timezone string for both. See [docs/TYPES.md](docs/TYPES.md).
- **`CUBRIDIsolationLevel.DEFAULT` corrected from `0x01` to `0x04` (READ COMMITTED) (#285)** — the enum advertised `DEFAULT = 0x01`, which aliases the lowest legacy pre-MVCC level (`COMMIT_CLASS_UNCOMMIT_INSTANCE`), not CUBRID's actual server default. CUBRID's default transaction isolation is **READ COMMITTED**, i.e. `REP_CLASS_COMMIT_INSTANCE = 0x04` (verified on live CUBRID 11.2: `GET TRANSACTION ISOLATION LEVEL` returns `4` on a fresh connection). `DEFAULT` now aliases `0x04`. The accompanying test that asserted `DEFAULT == 0x01` was fossilizing the wrong value and is corrected. A class-docstring note also records that CUBRID's MVCC engine (10.0+) only accepts levels `0x04`/`0x05`/`0x06`; the lower codes remain for protocol/back-compat reference only. This is a constant used for reference; the driver's connection logic does not consume it, so no runtime path changes.
- **Backslash-escape mode is now auto-negotiated from the server, fixing silent string corruption (#255)** — CUBRID's `no_backslash_escapes` system parameter defaults to `yes` (a backslash is an ordinary literal character), but pycubrid defaulted its client-side flag to `False` and unconditionally doubled backslashes. Against a stock server this silently corrupted data: `C:\temp\file` (12 chars) was stored as `C:\\temp\\file` (14 chars), and `regex \d+` became `regex \\d+`. `Connection`/`AsyncConnection` now probe the live server once at connect time with `SELECT CHAR_LENGTH('\\')` when `no_backslash_escapes` is not passed explicitly: a result of `2` selects literal mode (`True`, no doubling), `1` selects escape-processing mode (`False`), and any other value or a probe error raises `OperationalError` (see below). Passing `no_backslash_escapes=True|False` explicitly skips the probe. LIKE metacharacters (`%`, `_`) were never escaped and remain untouched. See [docs/PARAMETER_BINDING.md](docs/PARAMETER_BINDING.md#escape-mode-negotiation).
- **Async connection setup now matches the sync ordering and closes a negotiation race (#264)** — `AsyncConnection.connect()` applied the constructor's autocommit *before* negotiating the backslash-escape mode, and ran that negotiation probe **outside** the connection lock. This diverged from the sync driver (which negotiates *before* autocommit) and left a race window: a query issued on another task could execute before the escape mode was pinned, silently using the wrong escaping. Setup now runs the sync order exactly — connect → negotiate escapes → apply autocommit — behind a one-time setup gate that fences other tasks' public operations until the escape mode is pinned, while the setup task's own probe bypasses the gate (no deadlock) and a setup failure propagates to any waiting task. Transparent reconnect is unaffected and never re-probes.
- **Backslash-escape negotiation probe no longer leaves an open transaction on new connections (#292)** — `Connection`/`AsyncConnection` setup runs the `SELECT CHAR_LENGTH('\\')` probe in the default manual-commit mode, which opens a driver-owned transaction *before* the constructor's `autocommit` setting is applied. The probe never rolled that transaction back, so a freshly opened connection — including one created with `autocommit=True` — was handed to the caller with a lingering, driver-started transaction. Downstream this could cause CUBRID to reject a follow-up operation that requires a clean transaction boundary (e.g. SQLAlchemy setting `isolation_level` on a new pooled connection). Both the sync and async `_negotiate_backslash_escapes()` now roll back the probe transaction on *every* exit path — success, an unexpected probe result, or a probe error — via a best-effort `rollback()` in a `finally` block, so a failed negotiation cannot leave the driver-started transaction open either (passing `no_backslash_escapes` explicitly still skips the probe entirely and opens no transaction). In the async path the rollback is safe during setup because the setup-owning task bypasses `_wait_for_setup_if_needed()`. No public API change.
- **Backslash-escape negotiation no longer swallows a probe-transaction rollback failure (#300)** — the `finally` block that rolls back the `SELECT CHAR_LENGTH('\\')` probe transaction caught and discarded any rollback error (`except Exception: pass`). If the server-side rollback failed without closing the socket, the probe transaction could remain open and the connection was still handed back to the caller (or pool) in an unknown transaction state. Both sync and async `_negotiate_backslash_escapes()` now, on a rollback failure, close the connection so it can never be reused and raise `OperationalError` — unless a probe error is already propagating, in which case that original error is preserved (the connection is still closed). The success path is unchanged when rollback succeeds. No public API change.
- **Placeholder tokenizer is now CUBRID-dialect-aware (#265)** — `split_on_placeholders()` (which `bind_parameters()` uses to locate `?` placeholders) only understood ANSI single-quoted strings (`''` doubling), double-quoted identifiers, `--` line comments, and `/* */` block comments. A `?` inside CUBRID-specific lexical contexts was mis-detected as a real placeholder, throwing `wrong number of parameters` or interpolating into the wrong position. The tokenizer now also honours: backslash escapes inside single-quoted strings when the connection's `no_backslash_escapes` is `False` (so `\'` does not terminate the literal and `\\` is a literal backslash — `''` doubling stays valid in both modes; the standalone default `no_backslash_escapes=True` preserves the previous behaviour and matches CUBRID's server default), `//` C++-style line comments, and backtick (`` `id` ``) and bracket (`[id]`) quoted identifiers (first closing delimiter terminates). `bind_parameters()` now threads the connection's `no_backslash_escapes` through to the tokenizer for consistency with `escape_string()`. **Known limitation:** double quotes are treated as identifier delimiters, correct for CUBRID's default `ansi_quotes=yes`; SQL relying on `ansi_quotes=no` (where `"` delimits strings) is not specially handled, as pycubrid does not track that parameter.
- **Closed a `bool | None` type hole around the negotiated escape mode (#267)** — the cursor parameter-binding mixin declared `_connection: Any`, which masked from mypy that `Connection._no_backslash_escapes` is `bool | None` (unset until negotiated) while `bind_parameters()`/`format_parameter()` require a concrete `bool`. `CursorParamsMixin` is now `Generic` over the concrete connection type (bound to a minimal `_EscapeModeSource` protocol), `Cursor`/`AsyncCursor` specialise it (`CursorParamsMixin[Connection]` / `[AsyncConnection]`), and a new `_resolve_escape_mode()` narrows the value — raising `InterfaceError` if the mode is read before negotiation instead of silently mis-escaping. `AsyncCursor.__init__` is also tightened from `connection: Any` to `AsyncConnection`. No public behaviour change; mypy now catches misuse.
- **`Lob.read()` now rejects overlong server responses and invalid arguments (#268)** — `read()` returned `packet.lob_data` without checking it against the requested `length`, so a misbehaving server or corrupted transport could hand back more bytes than asked for. `read()` now raises `OperationalError` when the returned length exceeds the requested length, and both `read()`/`write()` validate that `offset` (and, for `read`, `length`) are non-negative, raising `InterfaceError` before any packet is sent.
- **`format_parameter` now rejects the Ctrl-Z (`0x1A`) byte in string parameters instead of emitting a raw control byte (#271)** — `escape_string()` previously prefixed `0x1A` with a backslash while leaving the raw control byte in the literal, even though CUBRID's SQL grammar defines **no** safe literal escape for it (there is no MySQL-style `\Z`, and the escape had no effect under CUBRID's default `no_backslash_escapes=yes`). Embedding `0x1A` is now rejected with `ProgrammingError("string parameter contains Ctrl-Z (0x1A) byte")` in **both** escape modes, consistent with the existing null-byte rejection. **Behavior change:** strings containing `0x1A` that previously produced a (malformed) literal now raise at bind time. The issue's two other claims were investigated and **rejected as invalid** against the CUBRID manual: `TIME` has second resolution (its literal grammar allows only `'HH:MI:SS'`), so dropping microseconds is correct; and CUBRID accepts scientific/exponential numeric literals (an `E`-form number is parsed as `DOUBLE`), so `str()`'s exponent output (e.g. `1e+20`) is a valid literal. Both are now pinned by tests.
- **`_extract_first_keyword()` now strips CUBRID C++-style `//` line comments (#256)** — the leading-comment stripper used by `executemany()` to detect the statement's DML verb only skipped `/* */` block comments and `--` ANSI line comments, so a statement prefixed with a `//` line comment (a valid CUBRID comment style) had its verb mis-detected and could be excluded from batch execution. `_RE_LEADING_COMMENTS` now also skips `//` line comments to EOL/EOF, keeping it consistent with the comment styles already honoured by `split_on_placeholders()` (#265).

### Changed
- **Public escape helpers now share a single `no_backslash_escapes=True` default (#293)** — `escape_string()`, `format_parameter()`, `bind_parameters()`, and `CursorParamsMixin._escape_string()` defaulted the keyword to `False` while `split_on_placeholders()` already defaulted to `True`. A caller that reached for one helper but omitted the keyword could silently get the opposite (non-server-consistent) escaping behavior from another. All five helpers now default to `True`, matching CUBRID's server default (`no_backslash_escapes=yes`, a backslash is an ordinary literal character). Internal driver paths always pass the connection's negotiated value explicitly, so this is a no-op for driver-mediated binding; it only affects code that calls these helpers directly without the keyword, which now gets the correct literal-backslash behavior. A regression test pins the shared default. See [docs/PARAMETER_BINDING.md](docs/PARAMETER_BINDING.md).
- **Ruff lint rule selection now declared explicitly (#247)** — `pyproject.toml` configured ruff but never set `[tool.ruff.lint] select`, so `ruff check` inherited ruff's implicit defaults. Ruff expanded that default set in 0.16 (59 → 413 rules against this repo's config), which is why #245 (`0.15.22 → 0.16.1`) failed lint with 237 errors in untouched code. Pinning the ruff *version* in #236 stopped unpinned installs from drifting, but could not survive the bump itself — the rule set is now pinned too, via `select = ["E4", "E7", "E9", "F"]`, which is exactly what ruff selected by default through 0.15.x (same 59 rules under both versions).
- **Backslash-escape negotiation now fails loud instead of silently defaulting to `False` (#263)** — when the `SELECT CHAR_LENGTH('\\')` probe raises or returns an unexpected length (neither `1` nor `2`), `Connection`/`AsyncConnection` previously set `no_backslash_escapes=False` (the legacy value) and logged a warning. Because the CUBRID default maps to `True`, that silent fallback could pick the *wrong* escaping mode and corrupt string literals (or enable SQL injection). Negotiation failure now raises `OperationalError`; pass `no_backslash_escapes` explicitly to skip detection when the probe cannot run. **Behavior change:** connections that previously succeeded with a mis-detected mode now raise at connect time.
- **Corrected the `Documentation` project URL and added a `Changelog` URL in packaging metadata (#269)** — `[project.urls].Documentation` pointed at the GitHub source tree (`.../tree/main/docs`) rather than the published docs site; it now points to `https://cubrid-lab.github.io/pycubrid/` (matching the README badge and `mkdocs.yml`). A `Changelog` URL (`.../blob/main/CHANGELOG.md`) was also added, mirroring sqlalchemy-cubrid. Packaging metadata only; no code or runtime behavior change.
- **Renamed the example table `cookbook_users` to `users` consistently across all documentation surfaces (#270)** — the placeholder name appeared in 81 places across `README.md`, all five translated READMEs (`docs/README.{ko,zh,hi,de,ru}.md`), `docs/CONNECTION.md`, `docs/EXAMPLES.md`, `docs/TYPES.md`, and the generated `docs/llms-full.txt`. Documentation only; no code or runtime behavior change.
- **Binding a Python collection as a single parameter now raises an actionable error message (#287)** — passing a `list`, `tuple`, `set`, `frozenset`, or `dict` as one bound value previously raised the generic `ProgrammingError("unsupported parameter type")`. It now raises `ProgrammingError` with a message that names the collection case and points at the fix: pycubrid does **not** auto-expand `IN (?, ?, ...)`, so placeholders must be expanded explicitly in the SQL. The exception class is unchanged (still `ProgrammingError`) and the message text remains non-contractual; other unsupported types keep the generic message. See [docs/PARAMETER_BINDING.md](docs/PARAMETER_BINDING.md#type-mapping-guarantees).

## [1.6.2] - 2026-08-06

### Fixed
- **Write-path serialization overflow now raises `DataError`, not raw `struct.error` (#223)** — `_send_and_receive` (sync) and `_do_send_and_receive` (async) called `packet.write(self._cas_info)` inside a block that only caught `OSError`. An oversized outbound value (e.g. a huge LOB offset/length, or a large `executemany` batch) trips `struct.pack`'s int32 range check and raised a bare `struct.error`, escaping the PEP 249 contract entirely — the write-side counterpart of the parse-side hardening already done in #201/#205. Both `packet.write()` call sites now catch `struct.error` and raise `DataError("parameter value too large to serialize into CAS request")`; since nothing was sent to the socket yet, the connection is left open and usable rather than torn down.
- **`AsyncConnection(autocommit=True)` no longer silently dropped (#224)** — constructing `AsyncConnection` directly (bypassing the `pycubrid.aio.connect()` factory) with `autocommit=True` swallowed the flag into `**kwargs` with no error and no effect, unlike sync `Connection`, which has always accepted `autocommit` in its own constructor. `AsyncConnection.__init__` now accepts a keyword-only `autocommit: bool = False` parameter, applied via `await self.set_autocommit(True)` the first time `connect()` completes. The `pycubrid.aio.connect()` factory no longer needs its own separate `set_autocommit()` call — `autocommit` just flows straight through to the constructor now.
- **`Decimal('NaN')`/`Decimal('Infinity')` now rejected like their `float` equivalents (#225)** — `format_parameter()`'s `Decimal` branch returned `str(value)` unconditionally, before ever reaching the NaN/Inf guard that already protects the `int`/`float` branch below it. A `Decimal('NaN')` or `Decimal('Infinity')` parameter was silently formatted as the bare token `NaN`/`Infinity` and sent straight to the server instead of raising the documented `ProgrammingError("nan and inf are not supported by CUBRID")`. The `Decimal` branch now checks `value.is_nan() or value.is_infinite()` first.
- **NUMERIC field parsing no longer leaks `decimal.InvalidOperation` (#231)** — `PacketReader._parse_numeric` constructed a `Decimal` straight from the wire string with no guard. An empty or corrupt NUMERIC field raised `decimal.InvalidOperation` (an `ArithmeticError`, not a `ValueError`), slipping past the malformed-response handlers in both sync and async `_send_and_receive`. This left the socket open and desynced for reuse. `_parse_numeric` now catches `InvalidOperation` and re-raises as `ValueError`, so parse errors flow through the existing handler: socket closed, `_connected = False`, `OperationalError` raised with cause chained.
- **CI lint now uses pinned ruff version (#236)** — the lint job used `pip install ruff` (unpinned), which installed the latest ruff. When ruff 0.16.x introduced new rules, every PR started failing lint. Now installs from `.[dev]` extras to match the pinned `ruff==0.15.22` in `pyproject.toml`.

## [1.6.1] - 2026-07-18

### Fixed
- **`executemany()` batch error handling (#186)** — `executemany_batch()` in both sync `Cursor` and `AsyncCursor` consumed `packet.results` but never checked `packet.errors`, silently swallowing per-statement batch failures. Partial failures (e.g. one INSERT in a batch of 10 hits a unique constraint violation) were invisible to the caller — data integrity risk. Now raises the first batch error using the same CAS error code dispatch as `protocol._raise_error()` (PR #208), mapping to the correct PEP 249 exception class (IntegrityError, ProgrammingError, OperationalError, etc.).
- **DATA_LENGTH broker response validation (#188)** — `_send_and_receive()` in both sync `Connection` and `AsyncConnection` unpacked the 4-byte `DATA_LENGTH` header and immediately allocated `bytearray(data_length)` without bounds checking. A malformed or hostile broker response with a negative value would raise a raw `ValueError` from `bytearray()`, and an oversized value could trigger unbounded memory allocation (OOM). Added `_validate_data_length()` that rejects negative values and values exceeding `DataSize.MAX_PACKET_SIZE` (256 MiB) with a clean `OperationalError`, applied at both the handshake and main send/receive paths.


## [1.6.0] - 2026-07-18

### Fixed
- **Cursor memory bounding for large result sets (#203, PR #207)** — both sync `Cursor` and `AsyncCursor` previously accumulated the entire result set in `_rows` via `_rows.extend(packet.rows)` on every server fetch. Iterating a 10K-row result set via `fetchone()` would buffer all 10K rows in memory even though only one row was needed at a time. Fixed by decoupling the server-side fetch position (`_fetched_count`, tracking total rows received) from the local buffer cursor (`_row_index`, indexing into `_rows`). `_fetch_more_rows()` now REPLACES the buffer with the new batch instead of extending it, keeping memory bounded by the configurable `fetch_size` (default 100). `fetchall()` additionally clears the buffer after consuming all rows. This fixes a latent dual-purpose bug where `_row_index` was used both as buffer index AND server fetch position, which would have caused infinite re-fetching if any naive trim scheme had been applied.
- **CAS error code dispatch (#204)** — `_raise_error()` in `protocol.py` previously classified exceptions by text substring matching (looking for keywords like "unique", "syntax", "duplicate" in the error message). This was fragile across CUBRID versions. Replaced with deterministic CAS error code dispatch: `CAS_ERROR_TO_EXCEPTION` mapping in `error_codes.py` maps 16 specific codes to the correct PEP 249 exception class (IntegrityError, ProgrammingError, OperationalError, InternalError, DataError). Text heuristics are now only used as a fallback for code -1 (ER_DBMS passthrough), where CAS wraps server-engine errors with a generic code. Also fixed a duplicate `return error_message` statement in `_add_error_hints()`.
## [1.5.1] - 2026-07-18

### Fixed
- **Sync `_send_and_receive` parse-error exception parity (#201)** — the sync connection path at `connection.py:_send_and_receive` previously caught only `OSError`, meaning malformed CAS broker responses that raised `struct.error`, `ValueError`, `IndexError`, or `UnicodeDecodeError` would bypass socket cleanup and leave the connection in a dirty state for reuse. The async path at `aio/connection.py:615-621` already handled these. Ported the full exception catch list to the sync path so parse errors now close the socket, mark `_connected = False`, and raise `OperationalError("malformed response from broker")` with the original cause chained.
- **LOB write server ACK verification (#202)** — `Lob.write()` returned `len(data)` unconditionally without checking whether the server actually wrote all the bytes. Under disk-full / quota-exceeded conditions, the server could write fewer bytes and the caller would never know — silent data truncation. `LOBWritePacket.parse()` now extracts `bytes_written` from the CAS response (the response code doubles as the byte count on success, matching `LOBReadPacket`'s existing pattern). `Lob.write()` compares `bytes_written` against `len(data)` and raises `OperationalError` on mismatch.


## [1.5.0] - 2026-05-23

### Policy
- **`RELEASE_POLICY.md` added; public API surface is now CI-gated.** The
  project's previously implicit 1.x semantic-versioning contract is now an
  explicit, machine-checkable document at the repository root. The CI workflow
  gains a `compat-check` job that runs `scripts/check_public_api.py` against
  the committed `api-baseline.json`; any change to the public surface
  (functions, classes, methods, parameters of `pycubrid.connect`,
  `pycubrid.aio.connect`, `Connection`, `Cursor`, `AsyncConnection`,
  `AsyncCursor`, `Lob`, exception classes, and the PEP 249 contract constants
  `paramstyle`/`apilevel`/`threadsafety` whose literal values are part of the
  contract) fails CI unless the developer regenerates the baseline in the same
  change, surfacing the surface diff for explicit human review. Type objects
  (e.g. `STRING`, `BINARY`) are tracked by their presence and type name only —
  identity-level changes are intentionally **not** flagged, per
  `RELEASE_POLICY.md` §2 "What the gate does *not* detect". The 1.2.0
  minor-release breaking change (removal of `Mapping` parameter style from
  `_bind_parameters`) is acknowledged in `RELEASE_POLICY.md` §4 as a historical
  violation rather than silently rewritten; the new gate exists to prevent any
  recurrence.
  README status line updated from "Beta" to "Stable (1.x)" across English
  and all five translated READMEs to align with the 1.0.0 declaration and the
  `Production/Stable` PyPI classifier already shipped since 1.0.0.

### CI
- **Cross-platform CI**: offline tests now run on **Ubuntu + Windows + macOS** (Python 3.10–3.14 matrix, 15 OS×Python combinations). pycubrid is pure Python so the code was already cross-platform — this proves it.
- **Per-PR `integration-tls` lane added** — `ci.yml` now includes an `integration-tls` job (Python 3.14 × CUBRID 11.4) that runs on every PR touching `pycubrid/aio/**`, `pycubrid/connection.py`, `pycubrid/_connection_common.py`, or `.github/workflows/**`. Mirrors the broker-provisioning logic from `integration-full.yml` so TLS regressions are caught BEFORE merge instead of waiting for the nightly full matrix. Path-gating is implemented via `dorny/paths-filter@v3.0.2` (SHA-pinned), and the `ci-gate` job treats `integration-tls.result == 'skipped'` as acceptable when no TLS-relevant files changed (closes #159).
- **TLS readiness probe now verifies the broker certificate** — both probe scripts in `integration-full.yml::integration-tls` previously called `ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE`, which silently accepted any TLS endpoint on `localhost:33000` and weakened the regression signal. The probes now use the verified default context built from `CUBRID_TLS_TEST_CA_FILE` directly; a misconfigured or untrusted broker cert fails the probe loudly (closes #157).
- **TLS skip allow-list tightened to full pytest node id** — the `grep -v 'test_aio_ssl_handshake_failure'` filter in `integration-full.yml` is replaced with the full nodeid `tests/test_aio_ssl_integration.py::test_aio_ssl_handshake_failure`. A future rename of the test no longer silently re-introduces the "unexpected skip" bug (closes #159).
- **Automated `SSL=ON` CUBRID broker provisioning** — `integration-full.yml` now includes an `integration-tls` job (Python {3.10, 3.14} × CUBRID 11.4) that starts a manually-managed CUBRID container, flips `BROKER1 SSL=OFF` → `SSL=ON`, extracts the broker's self-signed certificate, probes the TLS handshake, and runs `tests/test_aio_ssl_integration.py` with `CUBRID_TLS_TEST_*` env vars wired up. The job fails loudly if any TLS test is skipped, ensuring the live TLS path is exercised on every nightly/tag-push run instead of silently skipping (closes #147, #155)

### Fixed
- **Async TLS verification failures now surface promptly on Python 3.10** — `AsyncConnection._do_connect_handshake` now runs a narrow Python-3.10-only **preflight TLS verification probe** in the default executor immediately before `loop.start_tls()`. The probe opens a separate TCP socket to the same effective endpoint (replaying the `CUBRS` handshake on the no-redirect path, going straight to TLS on the redirect path), then performs a synchronous `ssl.SSLContext.wrap_socket()` with the **same** `SSLContext` and `server_hostname=self._host` as the real upgrade. Any `ssl.SSLError` propagates as `OperationalError`, matching the 3.11+ failure surface. Works around the known CPython 3.10 `asyncio` bug (gh-142352 family, fixed in 3.13/3.14) where `loop.start_tls()` hangs indefinitely on TLS-handshake-internal verification failures because `ssl_handshake_timeout` only bounds peer-unresponsive hangs. No-op on Python 3.11+; the 3.10 path incurs one extra TCP round-trip per connect (closes #156).
- **TLS handshake now matches CUBRID's STARTTLS-style upgrade** — both sync and async `connect()` previously wrapped the socket in TLS before any bytes were exchanged, which never worked against a real `SSL=ON` CUBRID broker. The driver now (1) opens a plaintext TCP socket, (2) sends the 10-byte ClientInfoExchange handshake using the SSL magic string `"CUBRS"` (vs `"CUBRK"` for plain), (3) reads the 4-byte broker status (negative codes now raise `OperationalError` instead of silently falling through), (4) reconnects to the redirected CAS worker on `new_connection_port > 0` without re-handshaking (matches upstream JDBC `BrokerHandler.connectBroker`), and (5) upgrades the connection to TLS before sending `OPEN_DATABASE`. The async path uses `loop.start_tls()` for Python 3.10 compatibility. Validated end-to-end against CUBRID 11.4 with `SSL=ON` and a self-signed broker certificate (#154)

### Documentation
- **Parameter binding contract documented** — added `docs/PARAMETER_BINDING.md` formalizing the driver-side literal-binding semantics for 1.x: per-type SQL-literal mapping (with `_cursor_common.py` line citations and pinned tests), the `escape_string` default and `no_backslash_escapes` modes (NUL rejection, single-quote doubling, backslash and CR/LF/`\x1a` handling), the placeholder tokenizer's behavior across quoted strings/identifiers/line and block comments, and the explicit non-guarantees (no server-side prepared statements, identifiers are not escaped, no `IN`-clause expansion, exception-message text is not contract). Linked from `README.md` and `docs/index.md`.
- **TLS/handshake documentation aligned with implementation** — corrected `AGENTS.md` protocol version (8/10.2) and OpenDatabase payload framing, fixed OpenDatabase response field order across `CONNECTION.md`/`ARCHITECTURE.md`, rewrote the CAS reconnection diagram to reflect actual `connect()` re-entry through the broker, corrected the `MAGIC_STRING_SSL` constant name in `PROTOCOL.md`, surfaced TLS 1.2 minimum on sync rows in `SUPPORT_MATRIX.md`, added a new "Async TLS Handshake Hangs on Python 3.10" troubleshooting section, and added the Python 3.10 async TLS caveat to `README.md` and all five translations (#160, #163)
- **TLS docs polish** — added TLS examples to `EXAMPLES.md`, an SSL/TLS TOC entry to `CONNECTION.md`, a Transport Security section to `SECURITY.md`, local TLS integration-test instructions to `CONTRIBUTING.md`, expanded `Connection.connect`/`AsyncConnection`/`_do_connect_handshake` docstrings with the STARTTLS flow, added a TLS field to the bug-report issue template, expanded `pyproject.toml` keywords, and added a `make integration-tls` target. Resolves remaining items from the TLS Phase 4 audit (#161, #162)
- **Async TLS handshake hang on Python 3.10 documented as a known limitation** — a known CPython asyncio TLS handshake bug on Python 3.10 causes `loop.start_tls()` to hang on cert-verify failures on 3.10 only (fixed in 3.13/3.14); pycubrid documents the workaround and skips the negative-path test on 3.10 (#156)

### Tests
- **Async TLS upgrade and handshake paths covered offline** — added `tests/test_aio_ssl_offline.py` with 7 mocked tests guarding the regressions enumerated in #158: `_upgrade_to_tls()` argument forwarding (incl. `ssl_context`, `server_hostname=self._host`, `ssl_handshake_timeout` with the documented 10-second default), `loop.start_tls()` failure cleanup (`old_transport.abort()` exactly once, exception re-raised unchanged, defensive `None`-return handling), incomplete-read and EOF on the initial 4-byte `CUBRS`/`CUBRK` broker status response, and async parity for `test_connect_redirect_sends_no_second_handshake` (redirect during TLS connect reconnects on the new port **without** a second handshake). These run without a broker so a regression silently disabling hostname verification or re-introducing a double-handshake no longer slips through offline CI (closes #158).
- **Sync/async lifecycle parity coverage expanded** — integration parity tests now share adapter-driven scenarios and cover connection lifecycle APIs including `ping()`, CAS-inactive reconnect, auto-commit transitions, insert identity helpers, batch rowcount semantics, close ordering, and the explicit `AsyncConnection` `create_lob` `AttributeError` contract (closes #140)
- **Async TLS integration coverage** — added `tests/test_aio_ssl_integration.py` with
  live async TLS success/failure/reconnect/shutdown coverage gated behind an
  explicitly configured TLS-enabled broker (#155)

### Validated
- **Native `Connection.ping()` causally validated at application layer** — Tier 2 ORM benchmark in [cubrid-benchmark `2026-04-22_native-ping-hotpath`](https://github.com/cubrid-lab/cubrid-benchmark/tree/main/experiments/orm-overhead/runs/2026-04-22_native-ping-hotpath) (paired same-version A/B vs forced `SELECT 1`, 7 trials, bootstrap 95% CI) confirms native CHECK_CAS ping is **+279.9% throughput** on raw ping_only [+278.0, +283.9] and **+587.8% on SQLAlchemy `checkout_only`** [+581.8, +603.8] with `pool_pre_ping=True`. Performance Loop ping propagation gap closed.

### Added (transport contract lock — #167)
- **Session state restoration on transparent reconnect** — When the broker
  signals ``CAS_INFO_STATUS_INACTIVE`` and pycubrid reconnects transparently
  (matching JDBC's ``UClientSideConnection.checkReconnect``), any session
  setting the caller has **explicitly** set on the connection (currently
  ``autocommit``) is now re-emitted on the new CAS worker via
  ``SetDbParameterPacket``. Settings the caller has never touched are left at
  the broker default to avoid spurious round-trips. The same restoration runs
  on the ``ping(reconnect=True)`` recovery path. Restore failures tear down
  the connection and chain the underlying transport error via PEP 3134
  ``__cause__`` so callers can diagnose them. Both sync ``Connection.ping()``
  and async ``AsyncConnection.ping()`` attempt the reconnect+restore at most
  **once per call** — the preflight ``_check_reconnect`` runs first and the
  ``CHECK_CAS`` request is sent with ``allow_reconnect=False`` so a restore
  failure cannot trigger a second attempt via ``_send_and_receive`` (PR #3
  Item 1).
- **Mid-fetch reconnect raises ``OperationalError``** — When the broker
  releases the CAS worker while a cursor still has rows pending on the
  server, the server-side query handle is no longer valid. Cursors now mark
  themselves as reconnect-invalidated and ``fetchone``/``fetchmany``/
  ``fetchall`` raise :class:`OperationalError` (``result set lost due to
  broker reconnect mid-fetch``) once the buffered rows are exhausted, instead
  of silently returning a truncated result set. Rows already buffered in the
  cursor remain accessible. ``execute()`` and ``close()`` reset the
  invalidation flag (PR #3 Item 2).

### Fixed
- **PEP 3134 ``__cause__`` preserved on async transport timeouts** —
  ``AsyncConnection._connect_locked`` (handshake timeout) and
  ``AsyncConnection._send_and_receive_locked`` (read timeout) now use
  ``raise OperationalError(...) from exc`` instead of ``from None``, so the
  underlying ``asyncio.TimeoutError`` is preserved on the chained exception
  for diagnostic tooling (PR #3 Item 3).

### Documentation
- **Reconnect contract documented in ``docs/CONNECTION.md``** — added a
  "Session-state restoration on transparent reconnect" section listing which
  settings are restored and which are not, plus a correction to the
  ``autocommit`` default note: pycubrid sends ``auto_commit`` per-statement
  on every ``PrepareAndExecute``, so the broker's own ``CUBRID_AUTO_COMMIT``
  setting is effectively overridden by the driver-side value.
- **Cursor mid-reconnect behaviour documented in ``docs/API_REFERENCE.md``** —
  ``fetchone``/``fetchmany``/``fetchall`` now document the
  :class:`OperationalError` raised when a transparent reconnect invalidates
  a partially-consumed result set.
- **Python 3.10 async-TLS caveat citation normalized** — the upstream issue
  reference (``gh-142352``) was removed from ``README.md``, all five README
  translations (``docs/README.{ko,zh,hi,de,ru}.md``), ``SECURITY.md``,
  ``CHANGELOG.md``, ``CONTRIBUTING.md``, ``docs/CONNECTION.md``,
  ``docs/TROUBLESHOOTING.md``, ``docs/DEVELOPMENT.md``, ``docs/EXAMPLES.md``,
  ``docs/SUPPORT_MATRIX.md``, ``tests/test_aio_ssl_integration.py``, and
  ``pycubrid/aio/connection.py`` because that issue describes a different
  ``start_tls()`` regression on 3.13/3.14/3.15 (PROXY-protocol buffered-data
  loss), not the 3.10 cert-verify hang pycubrid observes. The caveat is now
  described as a "known CPython async-TLS handshake bug on Python 3.10"
  tracked as pycubrid #156.

### Tests
- **12 new tests in ``tests/test_network_edge_cases.py``** — four new test
  classes covering: ``__cause__`` chaining on sync/async transport timeouts,
  explicit/unset session-state restore on reconnect (sync + async),
  restore-failure tear-down, mid-fetch ``OperationalError`` (sync + async),
  ``execute``/``close`` resetting the invalidation flag, and
  ``CancelledError`` propagation in ``AsyncConnection._close_streams``. Total
  offline tests: 858.

## [1.4.0] - 2026-05-13

### Added
- **TLS/SSL support for async connections** — `AsyncConnection` now supports `ssl=True`, `ssl=False`, or `ssl=ssl.SSLContext(...)` via `asyncio.open_connection(ssl=...)` with `StreamReader`/`StreamWriter` transport (#129, #136)
- **`mypy --strict` CI gate** — typecheck job added to CI workflow to enforce strict typing (#130)
- **Sync/async parity integration tests** — expanded test coverage for bytes, datetime, fetch_size, JSON, and edge-case scenarios (#134)

### Changed
- **`ConnectionCommonMixin` extracted** — deduplicated ~70% of shared logic between `Connection` and `AsyncConnection` into a common mixin (#133, #135)
- **`CursorParamsMixin` extracted** — eliminated sync/async cursor parameter-handling duplication (#123, #127)
- **Driver-side binding semantics documented** — README, ARCHITECTURE, and PRD updated to clarify that `?` placeholders are interpolated locally, not via server-side prepared statements (#131)

### Fixed
- **`fetch_size` validation** — `Connection` and `AsyncConnection` constructors now reject non-positive `fetch_size` values (#132)
- **Dead `backports.zoneinfo` fallback removed** — eliminated unused Python 3.8 compatibility code
- **Typed locals in async module** — replaced `str()` coercion with properly typed local variables
- **`mypy --strict` errors resolved** — full strict-mode compliance across the codebase
- **`asyncio.run()` for Python 3.14** — replaced deprecated `get_event_loop()` usage

## [1.3.2] - 2026-04-21

### Added
- **Native async `AsyncConnection.ping()`** using `CHECK_CAS` (FC=32) for lightweight CAS-level liveness checks. Native `CHECK_CAS` now performs a round trip regardless of `CAS_INFO` status, while `reconnect=False` suppresses implicit broker-handoff reconnect via `_send_and_receive(..., allow_reconnect=False)` (#95, #70)

### Fixed
- **Sync `Connection.ping(reconnect=False)` now honors broker handoff correctly** — native `CHECK_CAS` runs regardless of `CAS_INFO` status, while `reconnect=False` suppresses implicit broker-handoff reconnect via the new `_send_and_receive(..., allow_reconnect=False)` flag (#95, #70)

## [1.3.1] - 2026-04-21

### Documentation
- **Oracle audit fixes completed** — documentation gaps from the Oracle review were closed across the main guides, with no runtime or public API changes in `pycubrid/`.
- **Driver-level timing hooks documented** — `enable_timing=True` keyword and `PYCUBRID_ENABLE_TIMING` environment variable, `Connection.timing_stats` property, and the `TimingStats` accumulator are now covered in `docs/API_REFERENCE.md` and `docs/PERFORMANCE.md` (closes #16). The implementation has shipped since 1.0.0; this completes the "API documented" acceptance criterion.
- **Async parity wording clarified** — sync vs. async capability differences are now described consistently, including async-specific wording cleanups in the Korean docs.
- **`executemany()` guidance expanded** — bulk operation documentation now explains `executemany()` behavior and usage more clearly.
- **README translations synchronized** — Korean, German, Russian, Chinese, and Hindi READMEs were refreshed to match the current English documentation.

## [1.3.0] - 2026-04-20

### Added
- **SSL/TLS support for sync connections** — `ssl=True` (verified context), `ssl=False`/`None` (disabled), or `ssl=ssl.SSLContext(...)` for custom config on `pycubrid.connect()` (#85)
- **Reconnect / network edge case test suite** — 17 tests covering connection reset, timeout, broken pipe, partial reads, reconnect-after-failure (#87)
- **Concurrency stress tests** — threaded (16 workers × 25 inserts, 32 readers) and asyncio.gather (16 workers, 32 readers) with own-Connection isolation
- **Standalone version check script** — `scripts/check_version.py` AST-based pyproject/`__init__.py` consistency check, replaces fragile inline grep in CI (#88)
- **PyPI classifiers** — `Operating System :: OS Independent`, `Typing :: Typed`, `Programming Language :: Python :: 3 :: Only` (#89)
- **Character encoding documentation** — UTF-8-only contract documented in `docs/CONNECTION.md` (#86)

### Fixed
- **PEP 639 license conflict** — removed redundant `License ::` classifier; SPDX `license = "MIT"` is the single source of truth (follow-up #89)
- **`test_ping_reconnect_also_fails` dual-stack fragility** — patches `socket.create_connection` instead of `socket.socket`

### Deferred
- **#90 Sync/async deduplication** — refactor deferred per Oracle review (high regression risk vs. maintainability gain)

### Async SSL
SSL/TLS for async connections raises `NotSupportedError` — `asyncio.loop.sock_*` APIs reject `SSLSocket`. Use the sync interface for TLS, or async without encryption. Tracked for future asyncio integration.

## [1.2.0] - 2026-04-19

### Added
- **Native `Connection.ping()`** using CHECK_CAS (FC=32) — lightweight CAS-level health check without SQL execution (#70)
- **`errno`/`sqlstate` on `DatabaseError`** — all protocol errors now populate structured error metadata with standard SQLSTATE codes (#71)
- **JSON type decoding** — opt-in `json_deserializer` parameter on `connect()`, CAS protocol bumped to v8, `CUBRIDDataType.JSON = 34` (#72)
- **Collection type decoding** — opt-in `decode_collections` parameter on `connect()`, SET → frozenset, MULTISET → list, SEQUENCE → list (#73)
- **SQLSTATE mapping table** (`error_codes.CAS_ERROR_TO_SQLSTATE`) for 19 common CUBRID error codes
- **Async cursor parity** — sync and async cursors now share identical `_escape_string` and parameter binding logic (#76, #77)
- **Timezone datetime parsing** — `DATETIMETZ`/`TIMESTAMPTZ` wire format decoding with IANA timezone keys (#78)
- **`cursor.nextset()`** for PEP 249 completeness (#79)
- **Configurable `fetch_size`** — pass `fetch_size=N` to `connect()` instead of hardcoded 100 (#81)
- **Async `read_timeout`** — `asyncio.wait_for` wrapping in `_send_and_receive` (#82)
- **Async dual-stack address fallback** — `getaddrinfo` iteration for IPv4/IPv6 in `_create_socket_nonblocking` (#83)
- **`_format_parameter()` hardening** — reject `float('nan')`/`float('inf')` with `ProgrammingError`, `DATETIMETZ` literals for tz-aware datetime (IANA key preferred, UTC offset fallback), `bytearray` support alongside `bytes` (#74)

### Security
- **Hardened parameter binding** — escape backslashes, reject null bytes, escape control characters (\r, \n, \x1a) in client-side SQL interpolation (#74)

### Fixed
- **Cursor registration dedup** — cursors no longer self-register in `__init__`; only `Connection.cursor()` registers (#76)
- **`Cursor.close()` best-effort** — narrowed exception handling to `InterfaceError`/`OperationalError`/`OSError` only (#80)
- **Sync `read_timeout`** — uses `socket.create_connection` for proper timeout enforcement
- **Sync IPv6 dual-stack** — `create_connection` handles address fallback automatically
- **Unreachable return removed** — dead `DATETIMETZ` return path in `_format_parameter()` cleaned up
- **Test isolation** — `_CursorClass` global cache no longer leaks between unit/integration tests
- **Benchmark `demodb` default** — changed to `testdb` matching Docker fixture

### Changed
- CAS protocol version bumped from 7 to 8 (enables native JSON type recognition)
- **BREAKING**: `_bind_parameters()` now only accepts `Sequence` (tuple/list) — `Mapping` (dict) parameter style removed. Use positional `?` parameters only.

## [1.1.0] - 2026-04-18

### Added
- **Native asyncio support** via `pycubrid.aio` module
  - `pycubrid.aio.connect()` — async connection factory
  - `AsyncConnection` — async context manager, commit, rollback, cursor creation
  - `AsyncCursor` — async execute, fetch (one/many/all), iterate, executemany
  - Uses `loop.sock_*` non-blocking socket I/O — reuses existing protocol/packet layers
- 30 new async offline tests (`tests/test_async.py`)

## [1.0.0] - 2026-04-11

### Compatibility Policy

This release establishes the 1.x compatibility contract: the public API follows semantic versioning,
and breaking changes will only occur in major version bumps (2.0+).

### Supported Environments

- **Python**: 3.10, 3.11, 3.12, 3.13
- **CUBRID**: 11.2, 11.4
- **Protocol**: CAS wire protocol version 8 (since CUBRID 10.2+)

### Fixed
- Resolve all mypy errors: explicit `str` return types in `get_server_version`
  and `get_last_insert_id` (`connection.py`)
- Resolve all pyright errors: initialize `response_code` in `PrepareAndExecutePacket`
  and `PreparePacket.__init__` (`protocol.py`); guard `_CursorClass` optional call (`connection.py`)

### Changed
- Development Status classifier updated from "Beta" to "Production/Stable"
- Version bumped to 1.0.0

## [0.7.0] - 2026-04-04

### Added
- `docs/SUPPORT_MATRIX.md`: Comprehensive support matrix documenting Python versions,
  CUBRID versions, PEP 249 compliance, data type mappings, driver features, and known
  limitations — defines the 1.0 support boundary
- Connection pooling section in `docs/CONNECTION.md` clarifying that pycubrid has no
  built-in pool and recommending SQLAlchemy or external pooling

### Fixed
- README documentation table: Removed incorrect "connection pool" reference from
  Connection guide description — pycubrid has no driver-level connection pool

### Changed
- Version bumped to 0.7.0 (stabilization release on path to 1.0)

## [0.6.0] - 2026-03-28

### Added
- Transparent CAS reconnection when broker signals `CAS_INFO_STATUS=INACTIVE`,
  matching the official CUBRID JDBC driver's `UClientSideConnection.checkReconnect()` behaviour
- `_check_reconnect()` method inspects `CAS_INFO[0]` before every request and
  reconnects automatically when the CAS process has been released (`KEEP_CONNECTION=AUTO`)
- `_invalidate_query_handles()` clears stale cursor query handles after
  commit/rollback to prevent `CloseQueryPacket` on dead sockets
- `CAS_INFO` is now updated from every server response so the status byte is always current

### Changed
- `_send_and_receive()` now calls `_check_reconnect()` instead of `_ensure_connected()`
  for automatic reconnection support

### Performance
- Pre-compiled `struct` objects in `packet.py` — eliminates repeated `struct.Struct()`
  instantiation on every read/write call
- Dict-based type dispatch table `_TYPE_READERS` in `protocol.py` — replaces
  long if/elif chain in `_read_value()` for O(1) type dispatch
- Slice-based `fetchall()`/`fetchmany()` in `cursor.py` — replaces per-row
  `fetchone()` loop with direct list slicing
- `executemany()` DML batch path — pre-renders all parameter sets into SQL
  strings and sends a single `BatchExecutePacket` instead of N round-trips
- `recv_into()` in `_recv_exact()` — writes directly into a pre-allocated
  buffer via `memoryview`, avoiding temporary `bytes` allocations
- `TCP_NODELAY` and `SO_KEEPALIVE` socket options on connection creation
- Module-level `_CursorClass` cache — eliminates `importlib.import_module()`
  + `getattr()` on every `Connection.cursor()` call
- SELECT 10K rows fetch: 96ms → 78ms (−19%)
- Connection establishment: 2.24ms → 1.66ms (−26%)
- INSERT execute: 7.81ms → 7.10ms (−9%)

### Fixed
- DDL statements (CREATE TABLE, ALTER TABLE) followed by DML on the same
  connection no longer fail with "connection lost during receive" (closes #23)

## [0.5.0] - 2026-03-12

### Added
- SQLAlchemy integration via `sqlalchemy-cubrid` v2.1.0 (`cubrid+pycubrid://` URL scheme)
- Updated README with SQLAlchemy usage examples

### Changed
- Version bumped to 0.5.0

## [0.4.0] - 2026-03-12

### Added
- `Lob` class for BLOB/CLOB Large Object support (create, write, read)
- `Connection.create_lob()` helper for server-side LOB creation
- `Connection.get_schema_info()` for schema introspection via CAS protocol
- `Cursor.executemany_batch()` for batch execution of multiple SQL statements
- Exported `Lob` from package `__init__.py`

## [0.3.0] - 2026-03-12

### Added
- PEP 249 `Connection` class with full CAS handshake lifecycle
  (`ClientInfoExchange` → `OpenDatabase` → `CloseDatabase`)
- TCP socket management with partial-read handling
- `commit()`, `rollback()`, `close()`, `cursor()` methods
- `autocommit` property for transaction control
- `get_server_version()` and `get_last_insert_id()` helper methods
- Context manager protocol (`with conn:` auto-close)
- PEP 249 `Cursor` class with full query execution
  (`execute`, `executemany`, `fetchone`, `fetchmany`, `fetchall`)
- Client-side parameter binding (str, int, float, None, bool, bytes,
  date, time, datetime, Decimal)
- `description` and `rowcount` attributes per PEP 249 spec
- Iterator protocol and context manager for Cursor
- `callproc()`, `setinputsizes()`, `setoutputsize()` stubs

### Fixed
- Double-parse bug in `_send_and_receive()` — now correctly passes
  response body (without data_length prefix) to packet.parse()

## [0.2.0] - 2026-03-12

### Added
- Wire protocol `PacketWriter` and `PacketReader` for CAS binary frame
  serialization/deserialization (big-endian, length-prefixed fields)
- 18 CAS protocol packet classes (`ClientInfoExchangePacket`, `OpenDatabasePacket`,
  `PreparePacket`, `ExecutePacket`, `PrepareAndExecutePacket`, `FetchPacket`,
  `CloseQueryPacket`, `CommitPacket`, `RollbackPacket`, `CloseDatabasePacket`,
  `GetEngineVersionPacket`, `BatchExecutePacket`, `GetSchemaPacket`,
  `SetDbParameterPacket`, `GetDbParameterPacket`, `GetLastInsertIdPacket`,
  `LOBNewPacket`, `LOBWritePacket`, `LOBReadPacket`)
- Response parsing helpers: `_raise_error`, `_parse_column_metadata`,
  `_parse_result_infos`, `_parse_row_data`, `_read_value`
- `ColumnMetaData` and `ResultInfo` dataclasses for structured query metadata
- Full wire-level value deserialization for all 27+ CUBRID data types

## [0.1.0] - 2026-03-12

### Added
- Initial project scaffolding
- PEP 249 exception hierarchy (Warning, Error, InterfaceError, DatabaseError, DataError,
  OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError)
- PEP 249 type objects (STRING, BINARY, NUMBER, DATETIME, ROWID) and constructors (Date, Time,
  Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks, Binary)
- CAS protocol constants (41 function codes, 27+ data types, isolation levels)
