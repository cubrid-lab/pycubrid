# Wire protocol guide

> Moved from the root [AGENTS.md](../../AGENTS.md) (#926). Owner: pycubrid maintainers.
> Every rule in the root file still applies. Scope: CAS packet layouts, reply parsing bounds, bind encodings, LOB and handle lifetimes, CAS_INFO and reconnect rules, handshake and key constants. [docs/PROTOCOL.md](../PROTOCOL.md) is the user-facing protocol reference; supported versions are in the [support matrix](../SUPPORT_MATRIX.md).

## Wire Protocol Summary

### Packet Format

```
[0:4]  DATA_LENGTH  (4 bytes, big-endian int)
[4:8]  CAS_INFO     (4 bytes)
[8:]   PAYLOAD      (variable length)
```

Reply parsing never reads past the end of a reply (#383): a negative or
overrunning length, a row cell whose value does not use exactly its declared
size (#523), or collection elements that do not fill their size, raise
`ValueError`, which the connection reports as `OperationalError('malformed
response from broker')` and closes. That includes an FC20/FC21 row error
message length outside `0..remaining` and an FC40 type header longer than its
value (#897). Trailing bytes after the last declared
value are not checked; `DataError` is only for a complete reply (#492, #512),
including JSON text the built-in `json.loads` cannot decode (#543, #900):
undecodable column metadata text re-walks the remaining metadata by length
(#581), and FC41/refreshed FC3 defer that error until their declared tail and
inline rows have been validated without application hooks (#591). Later
structural errors close the session; complete replies retain the first
metadata DataError.

Known decoded collection elements validate their declared sizes and consumed
bytes even after a complete element raises DataError (#595). Later structural
damage wins; complete collections retain the first conversion error. Opaque
and unsupported nested member layouts keep their existing raw-byte contracts.

Scalar FC3 binds (`_encode_prepared_scalar`, public through `compat.native`
`bind_param()`; #439, widened by #840) send one type byte and its value bytes:
INT `8` (int32; `bool` as 1/0), BIGINT `21` (int64), NUMERIC `7` (ASCII
fixed-point text plus NUL, a lone leading `0` dropped because the CAS counts
every character but sign and point as precision; also ints past int64 up to
38 digits), DOUBLE `12` (finite only; the broker stores NaN), CHAR `1`
(charset bytes plus NUL), DATE `13`/TIME `14`/DATETIME `22` (always the
14-byte seven-int16 layout: the CAS reads TIME at offset 6 without a length
check), DATETIMETZ `31` (those 14 bytes plus the literal path's zone text, no
NUL: a NUL breaks region names, -1162), VARBIT `6` (non-empty: a zero-length
value is read as NULL) and NULL `0`. Dispatch on `type(value)` and read values
through base-class methods, as the literal path does (#518, #528).

Typed collection FC3 binds (#482; public only through `compat.native`
`set.imports()`/`bind_set()`, #440) send
the kind byte (SET `16`, MULTISET `17`, SEQUENCE `18`) as the type argument
and `[element type][int32 len, payload]*` as the value, with no element count.
INT elements are 4 bytes, STRING (`2`) elements are connection-charset bytes
plus NUL, and a NULL element has length 0. The broker silently keeps a partial
collection when an element length overruns the value, so validate every
element (flat tuple, no mixed/nested/bool/float/bytes) before building bytes.
Whole SQL NULL stays the scalar NULL pair. 10.2/11.4 brokers reject the
MULTISET kind with -454. The public #440 `imports()` matches the official
bytes: STRING elements whatever the requested type, default kind SET, and
`kind=MULTISET` sent as SEQUENCE.

EXECUTE_ARRAY (FC21, `ExecuteArrayPacket`; #839, public as
`executemany(..., prepared=True)` on both cursors) sends int handle, int timeout
(V4+), byte autocommit, then the FC3 (type, value) pair per marker per row in
row order, from `_encode_prepared_value` (scalars as above; `Set`/`Sequence`
and `Multiset`-as-SEQUENCE with STRING elements). The CAS ignores extra values,
so rows are checked against the PREPARE bind count first. The reply is per row
`count + 8-byte OID` or `DBMS_ERROR_INDICATOR (-2), code, len, message` (V3+),
then shard id (V5+); it stops early only after a reported `ER_INTERRUPTED` (-4)
row. Every row runs and only a
failing row is undone; autocommit commits per row, which deletes the session's
temp LOB files, so `Lob` values are refused with autocommit on. The cursor owns
the PREPARE handle at once so #488/#584 release paths free it; this FC2 (only
it, via `carries_deferred_closes`) carries queued deferred-close ids after the
autocommit byte, which CAS `fn_prepare_internal` frees as for FC41.

Typed LOB-handle FC3 binds (#441; public only through `compat.native`
`lob()`/`fetch_lob()`/`bind_lob()`) send BLOB `23`/CLOB `24` as the
type argument and the packed handle `[int32 db_type 33/34][int64 size][int32
locator length][locator NUL]` as the value: the official `bind_lob()` bytes
whenever the official lob type matches the column (official takes the type
byte from result column 1; the broker uses the handle's own `db_type`).
Fetched handles of committed rows may be bound on another connection (the
server copies them); a native lob records its origin (fetched/created) so
#442 can keep created temp handles on their own session.
The broker trusts the embedded size, so `Lob.write()` raises it to
`offset + bytes_written` after each write, as CCI does (and leaves it alone
when the reply claims more bytes than were sent). `_PreparedLob` records the
driver connection (by identity) and its physical generation; the native
cursor sends it only on that exact session, because generation numbers alone
repeat across connections. The server only appends (any other offset is
-1016). A fetched handle is copied into the row and is reusable; a LOB_NEW
temp handle is taken over by its first autocommit statement, even a failed
one.

An error reply's indicator (`-1` CAS, `-2` DBMS) becomes
`DatabaseError.error_source` (`"cas"`/`"server"`, else `None`); error numbers stay
as sent, with no renewed-code flag in the handshake (#903).

`CAS_INFO[0]` is transaction status: `0` is OUT_TRAN and `1` is IN_TRAN.
OUT_TRAN after END_TRAN is not a signal to reconnect; retain the physical
session. Because the CAS may still close the socket after an OUT_TRAN reply
(memory restart, broker reset, CHANGE CLIENT), probe with CHECK_CAS before the
next request (JDBC `checkReconnect` parity) and replace the session only when
that probe fails: once per request, before the request is first sent, restoring
driver-owned state (#485). Verification is explicit and per reply: every reply
is recorded unverified, and only OPEN_DATABASE, a successful CHECK_CAS or a healthy
ping marks it verified (#525); never key it on object identity, bytes or a
reconnect-only generation. Explicit `ping(reconnect=True)` also recovers a
confirmed CAS/transport failure; arbitrary SQL is never replayed automatically.
Commit/rollback CLOSE_REQ open cursor handles before END_TRAN.
With broker statement pooling on, autocommit releases (and cursors collected
without close) queue their handle ids for the next FC41's extra prepare
arguments (JDBC's wire mechanism, but result-set handles too, #488): at most 256
per statement, per physical generation, CLOSE_REQ'd at commit/rollback, dropped
on session retirement. With pooling off, known transaction-ending OUT_TRAN
replies retire existing handle ownership before parsing or identity lookup
(#584); FC41 success/DataError adoption cannot restore the already-freed ID.
Cached rows/counts remain usable, and only still-owned handles use immediate
CLOSE_REQ. Other OUT_TRAN echoes and schema/manual FETCH are not boundaries.
With `no_backslash_escapes` unset, probe each newly opened physical session
before binding against it; explicit `True`/`False` remains pinned. Healthy
same-session ping does not probe. A failed probe makes direct connect raise or
ping recovery return `False` after retiring the session. Async SQL already
bound under a prior session generation must be rejected before send, not
rebound or replayed. The CUBRID 10.2/11.4 manuals do not mark this setting
as dynamically changeable; do not claim a per-session `SET` toggle or verified
heterogeneous failover.

### Handshake Flow

1. **ClientInfoExchange**: Send 10 bytes (NO header) — magic `"CUBRS"` when `ssl` is requested (STARTTLS) or `"CUBRK"` plaintext, plus client type + version. Broker replies a 4-byte int32: `0`=ok, `<0`=fail-fast (`OperationalError`), `>0`=redirect port (reconnect on the new port WITHOUT repeating the handshake).
2. **TLS upgrade (optional)**: If `ssl` was truthy, upgrade the live transport via `loop.start_tls()` (async) or `ssl.SSLContext.wrap_socket()` (sync) before `OPEN_DATABASE`.
   The handshake uses `read_timeout` or a 10-second default; the default does not
   limit later requests.
3. **OpenDatabase**: Send db/user/password (628 bytes payload, no header — `PacketWriter(reserve_header=False)`)
4. **PrepareAndExecute / Prepare+Execute → Fetch → CloseQuery → EndTran → CloseDatabase**
   `executemany()` sends one EXECUTE_BATCH (FC20) of rendered literals by default;
   `executemany(..., prepared=True)` (#839) sends PREPARE (FC2) + EXECUTE_ARRAY (FC21)
   under one session-lock hold instead.

### Key Constants

- Magic string: `"CUBRK"` (plaintext) or `"CUBRS"` (STARTTLS-requested)
- Client type: `CAS_CLIENT_JDBC = 3`
- Protocol version: `8` (since CUBRID 10.2)
- Byte order: Big-endian throughout
- Text: no charset is negotiated and the broker converts nothing. The `charset`
  option (default UTF-8, #86) is stamped on every packet as `packet.encoding` and
  used for SQL, credentials, character values, metadata names, error text and
  LOB locators (lenient); fetched JSON, NUMERIC, TZ names and version strings
  stay UTF-8. Python euc_kr makeup sequences (Hangul outside KS X 1001) are
  rejected as unencodable.
- Column nullability is transmitted as `is_non_null`: zero permits NULL,
  nonzero means NOT NULL. Normalize it to `is_nullable` / DB-API `null_ok`.
- Column metadata keeps first-byte collection flags (`0x60`) distinct from the
  scalar/element type; `0x80` marks a full second type byte, not a scalar-only column.
- Cells of CALL/EVALUATE results and of NULL-typed columns carry their own type
  header, counted in the cell size, in the same layout: two bytes
  (`0x80 | collection bits | charset`, type) under protocol 7+, one byte from older
  brokers (#542). The row parser and the bounds re-walk before `DataError` read it
  with one helper; a header longer than its cell is malformed.
- FC9 schema metadata is condensed: type, scale, precision and name only, without
  SELECT constraint fields. Schema packets are owned by their original connection:
  consume with fetch_schema_info or abandon with close_schema_info, never replay
  their handles after transaction invalidation or physical-session replacement.
