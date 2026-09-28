# Typed CAS binding design for the explicit compatibility cursor (#418)

Status: reviewed design candidate, **not an implemented API**. The first delivery
is the bounded scalar slice in [#439](https://github.com/cubrid-lab/pycubrid/issues/439).
Internal FC2/FC3 scalar packet groundwork is tracked separately by
[#475](https://github.com/cubrid-lab/pycubrid/issues/475); it does not by
itself make a prepared cursor usable.
Ordinary `pycubrid.Cursor.execute()` and `pycubrid.aio.AsyncCursor.execute()`
continue to render 1.x SQL literals through FC41. This design does not switch
their defaults, promise a measured speedup or plan-cache effect, establish a
current SQL-injection exploit, or change CUBRID `TIME` precision.

The [official public API inventory](UPSTREAM_COMPATIBILITY.md) and
[#438 additive contract](UPSTREAM_COMPATIBILITY.md#selected-additive-contract-438)
select the future sync-only `pycubrid.compat.native` prepared cursor. A public
async prepared cursor is **not** part of #439. Shared protocol invariants and
private async transport tests must still be designed before one is advertised.
Wrapper query arguments, collections, LOBs, cursor navigation and broad
differential proof remain separate #440–#446 work. No upstream source is
copied into pycubrid; see [provenance](UPSTREAM_COMPATIBILITY.md#verification-and-provenance).

The #439 sync entrypoint extends the existing compatibility native connection
with `cursor()`, `commit() -> None` and `rollback() -> None`. It does **not**
implement effective `set_autocommit()` or cached setting members; those belong
to #467. Public #439 connections retain the current compatibility default
`autocommit=True`. Manual-mode protocol tests use an explicit private fixture
to set the underlying driver mode and do not advertise a public manual-mode
setting before #467. The first cursor subset is `prepare(sql) -> None`,
`bind_param(index, value, bind_type=0, /) -> None`,
`execute(option=0, max_col_size=0, /) -> int`,
`fetch_row(how=0, /) -> tuple | None`, and `close() -> None`. Indexes are
one-based. Only `bind_type=0`, `option=0`, `max_col_size=0` and tuple fetch
`how=0` are accepted initially; nondefault modes fail explicitly before I/O.
Dict rows and converter callbacks are #466, positioning #444, and extended
metadata #445. This is a deliberately incomplete official-public subset,
not an unqualified native or DB-API parity claim.

## Source and live evidence boundary

Source pins: official Python driver
[`e75ec36`](https://github.com/CUBRID/cubrid-python/tree/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b),
its CCI gitlink
[`7d1eb8f`](https://github.com/CUBRID/cubrid-cci/tree/7d1eb8f40f04089b8218d08e36e2c24a2de11b24),
broker 10.2
[`d56a158`](https://github.com/CUBRID/cubrid/tree/d56a158c06ee6ef917c9db7c52b778c63871bdd7)
and 11.4
[`6b2bc75`](https://github.com/CUBRID/cubrid/tree/6b2bc75527c8bad94d9ad8aba961638efdfb3269).
The design probes ran 2026-09-28 with pycubrid main `0057f98b`, Python
3.10.12, CUBRID `10.2.18.9024` and `11.4.6.1963`. The pinned native
extension was built from the above source/CCI revisions, not installed as a
runtime dependency. Both live OPEN_DATABASE replies advertised
`statement_pooling=1`; pooling disabled was **not** measured. Native probes
used actual autocommit `True` and `False`, and an independent observer for DML.
The native CCI may transparently reprepare, so reuse of one native Python
cursor alone is not server-handle proof.

Independent direct CAS probes on both brokers sent one FC2 followed by
multiple FC3 packets with the **same numeric server handle and physical
session**, without another FC2. For signed INT values 11, 12, 13 after
explicit commit, and 14 after explicit rollback, each SELECT returned the
matching row in both autocommit modes. A manual-mode probe with CCI's
forward-only byte `0` returned the same values. Repeated typed-INT INSERTs
also succeeded on one FC2 handle in both modes; an independent observer saw
manual work only after commit and auto-mode work after each execute. FC2
prepare flags `NORMAL=0` and,
separately, `HOLDABLE=0x08` were accepted for the no-bind repeated-execution
case. Native active SELECT results remained fetchable after commit but not
after rollback; that result observation is separate from direct handle reuse.
A direct HOLDABLE=0x08 FC8 request at position 2 also succeeded after manual
commit and failed after rollback on both brokers, although its first FC8
response contained both rows and did not prove a partial multi-FETCH result.
Native cursor close did not commit manual DML. A direct FC6 observer probe
found handle-only and explicit false left a pending INSERT invisible, while
FC6 with true made it visible on both servers. The probe table was removed
and its absence checked. These results apply to the two stock pooling-on
lanes, not arbitrary broker settings, reconnects or all official capabilities.

The protocol shape is grounded in pinned CCI
[prepare](https://github.com/CUBRID/cubrid-cci/blob/7d1eb8f40f04089b8218d08e36e2c24a2de11b24/src/cci/cci_query_execute.c#L395),
[execute](https://github.com/CUBRID/cubrid-cci/blob/7d1eb8f40f04089b8218d08e36e2c24a2de11b24/src/cci/cci_query_execute.c#L558),
[value serialization](https://github.com/CUBRID/cubrid-cci/blob/7d1eb8f40f04089b8218d08e36e2c24a2de11b24/src/cci/cci_query_execute.c#L6645),
and [close](https://github.com/CUBRID/cubrid-cci/blob/7d1eb8f40f04089b8218d08e36e2c24a2de11b24/src/cci/cci_query_execute.c#L1390),
not by treating the incomplete current `ExecutePacket.write()` as finished
binding support. The [10.2](https://github.com/CUBRID/cubrid/blob/d56a158c06ee6ef917c9db7c52b778c63871bdd7/src/broker/cas_function.c#L447)
and [11.4](https://github.com/CUBRID/cubrid/blob/6b2bc75527c8bad94d9ad8aba961638efdfb3269/src/broker/cas_function.c#L487)
broker arity branches agree on the FC3 argument layout.

## Frozen first-slice wire contract

Every argument is prefixed by a signed big-endian int32 byte length. There
is no top-level argument-count field. FC2 and FC3 use the existing framed
DATA_LENGTH/CAS_INFO header; prepared handles are scoped to one physical CAS
session. The first public implementation must assert exact bytes, not just
query results.

| Request | Ordered arguments and initial choice | Response / parser obligation |
| --- | --- | --- |
| FC2 PREPARE | NUL-terminated UTF-8 SQL; one-byte `HOLDABLE=0x08` prepare flag on the measured 10.2/11.4 lane; one-byte effective autocommit. Do not append CCI deferred-close IDs in the first slice. | Server handle; cache lifetime; statement type; **authoritative bind count**; updatable flag; column count/metadata. A negative result uses the normal CAS error parser. |
| FC3 EXECUTE | Ten fixed arguments, in order: handle int32, `NORMAL=0` flag byte, max-column-size int32=0, max-rows int32=0, empty CALL-mode argument, SELECT fetch byte (1 for SELECT, otherwise 0), effective autocommit byte, forward-only byte **1 for auto mode / 0 for manual mode**, zero cache timestamp (8 bytes), timeout int32=0. Then **two** length-prefixed arguments per bind: type byte and typed value bytes. Thus argc=10+2N. | Count, cache-reusable byte, result count/records; for protocol >1, honor optional refreshed-column-info block **before** shard ID and inline FETCH. Do not merely skip its flag. Parse errors retire uncertain transport, not a partial success. |
| FC6 CLOSE_REQ_HANDLE | Handle int32 only (equivalent to false in the measured lanes); never append effective autocommit true merely because the connection is in auto mode. | Check broker result. Close only an owner handle from the current physical generation; never send a retired handle on a replacement session. |

`prepare(sql)` rejects an embedded NUL or invalid UTF-8 encoding **before
FC2**; a malformed SQL string is never truncated at its NUL terminator or
leaked into a broker error. FC2 `HOLDABLE=0x08` follows CCI's effective native default on these brokers,
while direct FC2 with flag 0 also reused handles in the measured pooling-on
lanes. The first compatibility slice **must** preserve active SELECT fetch
after a successful explicit commit and close it after rollback, matching the
measured native behavior. Ordinary pycubrid keeps its separate 1.x result
invalidation contract. The larger partially buffered multi-FETCH case is a
mandatory live #439 gate; if it cannot be proven, stop/split #439 rather than
advertise the prepared cursor with silently truncated results.
Pooling-off and unknown broker-info values are not permission to reuse a
handle; the first slice must reject them before prepared I/O with a clear
unsupported-capability error until a separate measured policy exists.

### Scalar binding and pre-I/O decisions

The selected default string type is CHAR=1 (matching the pinned native
serializer), although direct probes also accepted STRING=2 on these brokers.
The first slice does not expose that alternate mapping. `bind_type=0` means
infer one of the rows below; nonzero bind types are rejected before wire I/O
until their contracts are designed. Unbound and explicitly bound SQL NULL are
different local states.

| Python input | FC3 type argument | FC3 value argument | Boundary |
| --- | --- | --- | --- |
| `int` but not `bool`, range `[-2^31, 2^31-1]` | length 1, INT=8 | length 4, signed big-endian int32 | Out-of-range raises `DataError` before I/O; no truncation. |
| `str` without embedded NUL | length 1, CHAR=1 | UTF-8 bytes plus one NUL; empty string is one NUL byte | Encoding failure or embedded NUL is rejected before I/O. Quotes/backslashes stay **value bytes**, never rendered into SQL. |
| `None` | length 1, NULL=0 | length 0, no payload | A complete, explicitly bound SQL NULL, not an omitted slot. |

`bool`, floats, date/time, bytes, objects, collections and LOB handles are
unsupported in #439 and rejected before I/O; their appropriate slices or
explicit gap records remain open. Native direct `bind_param(None)` currently
raises a `SystemError`; that upstream bug is not a parity target. Unsupported
execute flags, nonzero `max_col_size`, incomplete/extra parameters and invalid
one-based indexes likewise fail before FC3. A validation failure must leave
the previous usable result and handle intact; no FC41 fallback is allowed.
The initial error contract is `ProgrammingError` for bad index/arity, unbound
slot, embedded NUL in SQL or a value, unsupported input type or option,
`DataError` for INT overflow and SQL/value UTF-8 encoding failure,
`InterfaceError` for an unprepared/closed/stale owner,
`NotSupportedError` for pooling-off/unknown broker capability, and
`OperationalError` for uncertain transport. Exact message text is not stable.

## Owner and lifecycle state machine

One explicit compatibility cursor owns four independent axes. The compatibility
connection keeps a **separate prepared-owner registry/transaction hook**, not
ordinary `_DriverConnection._cursors`: the ordinary driver's commit/rollback
invalidation nulls ordinary cursor query handles and must not erase FC2 owners.
Public compatibility `commit()`/`rollback()` delegate to the underlying
driver, then notify prepared owners **only after a successful boundary**.
Compatibility `close()` closes current-generation prepared owners with
FC6(false) before closing the transport; failed/retired sessions send no FC6.
The future #467 effective autocommit setter must notify the same owner hook.

The four axes are:

1. **Physical session:** connection identity and monotonically increasing
   generation (as in the current connection recovery fence).
2. **Prepared statement:** server handle, its owning generation, bind count,
   statement/column metadata, and a local handle epoch.
3. **Result:** none, active, exhausted, or invalid. Result invalidation does
   not itself prove statement-handle invalidation.
4. **Transaction:** current epoch and boundary type (implicit autocommit,
   explicit commit, rollback). Bound slots and per-execution snapshots are
   separate from these axes.

| Event | Handle policy | Result/bind policy | Network policy |
| --- | --- | --- | --- |
| `prepare(sql)` succeeds | Replace previous owned handle only after safe close; capture FC2 count/columns, generation and epoch. | Clear bound slots/result. | One FC2; close only the old current-generation handle. |
| `bind_param(i, value, 0)` | Unchanged. | Validate one-based `1..N`, supported type/range and encode now; set slot even for `None`. | **Zero I/O**. |
| `execute(0, 0)` | Reuse current-generation handle in negotiated pooling-on lane; no implicit FC2. | Require every slot bound, snapshot typed pairs; replace prior result only after validation. | One FC3 with exactly N pairs. No FC41 or silent reprepare. |
| Successful explicit commit | Keep statement handle in the measured pooling-on lane; advance transaction epoch through the compatibility hook. | Preserve a live HOLDABLE SELECT result for further fetch; preserve bound slots for deliberate re-execution with a fresh snapshot. | No FC6 merely for the boundary. Multi-FETCH continuation is mandatory live proof. |
| Successful explicit rollback | Keep statement handle in the measured pooling-on lane; advance transaction epoch through the compatibility hook. | Invalidate the prior active result; preserve bound slots for deliberate re-execution with a fresh snapshot. | No FC6 merely for the boundary. |
| FC3 implicit auto boundary | Keep the same-generation statement handle in the pooling-on lane. | Keep the just-produced SELECT result fetchable until consumption **only if** live P05 proves the selected HOLDABLE/FETCH policy; otherwise stop or split #439, not silently return partial rows. | Autocommit is carried by FC3; do not append FC6(true). |
| Physical reconnect, failed restore, uncertain request/response, or cancellation after possible write | Retire handle and owner generation; never match on numeric handle alone. | Invalidate result and unsent snapshot. | No FC3/FC6 from the old owner on the new session; no automatic SQL/transaction replay. |
| Explicit cursor or connection close | Retire owner locally; if same generation and transport is trustworthy, send FC6 handle-only/false. | Invalidate result/slots. | Connection close drains its registry before transport close; no FC6 after physical retirement, and preserve the primary error if best-effort cleanup also fails. |

This chooses measured **R1** same-generation retention with pooling on and
explicit **P1** re-prepare after physical invalidation. Automatic P2
re-prepare is deferred: a hidden FC2 can change SQL/session semantics and
must never occur after uncertain FC3 delivery. A per-execution FC2+FC3
implementation does not satisfy repeated-handle reuse. For pooling off or
unknown, the first slice is gated before FC2; its different retention policy
requires a separate measured design and cannot silently fall back to FC41.

The generation check must be inside the same request/lifecycle critical
section as final serialization/write. Current send paths can preflight a
reconnect, so an outer-only check is insufficient. Under that section:
preflight transport; apply pending boundary/cleanup transitions; compare
connection identity, generation and handle epoch; only then serialize and
write. A definitely zero-byte validation or cancel-before-write error may
leave the session usable. After a failed write attempt, partial send,
uncertain read or cancellation after possible write, retire the session,
handles and results; do not
send FC6 to the replacement or retry SQL automatically. A completed
server-reported SQL error follows the normal DB-API error mapping without
inventing a transport failure. Public async preparation remains out of scope,
but private async request cancellation must obey the same no-replay rule.

## #439 acceptance tests specified before implementation

These IDs define expected assertions, not tests already passing. Each test
records the request count/function codes, server version, broker-info pooling
bit, physical generation, result/error, cleanup and skip reason. The two
measured **design** lanes are Python 3.10/CUBRID 10.2 and Python 3.14/CUBRID
11.4 with pooling on. Before #439 advertises the public prepared feature on
all supported servers, its live implementation gate must also pass on 11.0
and 11.2 (or explicitly reject those unverified lanes before prepared I/O and
keep the support gap open). Other supported Python versions are covered
offline; the full compatibility matrix and native differential certification
remain #446/#396 work. A
zero-case or unexplained skip is failure. Native comparison additionally
records the pinned e75/7d source/build identities; missing native dependencies
are not parity evidence.

| ID | Scenario / expected observable result | Wire and cleanup assertion |
| --- | --- | --- |
| P01 | FC2 SQL/flag/autocommit and bind count 0/1/N; server metadata retained. | Exact FC2 frame; negative response releases no invented handle. |
| P02 | INT min/0/max, CHAR ASCII/Korean/quote/backslash/empty, explicit NULL each round-trip with value and Python type. | Exact FC3 `10+2N` argument lengths; no FC41 or SQL literal substitution. |
| P03 | Index 0/N+1, unbound slot, extra/short arity, int overflow, `bool`, embedded NUL, unsupported type/option reject with the named exception classes above. | Zero FC3/FC6/write; previous usable result survives validation failure. |
| P04 | `prepare; bind; execute; bind; execute` returns distinct SELECT rows and inserts two distinct DML values in both modes. | One FC2, two FC3, same current-generation handle; observer sees manual DML only after commit, auto DML after each FC3. |
| P05 | Public `commit()` preserves active HOLDABLE SELECT fetch across multiple FC8 pages; public `rollback()` invalidates active result, but both retain the same-generation prepared owner. Use private manual-mode test setup to prove a real transaction boundary; public default auto-mode tests remain separate. | Native 10.2/11.4 comparison, explicit result/fetch assertions, one FC2 across re-execution, no silent partial row success. |
| P06 | FC3 refreshed-column-info absent and present, then shard and inline FETCH. | Full frame consumed; updated metadata parsed before row decode; malformed block fails closed. |
| P07 | Close after manual pending DML does not commit; execute in auto mode commits by FC3, not FC6. | Current-generation FC6 handle-only/false; no FC6(true); independent observer. |
| P08 | Reconnect between outer check and write rejects stale owner before FC3/FC6. | Zero stale-handle bytes on replacement, even when numeric IDs coincide. |
| P09 | Pre-byte validation/cancel versus partial send/read timeout/malformed response and async cancellation. | Correct primary error; retire only uncertain transport; no retry, no new-session FC6. |
| P10 | Ordinary sync/async FC41 and SQLAlchemy smoke retain their current signatures/results. | No prepared path activated by ordinary execute or connection creation. |
| P11 | Native e75 scalar/NULL comparison on both live lanes; known native None crash classified as deviation. | Exact build/source identities, executed cases and unsupported differences; no unavailable-oracle success. |
| P12 | Pooling-off/unknown broker info. | First slice rejects before FC2; no implied handle-reuse guarantee or FC41 fallback. |
| P13 | Property/security generation of valid and invalid SQL/value strings: arbitrary quotes, backslashes, empty, embedded NUL and surrogate/UTF-8 failures. | Successful cases keep SQL template and typed value bytes separate; invalid SQL writes zero FC2, and invalid bound values/options write zero FC3 after an earlier valid FC2. Existing owned-handle cleanup remains governed by P03/P07. No FC41 fallback or SQL/DSN/parameter content in error/log output. |

Success of #418 is a design decision and evidence boundary only. #439 must
implement and run these cases before its public surface can be advertised;
#439 should be delivered as at least two reviewable PRs if its provisional M
scope grows: first internal FC2/FC3 parser/serializer tests with **no public
API or parity claim**, then the compatibility owner/fetch/transaction slice
with the live/fault gates. Neither internal packet groundwork nor a partially
passing public PR closes #439. Keep separate acceptance evidence for each PR.
#440/#441 add whole/element NULL, empty collection, order/duplicates and
session-bound LOB handle/short-transfer cases separately. #446 remains the
blocking broader differential gate, and #396 cannot be closed from this RFC.
