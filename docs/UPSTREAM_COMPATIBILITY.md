# Official driver API inventory and compatibility design

The [machine-readable catalog](https://github.com/cubrid-lab/pycubrid/blob/main/tests/fixtures/official_api_inventory.json)
accounts for driver-declared public operations in the official
[CUBRID/cubrid-python snapshot](https://github.com/CUBRID/cubrid-python/tree/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b).
It records source references, signatures, defaults, return/error observations,
pycubrid counterparts at pinned baseline 7e0aad8, and explicit gaps. It is the inventory deliverable
for [#436](https://github.com/cubrid-lab/pycubrid/issues/436), within
[#396](https://github.com/cubrid-lab/pycubrid/issues/396).

This is source accounting. It does not certify functional parity, successful
official-driver execution, or a complete API superset. Similar names and existing
packet classes are insufficient evidence. The selected additive design for
[#438](https://github.com/cubrid-lab/pycubrid/issues/438) is recorded below;
its namespaces now provide construction and close only (#465). The catalog does
not certify future execution APIs or native parity; ordinary 1.x defaults stay
unchanged.
The catalog's target mappings remain a historical baseline; later #465
construction is described here without rewriting that source snapshot.

## Reading the catalog

Each operation has a stable `id`. `CUBRIDdb` identifies wrapper operations;
`documented_cubrid` identifies the native `_cubrid` module's documented/exported
surface. These namespaces are distinct: the wrapper's `execute(query, args)`
is not the native prepared cursor's `execute(option, max_col_size)`.

`sources` refer to paths and lines at the fixed upstream revision. Every row has
a signature/default map, an observable return/error description, and a current
pycubrid target or explicit absence. A target is a source-level counterpart,
**not** a passing comparison. `internal_analogue` specifically means a matching
protocol enum exists internally, without a compatible public export.
`tracking` references existing work; compatibility decisions do not themselves
implement the capabilities they discuss.
The native error profile separately records code/facility conversion and the
two-element exception argument tuple; matching exception names do not certify
equal error codes, arguments, or Python argument-validation behavior.

The wrapper imports native public names. Native rows' `aliases` and the top-level
reexport rule account for this without counting aliases as new capabilities.
The wrapper replaces the native module's `connection` name with its factory;
native connection operations are reached through the wrapper's `connection`
attribute, not through nonexistent `CUBRIDdb.connection.<method>` aliases.
Shared wrapper cursor methods are recorded on `cursors.BaseCursor`; both
`cursors.Cursor` and `cursors.DictCursor` inherit them. Standard library classes
reexported by the wrapper retain their Python constructor behavior; the catalog
does not enumerate every inherited built-in method as a new driver operation.

## Differences requiring decisions or implementation

| Surface | Existing pycubrid path or outstanding work |
| --- | --- |
| Constructors, autocommit, threading | Native initialization enables autocommit; ordinary pycubrid defaults to manual commit and declares `threadsafety=1`. Explicit CUBRID/UTF-8 construction and aliases are available in #465; sharing, settings and CCI URL/HA options remain separate work. |
| Charset, dict cursors, converters | The official wrapper exposes these options. pycubrid has no equivalent configurable surface. Charset proposal #86 tracks the missing encoding option; documentation of UTF-8-only behavior is not implementation evidence. UTF-8 defaults remain unchanged. |
| Prepare/typed binding | Public native prepare/bind/execute capability is absent even though packet classes exist: #418/#439; typed collection handles #440; LOB handles #441. |
| LOB cursor/file behavior | pycubrid has explicit-offset bytes read/write, not the official mutable-position/implicit-create interface. Seek and read/write contracts are #442; file import/export is #443. |
| Result navigation and metadata | Absolute/relative seek and position are #444; 15-field result metadata is #445 with #398. Native next_result exists; the wrapper nextset stub and pycubrid's unsupported nextset do not supply that capability. |
| Schema rows | #412 retains schema result-consumption/handle-cleanup work. A returned protocol packet is not the native schema-row return contract. |
| Batch facade and option flags | `Cursor.executemany_batch(sql_list, auto_commit=None)` already batches arbitrary SQL. A connection-level facade and native per-statement error records differ from its tuple results/first-error raising. Execute flags/query-plan options and connection member setters require focused follow-up under #438. |

## Selected additive contract (#438)

Maintainer-selected design, 2026-09-28: preserve ordinary pycubrid and add separate
`pycubrid.compat.cubriddb` (wrapper) and `pycubrid.compat.native` (native) namespaces.
Only construction and close are importable today (#465); the remaining rows below
are future targets, not delivered capabilities. This reversible additive design
does not authorize replacing 1.x defaults, adopting a
2.0 replacement, or publishing a release. A global switch,
shadowing `CUBRIDdb`/`_cubrid`, and overloading ordinary `Cursor.execute` are rejected:
the wrapper and native execution shapes cannot be unified without changing meaning.
Both surfaces must reuse the pure-Python transport, not introduce a second driver.
Later compatibility features must extend the same native connection owner; its
ordinary-driver handle is private and is not a new public transport API.

Ordinary connect/aio, user=`dba`, autocommit=False, threadsafety=1, execute-return-self,
cached string/None identity, None size fields, Boolean null_ok, normalized collection
codes 16/17/18, raw/opt-in typed values and explicit-offset bytes LOB APIs stay
unchanged. Ordinary SQLAlchemy imports and its current `pycubrid>=1.3.2,<2.0`
requirement stay unchanged. No new dependency or import-time native driver is needed.

The following are selected **target contracts**; `/` marks positional-only arguments,
and an omitted optional argument is not interchangeable with explicit None.

| Surface | Selected contract / delivery boundary |
| --- | --- |
| Factories (#465) | Wrapper `Connect/connect/connection(*args, **kwargs)` delegate to `Connection(dsn='', user='public', password='', charset='utf8')`; up to three positional values override dsn/user/password keywords. Native `connect(url, user='public', passwd='')` and lower-case connection construction start with autocommit=True. Wrapper `.connection` is the exact compatibility native object, not the ordinary object. Construction/close are delivered; excess positional/unsupported keyword or DSN options are rejected. Selectable charset/HA and cursor execution are not delivered. |
| Sharing / globals (future) | Wrapper apilevel='2.0', paramstyle='qmark', threadsafety=2 require real cursor support and explicit-object per-connection request/lifecycle serialization with two-thread tests first. The construction-only modules export none of these globals. Ordinary unlocked objects/global threadsafety=1 remain unchanged. |
| Settings | Native autocommit/isolation_level/lock_timeout/max_string_len assignments change cached snapshots only. `set_autocommit(mode)` / `set_isolation_level(level)` perform server operations and update caches; max_string_len retains the source's read-failure fallback 0. Wrapper `.autocommit` is server-backed. Do not invent effective setters for snapshot members. |
| Wrapper cursor | `cursor(dictCursor=None)`, `execute(query, args=None, set_type=None) -> int`, `executemany(query, args_list) -> None`; tuple/dict fetch and connection fetch-converter callback. Contradictory mapping-binding/default_cursor docstrings are not working capability promises. |
| Native prepared cursor | `prepare(sql) -> None`; `bind_param(index, value, bind_type=0, /) -> None`, index one-based; `execute(option=0, max_col_size=0, /) -> int`; `fetch_row(how=0, /)` returns tuple/dict or None. Parsed option 0, not docstring QUERY_ALL; #418/#439 implement the core. |
| Description | `(name, native_type, 0, 0, precision, scale, null_ok)`, with integer 0/1 null_ok, query-specific precision and native flagged types. Preserve value AND Python type; no unconditional collection 16→32 conversion. |
| Extended metadata | `result_info(n=0, /)` returns tuple-of-15-tuples (one outer entry for n>=1), or None with no columns. Actual order: type, not_null, scale, precision, name, attribute, class, default, auto_increment, unique, primary, foreign, reverse_index, reverse_unique, shared. Preserve empty versus absent metadata; #445 must not fabricate unavailable fields. |
| Collections | Stored SET targets mutable set, MULTISET/SEQUENCE list; validated type-aware textual non-NULL elements, preserving duplicates/order/empty values. Whole SQL NULL and NULL elements remain None by the safety deviation below. A brace literal is not evidence for stored SET; typed import/bind is #440. |
| Identity / schema | Native `insert_id() -> int \| None` queries current broker identity, not a cast of the ordinary cached INSERT snapshot. `schema_info(schema_type, class_name, attr_name omitted, /)` accepts no keywords/flags/explicit None, returns the first row as list or None; infer CLASS/VCLASS flag 1, ATTRIBUTE/CLASS_ATTRIBUTE flag 2, otherwise 0. Reuse #456 eager consumption/cleanup when available; ordinary consumption still returns all rows. |
| Native LOB | Separate mutable byte-position object, initially unpopulated. `write(string, type omitted, /) -> None` accepts str/bytes (UTF-8 for str), creates BLOB by default or B/C when requested. `read(len=0, /) -> str` reads remaining bytes for omitted/0 and decodes strict UTF-8. `seek(offset, whence=SEEK_CUR, /) -> int`; SEEK_END is size-offset. #442/#443 own lifecycle/short transfer/file behavior; ordinary bytes methods are not replaced. |
| Exceptions | Namespace-specific PEP 249 adapters retain `(numeric_code, formatted_message)` args and code/errno/SQLSTATE evidence without changing ordinary exception identities/args. Exact unstable messages and native argument-parser crashes are not targets. |

### Evidence and intentional safety deviations

Source contracts follow the pinned wrapper/native implementations, not conflicting
docstrings: [wrapper construction](https://github.com/CUBRID/cubrid-python/blob/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b/CUBRIDdb/connections.py#L15),
[cursor adaptation](https://github.com/CUBRID/cubrid-python/blob/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b/CUBRIDdb/cursors.py#L234),
and [native implementation](https://github.com/CUBRID/cubrid-python/blob/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b/cubrid_ext/python_cubrid.c#L1323),
with CCI gitlink `7d1eb8f40f04089b8218d08e36e2c24a2de11b24`.

Maintainer-local evidence `native-e75-oracle.y5UnxU` contains README,
`observations.jsonl` and `verify_observations.py`: exact e75+7d build, Python 3.10.12,
33 typed records from static SELECTs on CUBRID 10.2.18.9024/11.4.6.1963. It measured
scalar tuple rows, integer null_ok, metadata containers and initial autocommit=True.
Collection literals returned lists of strings and codes 104/96 or 72/64, not stored
SET proof or query-independent precision. Stored columns, LOBs, schema_info,
charset/HA and fault behavior remain unverified by that oracle; #446 tracks wider
portable differential evidence. This document does not rerun or certify native parity.

- Native direct `bind_param(None)` raised SystemError; later execution of an
  unbound NULL slot is not explicit NULL-binding success. Target core binds SQL NULL
  safely; precise pre-execution rejection while incomplete does not complete it.
- Native collection NULL became ''. Preserve None versus genuine empty text; do not
  reconstruct native text by `str()` of ordinary decoded values. Typed import needs
  explicit element type and None for SQL NULL, not a lossy string sentinel.
- Advance LOB position by actual transferred bytes; short writes raise. Do not copy
  requested-length advancement or unsafe short-read buffers; retain ordinary #394 safety.
- Reject excess factory arguments and unsupported mapping calls instead of silently
  discarding them or treating mapping keys as bound values.

These are explicit differential classifications, not blanket waivers or
"unsupported therefore complete" entries. Unaffected behavior needs exact comparisons;
NULL/data preservation needs tests. Missing stored-type evidence blocks that dimension's
certification, not independent prepared-core work.

### Migration targets and small delivery acceptance

For actual queries keep existing imports unchanged: the new modules only construct
and close connections. Once later capabilities exist, wrapper migration is
`import CUBRIDdb` → `from pycubrid.compat import cubriddb as CUBRIDdb`; native migration
is `import _cubrid` → `from pycubrid.compat import native as _cubrid`.
For manual transactions, wrapper callers use `conn.autocommit = False`; native
callers must use `conn.set_autocommit(False)`—native member assignment changes only
a snapshot. NULL consumers replace ''-as-NULL tests with `is None`, retaining actual
empty strings. Binary LOB users retain ordinary bytes APIs, not compatibility Unicode reads.

| Provisional leaf | Acceptance / dependency |
| --- | --- |
| M factories / M sharing | #465 delivers explicit construction/close, aliases, DSN/user/autocommit defaults and native-wrapper identity without changing ordinary behavior. Sharing/lifecycle serialization remains separate and gates threadsafety=2; no prepared engine or global switch is implied. |
| M prepared / typed binding | #439 after #418 and this contract: exact count/return/positional/NULL/error checks; #440 collections and #441 LOB binding follow core. |
| M conversion / charset / HA | Separate dictCursor/converter leaf; #86 real encoding; separate HA/URL-option leaf with actual failover evidence. Parsing options or upstream default_cursor stubs alone are incomplete. |
| S batch / errors / identity | Separate native batch records using existing arbitrary-SQL transport, namespace exception/export adapters, and broker-driven identity leaf with fresh/transaction/CALL/non-auto controls. Preserve ordinary first-error and cached-string behavior. |
| M settings / navigation | Effective server operations versus four cached members; #444 seek/position; separate next_result/execute-option/query-plan leaves. Stubs/flags do not complete capabilities. |
| S/M metadata / schema / LOB | #445 value/type/15-field metadata; #412/#455–457 owned schema reuse; #442 actual byte-position/short transfers; #443 files. Each is a focused slice, not a monolithic facade PR. |
| M differential evidence | #446/#351 compare each delivered slice at pinned native revisions, including stored collection NULL/empty/order/duplicates, LOB UTF-8 failures and ordinary SQLAlchemy smoke compatibility. Source accounting is not passing parity. |

The #465 foundation declares explicit submodule `__all__`, extends the existing
public-API checker's tracked modules/classes and RELEASE_POLICY §1, and regenerates
the baseline in the same PR. Future API slices must update that baseline again.
There are no new root aliases or async changes. New explicit APIs are MINOR
additions; ordinary promise corrections remain PATCH. #438 selected the design,
while #465 delivers construction only, not the remaining leaf capabilities.
#396 remains open until its scoped capabilities and verification are complete.

## Source discrepancies are not parity targets

The catalog retains discrepancies instead of silently normalizing them:

- Wrapper `callproc()` and `nextset()` are stubs despite descriptive docstrings.
- `result_info()` implements a different ordering of its first fields than its
  docstring; the recorded order follows tuple construction in the pinned source.
- Native execute parses option default `0`; its docstring describes QUERY_ALL.
  `bind_param` parses integer default `0`, not its documented `None` default.
- Native schema_info requires class_name; Set.imports requires the type argument,
  despite optional/default wording in their documentation.
- Schema-info constructs a row list despite tuple wording in its documentation;
  native insert_id returns an integer, unlike pycubrid's string convenience API.
- Package `__all__` advertises names without established package assignments;
  the catalog distinguishes advertised exports from functioning operations.
- Wrapper charset conversion, collection element inference, LOB short-transfer
  accounting/text decoding, and selected LOB-column handling need independent
  contract evidence. Source observation is not a new executed defect claim.

Genuinely internal details have explicit exclusion rationales. For example,
`_set_charset_name` says it is internal and must not be called by users; the
wrapper's public charset capability remains included as a gap. Native ABI layout,
CCI pointers, allocators/destructors and memory-address formatting are not copied.
An operation is never excluded simply because it is implemented in C.

## Verification and provenance

Run the offline consistency checks with:

```bash
python -m pytest tests/test_official_api_inventory.py -q
```

They validate accounting and currently named targets, without importing the
native driver, opening a database, or converting a mapping into parity proof.
Actual source-scenario accounting and official comparison evidence remain
separate deliverables (#437/#446).

We acknowledge the official driver's maintainers and contributors. Descriptions
here are independently paraphrased from the pinned source; no upstream source
or docstrings are copied wholesale. Existing [NOTICE](https://github.com/cubrid-lab/pycubrid/blob/main/NOTICE) and
[third-party reference notes](https://github.com/cubrid-lab/pycubrid/blob/main/THIRD_PARTY_LICENSES.md#reference-test-suite)
remain authoritative for provenance and licensing limits. This catalog adds no
license inference, runtime dependency, or permission to reuse upstream source.
