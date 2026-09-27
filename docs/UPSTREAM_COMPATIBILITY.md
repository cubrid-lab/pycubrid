# Official driver API inventory

The [machine-readable catalog](../tests/fixtures/official_api_inventory.json)
accounts for driver-declared public operations in the official
[CUBRID/cubrid-python snapshot](https://github.com/CUBRID/cubrid-python/tree/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b).
It records source references, signatures, defaults, return/error observations,
current pycubrid counterparts, and explicit gaps. It is the inventory deliverable
for [#436](https://github.com/cubrid-lab/pycubrid/issues/436), within
[#396](https://github.com/cubrid-lab/pycubrid/issues/396).

This is source accounting. It does not certify functional parity, successful
official-driver execution, or a complete API superset. Similar names and existing
packet classes are insufficient evidence. The compatibility/migration contract
is still tracked in [#438](https://github.com/cubrid-lab/pycubrid/issues/438);
this catalog changes no 1.x defaults and selects no new facade.

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
| Constructors, autocommit, threading | Native initialization enables autocommit; pycubrid defaults to manual commit and declares `threadsafety=1`. CCI URL/HA options, user defaults and wrapper constructor aliases require #438 decisions. |
| Charset, dict cursors, converters | The official wrapper exposes these options. pycubrid has no equivalent configurable surface. Charset proposal #86 tracks the missing encoding option; documentation of UTF-8-only behavior is not implementation evidence. UTF-8 defaults remain unchanged. |
| Prepare/typed binding | Public native prepare/bind/execute capability is absent even though packet classes exist: #418/#439; typed collection handles #440; LOB handles #441. |
| LOB cursor/file behavior | pycubrid has explicit-offset bytes read/write, not the official mutable-position/implicit-create interface. Seek and read/write contracts are #442; file import/export is #443. |
| Result navigation and metadata | Absolute/relative seek and position are #444; 15-field result metadata is #445 with #398. Native next_result exists; the wrapper nextset stub and pycubrid's unsupported nextset do not supply that capability. |
| Schema rows | #412 retains schema result-consumption/handle-cleanup work. A returned protocol packet is not the native schema-row return contract. |
| Batch facade and option flags | `Cursor.executemany_batch(sql_list, auto_commit=None)` already batches arbitrary SQL. A connection-level facade and native per-statement error records differ from its tuple results/first-error raising. Execute flags/query-plan options and connection member setters require focused follow-up under #438. |

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
or docstrings are copied wholesale. Existing [NOTICE](../NOTICE) and
[third-party reference notes](../THIRD_PARTY_LICENSES.md#reference-test-suite)
remain authoritative for provenance and licensing limits. This catalog adds no
license inference, runtime dependency, or permission to reuse upstream source.
