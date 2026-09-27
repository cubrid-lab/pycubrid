# Upstream scenario ledger

The [scenario ledger](https://github.com/cubrid-lab/pycubrid/blob/main/tests/fixtures/upstream_scenarios.csv) is an initial
inventory for [#437](https://github.com/cubrid-lab/pycubrid/issues/437), under the
[public-parity tracker](https://github.com/cubrid-lab/pycubrid/issues/396).
It is not a functional-parity certificate or a Python 2 compatibility promise.

## Accounting boundary

Source: [CUBRID/cubrid-python at e75ec36](https://github.com/CUBRID/cubrid-python/tree/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b).
The inventory retains all **450 raw test-prefixed function declarations**:

| Source folder | Raw declarations |
|---|---:|
| `tests/` | 68 |
| `tests2/` | 198 |
| `tests3/` | 184 |

This includes the legacy function named `test_`. Path, class, function and source
line identify each declaration; eight separately named assertion subcases bring
the initial ledger to 458 rows. These are accounting counts, not unique behavioral
scenarios. Subcase assessment remains incomplete, as do helper/setup and top-level
script assertions. Do not divide local parametrized tests by these declarations
to advertise a parity percentage.

Legacy declarations remain in the ledger. `duplicate_candidate_of` is a
**name-only review hint**, pointing to a provisional newer source declaration;
neither the hint nor its target establishes assertion equivalence or a canonical
unique-scenario count. No declaration is removed because it looks duplicated.

Identifiers and independently paraphrased expectations reference upstream; no
upstream test bodies are imported or copied. Keep the source acknowledgement and
[unresolved test-file licensing terms](https://github.com/cubrid-lab/pycubrid/blob/main/THIRD_PARTY_LICENSES.md#reference-test-suite).
Confirm applicable terms before any verbatim source reuse.

## Mapping is separate from execution

`classification` has these meanings:

| Value | Meaning |
|---|---|
| `unknown` | Assertions/subcases require assessment. |
| `duplicate_candidate` | A name matches a legacy declaration; behavior is still unassessed. |
| `related` | Local coverage exists, but matching assertions/API/setup have not been established. |
| `assertion_equivalent` | Only the named expectation matches a reviewed local assertion. |
| `unsupported` | A reviewed required capability is absent and remains functional backlog. |

`local_nodes` is reserved for explicit assertion-subcase rows with equivalent mappings; `related_nodes` is
not counted as equivalent. `local_revision` pins the inspected local source.
Every row has an owner/family, expected behavior or explicit unknown, and a gap
reason/issue. #437 delivers this seed inventory; closing it does not complete
unassessed declarations or candidate aliases. Ongoing family assessment remains
under the open parent #396 or the named capability follow-ups; #437 is retained
as delivery provenance. Unsupported scenarios are not exclusions from parity work.

The initial review references the 41 collected local SQL/type/data nodes already
merged at `7e0aad8fe83a37f324c1c9efea0ff68de15680f2`. Only eight exact assertion
subcases map to seven local nodes: selected IDs/counts, identical ENUM ordinal
pairs, and the two trigger result rows. Parent declarations remain related rather
than declaring their entire fixture/API behavior equivalent. In particular:

- INDEX joins/subselects assert 4 locally versus 5 upstream.
- PARTITION schemas/payloads and padded CHAR assertions differ.
- ENUM update datasets, affected counts and extra metadata checks differ.
- Collection literal/decode tests do not implement prepared collection binding
  (#440), and official string-valued containers differ (#438).
- LOB literal round-trips use different payloads and do not establish official
  handle bind/fetch (#441), seek (#442) or file import/export (#443) equivalence.

All initial `evidence_status` values are `not_run`: this change inspects assertions
and collects node IDs but does not execute either driver's live contracts.
Mapping, test collection and a skipped integration test are not passing evidence.

For a later execution observation, record `verification_commit`, server version,
Python version, mode, result and an immutable CI/JUnit or retained artifact
reference. Use `observed` for honest failed or explained-skipped results; retain
the gap and never relabel them as passes. Non-skipped results must leave `skip_reason`
empty. `verified_pass` requires an equivalent
assertion mapping, a passing result and complete identities. One observation
does not certify all servers/modes. Existing lane/JUnit mechanisms supply runtime
evidence; [#446](https://github.com/cubrid-lab/pycubrid/issues/446) tracks that work.

## Check and maintain

```bash
python -m pytest tests/test_upstream_scenario_ledger.py
```

The focused check uses the existing pytest collection mechanism without a server
to reject broken node IDs, duplicate source IDs, missing ownership/gap reasons,
unsupported/failed pass claims and unexplained skips. Synthetic validator inputs
test those rejection rules; they are not stored as driver execution evidence.

When assessing another declaration, preserve its source ID and candidate links,
add explicit assertion subcases where necessary, and compare fixtures, arguments,
types and exact expected values before supplying `local_nodes`. Keep execution
fields independent. This is test-accounting maintenance only: no driver behavior,
support policy, compatibility facade, CI scheduler or new dependency changes.
