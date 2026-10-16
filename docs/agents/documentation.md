# Documentation guide

> Moved from the root [AGENTS.md](../../AGENTS.md) (#926). Owner: pycubrid maintainers.
> Every rule in the root file still applies. Scope: Documentation map, translation and generated-file rules. The Documentation definition of done in the root file still applies.

## Documentation

User and contributor documentation lives in `docs/` and is published with MkDocs
(`mkdocs.yml`). Every English page under `docs/` has a Korean counterpart under `docs/ko/`
that must change in the same PR (`scripts/check_docs_translation.py`). A `README.md`
change requires `docs/README.ko.md`; the other README translations (`docs/README.*.md`)
are advisory (`scripts/check_translation_sync.py`).
`scripts/generate_llms_full.py` builds `docs/llms-full.txt` by concatenating the pages
directly under `docs/` (except `PRD.md` and the README translations) and copies the canonical index
`docs/llms.txt` to the root `llms.txt`; run it after doc edits and commit both outputs.

| Document | Content |
|---|---|
| `docs/index.md` | Documentation site landing page |
| `docs/quickstart.md`, `docs/first-query.md`, `docs/save-a-row.md`, `docs/commit-and-rollback.md`, `docs/finish-safely.md`, `docs/EXAMPLES.md` | The five beginner lessons (preparation first) and practical usage examples (page roles and the lesson convention: `docs/DEVELOPMENT.md#documentation-structure`) |
| `docs/CONNECTION.md` | Connection strings, URL format, options, TLS, timeouts, reconnect |
| `docs/API_REFERENCE.md` | Complete sync, async and compatibility API reference |
| `docs/TYPES.md` | Type mapping and CUBRID-specific types |
| `docs/PARAMETER_BINDING.md` | Driver-side literal binding contract: per-type SQL mapping, escaping, non-guarantees |
| `docs/PROTOCOL.md` | CAS wire protocol reference |
| `docs/ARCHITECTURE.md` | Request flow, CAS reconnection, module boundaries, design decisions |
| `docs/SUPPORT_MATRIX.md` | Supported Python and CUBRID versions and features |
| `docs/CI_POLICY.md` | CI event tiers, lane selection and release evidence |
| `docs/DEVELOPMENT.md` | Dev setup, tests, Docker, coverage, CI/CD |
| `docs/PERFORMANCE.md` | Benchmark methodology, profiling, timing hooks |
| `docs/TROUBLESHOOTING.md`, `docs/faq.md` | Common errors and questions |
| `docs/UPSTREAM_COMPATIBILITY.md`, `docs/UPSTREAM_TEST_LEDGER.md`, `docs/PREPARED_BINDING_DESIGN.md` | Official-driver compatibility inventory, scenario ledger and typed binding design |
| `docs/PRD.md` | Product requirements |

Repository-root policy documents: [RELEASE_POLICY.md](../../RELEASE_POLICY.md) (public surface,
semantic versioning, behavior-change classifications), [RELEASING.md](../../RELEASING.md),
[CONTRIBUTING.md](../../CONTRIBUTING.md), [ROADMAP.md](../../ROADMAP.md) and [SECURITY.md](../../SECURITY.md).
