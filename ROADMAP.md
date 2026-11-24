# Roadmap

pycubrid plans its work in the GitHub issue tracker rather than in a document
that can fall out of date. This page explains where to look.

## Planned work

- [Open issues](https://github.com/cubrid-lab/pycubrid/issues?q=is%3Aissue+is%3Aopen)
  are the current backlog. Each carries a type label, one `priority:` label and
  one `size:` label.
- Sort by priority:
  [critical](https://github.com/cubrid-lab/pycubrid/issues?q=is%3Aissue+is%3Aopen+label%3A%22priority%3A+critical%22),
  [high](https://github.com/cubrid-lab/pycubrid/issues?q=is%3Aissue+is%3Aopen+label%3A%22priority%3A+high%22),
  [medium](https://github.com/cubrid-lab/pycubrid/issues?q=is%3Aissue+is%3Aopen+label%3A%22priority%3A+medium%22),
  [low](https://github.com/cubrid-lab/pycubrid/issues?q=is%3Aissue+is%3Aopen+label%3A%22priority%3A+low%22).
- [Milestones](https://github.com/cubrid-lab/pycubrid/milestones) group issues
  that belong to a themed or versioned goal.
- Issues labeled `status: needs triage` have not yet been reviewed, so their
  priority and size are not final.

## What the labels mean

- `priority:` records urgency and impact (`critical`, `high`, `medium`, `low`).
- `size:` records estimated effort (`XS` to `XL`), not importance.

Priority is not merge readiness: the next PR to land also depends on
dependencies, risk and validation. See [CONTRIBUTING.md](CONTRIBUTING.md) for how
issues are triaged and labeled.

## Contributing

- [`good first issue`](https://github.com/cubrid-lab/pycubrid/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
  marks small, well-scoped tasks. Check [CONTRIBUTING.md](CONTRIBUTING.md) before
  starting one.
- [`help wanted`](https://github.com/cubrid-lab/pycubrid/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22)
  marks work where outside help is especially welcome.

## Releases and supported versions

- [CHANGELOG.md](CHANGELOG.md) lists what each release changed and what is
  unreleased.
- [RELEASE_POLICY.md](RELEASE_POLICY.md) defines versioning, deprecation and
  compatibility rules; [RELEASING.md](RELEASING.md) describes how releases are
  published.
- [docs/SUPPORT_MATRIX.md](docs/SUPPORT_MATRIX.md) lists the supported Python,
  CUBRID and driver versions, including any announced end of support.
- The current source version is [`pycubrid.__version__`](pycubrid/__init__.py).
