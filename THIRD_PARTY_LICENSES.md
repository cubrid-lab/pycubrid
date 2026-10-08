# Third-Party Software Licenses

This file records the third-party open-source software that **pycubrid** uses at
runtime and in development, and what pycubrid itself distributes. It is an
engineering inventory, not legal advice.

## Runtime dependencies

**pycubrid has no runtime dependencies except on Windows**, where it depends on
[`tzdata`](https://pypi.org/project/tzdata/) (Apache-2.0;
`tzdata; sys_platform == 'win32'`) because Windows ships no IANA time zone
database for `zoneinfo` (#413). On Linux and macOS `pip install pycubrid`
installs nothing else. This entry is stated explicitly because the inventory
below is generated on Linux, where the conditional dependency is not installed.

> **CUBRID server license, for the record.** The CUBRID server engine is
> distributed under Apache License 2.0 and the official APIs/connectors under
> BSD (upstream `COPYING`, http://www.cubrid.org/cubrid) — the frequently cited
> GPL v2+ no longer applies. This project is an independent wire-protocol client
> that neither includes nor links any CUBRID server code; the `cubrid/cubrid`
> Docker image is used for CI verification only.

## What pycubrid distributes

- **Wheel**: only the `pycubrid` package and its metadata, plus `LICENSE` and
  `NOTICE`. No third-party source code is vendored.
- **Source distribution**: the same package plus the test suite. Test fixtures
  contain only material produced for this project: a ledger of upstream test
  identifiers (`tests/fixtures/upstream_scenarios.csv`), recorded upstream API
  names and observed behaviour (`official_api_inventory.json`,
  `official_differential_claims.json`), byte values observed by running CCI
  (`cci_collection_golden.json`) and self-generated TLS test certificates
  (`tests/fixtures/tls/generate.sh`). No upstream source file is copied.
- **Development and test dependencies** are installed by contributors from
  PyPI. pycubrid uses them as tools; it does not bundle or redistribute them.

## License categories

The inventories below contain three categories:

- **Permissive**: MIT, MIT-0, BSD-2-Clause, BSD-3-Clause, Apache-2.0 and the
  Python Software Foundation License, as declared by each package. Most
  packages fall here.
- **Weak (file-level) copyleft: MPL-2.0**: `certifi`, `hypothesis`, `pathspec`.
  MPL-2.0 is not a permissive license. Its obligations apply to the MPL-covered
  files themselves: distributing those files, modified or not, requires making
  their source available under MPL-2.0. These packages appear only in the
  development and test toolchain, and pycubrid does not distribute them, so using
  them imposes nothing on pycubrid's MIT-licensed code.
- **Needs review**: any package whose metadata mentions a GPL-family license, or
  a license the generator cannot classify. Multiple license classifiers do not say
  whether they combine as "or" or "and", so these are never treated as
  permissive automatically. Each one is resolved below from the package's own
  license files.

### Reviewed entries

- **docutils** (0.23, pulled in by `twine` through `readme_renderer`; development
  only). Its metadata carries Public Domain, BSD and GPL classifiers. Its
  `COPYING.rst` places most files in the public domain, with BSD-2-Clause
  exceptions (`docutils/utils/smartquotes.py`, `docutils/utils/math/latex2mathml.py`,
  and `docutils/utils/math/math2html.py`, which was relicensed from GPL-3.0+ to
  BSD-2-Clause for Docutils). The GPL-3.0+ file it lists,
  `tools/editors/emacs/rst.el`, is not part of the installed package. As
  installed, docutils is therefore public domain plus BSD-2-Clause.

## Reference test suite

We acknowledge the maintainers and contributors of [CUBRID/cubrid-python](https://github.com/CUBRID/cubrid-python). Its test scenarios informed pycubrid's SQL-feature and type/data regressions (#409, #410 and #433), adapted to pycubrid's APIs and documented differences. This is a reference relationship, not a runtime dependency or a claim of complete CUBRIDdb API parity.

Reviewed upstream source snapshot: `e75ec36b2a92b8829a49a967a29a1fbb9d7c322b`:

- [tests3/test_execute.py](https://github.com/CUBRID/cubrid-python/blob/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b/tests3/test_execute.py) — INDEX, PARTITION, VIEW and TRIGGER scenarios.
- [tests3/test_enum.py](https://github.com/CUBRID/cubrid-python/blob/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b/tests3/test_enum.py) — ENUM insert, cast and update scenarios.
- [tests3/test_set.py](https://github.com/CUBRID/cubrid-python/blob/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b/tests3/test_set.py) — collection type/value scenarios.
- [tests3/test_cubrid.py](https://github.com/CUBRID/cubrid-python/blob/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b/tests3/test_cubrid.py) — collection and BLOB/CLOB value scenarios.

At this snapshot, no top-level `LICENSE`/`COPYING` file or per-file license notice was found in these four tests. We do not infer a license for them from other CUBRID components. This acknowledgment records provenance; it does not establish permission to copy upstream source or relicense it. Confirm the applicable terms with upstream before any verbatim source reuse.

## How the inventories were generated

The tables are a snapshot of concrete versions observed in fresh environments.
The allowed version ranges are the ones declared in `pyproject.toml`, which
remains authoritative.

- Dependency declarations: commit `a1a5fb0ee77c67f887a5f8bac219c4fb933e501c`
- Environment: CPython 3.12.13 on Linux x86_64 (glibc 2.35), uv 0.11.7,
  generated 2026-10-09
- Commands (one fresh environment per extra; `--exclude pycubrid` drops the
  project itself):

```bash
uv venv -p 3.12 /tmp/tpl-dev
uv pip install -p /tmp/tpl-dev/bin/python -e ".[dev]"
/tmp/tpl-dev/bin/python scripts/generate_third_party_licenses.py --exclude pycubrid

uv venv -p 3.12 /tmp/tpl-mutation
uv pip install -p /tmp/tpl-mutation/bin/python -e ".[mutation]"
/tmp/tpl-mutation/bin/python scripts/generate_third_party_licenses.py --exclude pycubrid
```

`scripts/generate_third_party_licenses.py` uses only the standard library. It
reads the PEP 639 `License-Expression` field, then `License ::` classifiers, then
a short `License` field, and never guesses a license. `tests/test_third_party_licenses.py`
fails when a dependency declared in `pyproject.toml`, or an exact version pin, is
missing from or disagrees with these tables.

## Development / test dependencies: `.[dev]` (63 packages, not distributed)

| Name | Version | License | Category | URL |
|---|---|---|---|---|
| ast_serialize | 0.12.1 | MIT | Permissive | https://github.com/mypyc/ast_serialize |
| bandit | 1.9.4 | Apache-2.0 | Permissive | https://github.com/PyCQA/bandit |
| build | 1.6.1 | MIT | Permissive | https://build.pypa.io |
| cachetools | 7.2.1 | MIT | Permissive | https://github.com/tkem/cachetools/ |
| cffi | 2.1.1 | MIT-0 | Permissive | https://github.com/python-cffi/cffi |
| cfgv | 3.5.0 | MIT | Permissive | https://github.com/asottile/cfgv |
| charset-normalizer | 3.5.2 | MIT | Permissive | - |
| colorama | 0.4.6 | BSD License | Permissive | https://github.com/tartley/colorama |
| coverage | 7.16.2 | Apache-2.0 | Permissive | https://github.com/coveragepy/coveragepy |
| cryptography | 50.0.2 | Apache-2.0 OR BSD-3-Clause | Permissive | https://github.com/pyca/cryptography |
| distlib | 0.4.3 | Python Software Foundation License | Permissive | https://github.com/pypa/distlib |
| filelock | 4.0.12 | MIT | Permissive | https://github.com/tox-dev/py-filelock |
| id | 1.6.1 | Apache Software License | Permissive | https://pypi.org/project/id/ |
| identify | 2.6.20 | MIT | Permissive | https://github.com/pre-commit/identify |
| idna | 3.20 | BSD-3-Clause | Permissive | https://github.com/kjd/idna |
| iniconfig | 2.3.1 | MIT | Permissive | https://github.com/pytest-dev/iniconfig |
| jaraco.classes | 3.4.0 | MIT License | Permissive | https://github.com/jaraco/jaraco.classes |
| jaraco.context | 6.1.2 | MIT | Permissive | https://github.com/jaraco/jaraco.context |
| jaraco.functools | 4.6.0 | MIT | Permissive | https://github.com/jaraco/jaraco.functools |
| jeepney | 0.9.0 | MIT | Permissive | https://gitlab.com/takluyver/jeepney |
| keyring | 25.7.0 | MIT | Permissive | https://github.com/jaraco/keyring |
| librt | 0.16.0 | MIT | Permissive | https://github.com/mypyc/librt |
| markdown-it-py | 4.2.0 | MIT License | Permissive | https://github.com/executablebooks/markdown-it-py |
| mdurl | 0.1.2 | MIT License | Permissive | https://github.com/executablebooks/mdurl |
| more-itertools | 11.1.0 | MIT | Permissive | https://github.com/more-itertools/more-itertools |
| mypy | 2.4.0 | MIT | Permissive | https://www.mypy-lang.org/ |
| mypy_extensions | 1.1.0 | MIT | Permissive | https://github.com/python/mypy_extensions |
| nh3 | 0.3.7 | MIT | Permissive | https://github.com/messense/nh3 |
| nodeenv | 1.11.0 | BSD License | Permissive | https://github.com/ekalinin/nodeenv |
| packaging | 26.3 | Apache-2.0 OR BSD-2-Clause | Permissive | https://github.com/pypa/packaging |
| platformdirs | 4.12.4 | MIT | Permissive | https://github.com/tox-dev/platformdirs |
| pluggy | 1.6.0 | MIT License | Permissive | - |
| pre_commit | 4.6.2 | MIT | Permissive | https://github.com/pre-commit/pre-commit |
| py-cpuinfo2 | 10.1.1 | MIT | Permissive | https://github.com/akx/py-cpuinfo2 |
| pycparser | 3.1 | BSD-3-Clause | Permissive | https://github.com/eliben/pycparser |
| Pygments | 2.21.0 | BSD-2-Clause | Permissive | https://pygments.org |
| pyproject-api | 1.11.4 | MIT | Permissive | https://pyproject-api.readthedocs.io |
| pyproject_hooks | 1.3.3 | MIT | Permissive | https://github.com/pypa/pyproject-hooks |
| pytest | 9.1.1 | MIT | Permissive | https://docs.pytest.org/en/latest/ |
| pytest-asyncio | 1.4.0 | Apache-2.0 | Permissive | https://github.com/pytest-dev/pytest-asyncio |
| pytest-benchmark | 5.3.0 | BSD-2-Clause | Permissive | - |
| pytest-cov | 7.1.0 | MIT | Permissive | - |
| python-discovery | 1.6.1 | MIT License | Permissive | https://github.com/tox-dev/python-discovery |
| PyYAML | 6.0.3 | MIT License | Permissive | https://github.com/yaml/pyyaml |
| readme_renderer | 46.0 | Apache-2.0 | Permissive | https://github.com/pypa/readme_renderer |
| requests | 2.34.2 | Apache Software License | Permissive | https://github.com/psf/requests |
| requests-toolbelt | 1.0.0 | Apache Software License | Permissive | https://github.com/requests/toolbelt |
| rfc3986 | 2.0.0 | Apache Software License | Permissive | http://rfc3986.readthedocs.io |
| rich | 15.0.0 | MIT License | Permissive | https://github.com/Textualize/rich |
| ruff | 0.16.10 | MIT | Permissive | https://github.com/astral-sh/ruff |
| SecretStorage | 3.5.0 | BSD-3-Clause | Permissive | https://github.com/mitya57/secretstorage |
| sortedcontainers | 2.4.0 | Apache Software License | Permissive | http://www.grantjenks.com/docs/sortedcontainers/ |
| stevedore | 5.9.1 | Apache-2.0 | Permissive | https://docs.openstack.org/stevedore |
| tomli_w | 1.2.0 | MIT License | Permissive | https://github.com/hukkin/tomli-w |
| tox | 4.64.10 | MIT | Permissive | https://tox.wiki |
| twine | 7.0.0 | Apache-2.0 | Permissive | https://twine.readthedocs.io/ |
| typing_extensions | 4.16.0 | PSF-2.0 | Permissive | https://github.com/python/typing_extensions |
| urllib3 | 2.8.0 | MIT | Permissive | - |
| virtualenv | 21.14.6 | MIT | Permissive | https://github.com/pypa/virtualenv |
| certifi | 2026.7.22 | Mozilla Public License 2.0 (MPL 2.0) | Weak copyleft (MPL-2.0) | https://github.com/certifi/python-certifi |
| hypothesis | 6.168.5 | MPL-2.0 | Weak copyleft (MPL-2.0) | https://hypothesis.works |
| pathspec | 1.1.1 | Mozilla Public License 2.0 (MPL 2.0) | Weak copyleft (MPL-2.0) | https://github.com/cpburnz/python-pathspec |
| docutils | 0.23 | BSD License / GNU General Public License (GPL) / Public Domain | Needs review | https://docutils.sourceforge.io |

## Mutation-testing dependencies: `.[mutation]` (19 packages, not distributed)

| Name | Version | License | Category | URL |
|---|---|---|---|---|
| click | 8.5.0 | BSD-3-Clause | Permissive | https://github.com/pallets/click/ |
| coverage | 7.16.2 | Apache-2.0 | Permissive | https://github.com/coveragepy/coveragepy |
| iniconfig | 2.3.1 | MIT | Permissive | https://github.com/pytest-dev/iniconfig |
| libcst | 1.9.0 | MIT License | Permissive | - |
| linkify-it-py | 2.2.0 | MIT License | Permissive | https://github.com/tsutsu3/linkify-it-py |
| markdown-it-py | 4.2.0 | MIT License | Permissive | https://github.com/executablebooks/markdown-it-py |
| mdit-py-plugins | 0.6.1 | MIT License | Permissive | https://github.com/executablebooks/mdit-py-plugins |
| mdurl | 0.1.2 | MIT License | Permissive | https://github.com/executablebooks/mdurl |
| mutmut | 3.8.0 | BSD-3-Clause | Permissive | https://github.com/boxed/mutmut |
| packaging | 26.3 | Apache-2.0 OR BSD-2-Clause | Permissive | https://github.com/pypa/packaging |
| platformdirs | 4.12.4 | MIT | Permissive | https://github.com/tox-dev/platformdirs |
| pluggy | 1.6.0 | MIT License | Permissive | - |
| Pygments | 2.21.0 | BSD-2-Clause | Permissive | https://pygments.org |
| pytest | 9.1.1 | MIT | Permissive | https://docs.pytest.org/en/latest/ |
| PyYAML | 6.0.3 | MIT License | Permissive | https://github.com/yaml/pyyaml |
| rich | 15.0.0 | MIT License | Permissive | https://github.com/Textualize/rich |
| setproctitle | 1.3.8 | BSD-3-Clause | Permissive | https://github.com/dvarrazzo/py-setproctitle |
| textual | 8.2.8 | MIT License | Permissive | https://github.com/Textualize/textual |
| typing_extensions | 4.16.0 | PSF-2.0 | Permissive | https://github.com/python/typing_extensions |
