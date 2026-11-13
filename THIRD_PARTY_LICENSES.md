# Third-Party Software Licenses

This file records the third-party open-source software that **pycubrid** uses at
runtime, in development and to build its documentation site, what pycubrid itself
distributes, and which third-party assets the published documentation site
contains or loads. It is an engineering inventory, not legal advice.

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
- **Source distribution**: the same package, metadata, `LICENSE` and `NOTICE`,
  project metadata files (`README.md`, `pyproject.toml`, `setup.cfg`), plus the
  `tests/test_*.py` modules. Test fixtures, `conftest.py` and test
  helpers are not included in the sdist.
- **Repository only (not in either distribution)**: test fixtures contain only
  material produced for this project: a ledger of upstream test identifiers
  (`tests/fixtures/upstream_scenarios.csv`), recorded upstream API names and
  observed behaviour (`official_api_inventory.json`,
  `official_differential_claims.json`, `docs-reason-events.json`), byte values
  observed by running CCI (`cci_collection_golden.json`) and self-generated TLS
  test certificates (`tests/fixtures/tls/generate.sh`). These fixtures contain
  no copied upstream source.
- **Acknowledgments**: `NOTICE` records the reference implementations consulted
  while writing pycubrid (node-cubrid, the cubrid-python test suite). pycubrid is
  an independent implementation and vendors none of their code.
- **Development and test dependencies** are installed by contributors from
  PyPI. The repository also carries a derived MIT-licensed PyYAML 6.0.3 pure
  Python wheel for CI on Python 3.15, with its upstream licence inside the wheel.
  Its input/output hashes and reproduction steps are recorded in
  [.github/ci-wheels/README.md](.github/ci-wheels/README.md). It is excluded from
  the published wheel and sdist.
- **Documentation site**: the MkDocs site published to GitHub Pages
  (https://cubrid-lab.github.io/pycubrid/) is a separate distribution. Neither
  the wheel nor the sdist contains it or its build tools. What the built site
  contains, and what it only links to, is recorded in the next section.

## Documentation site

The `docs` workflow (`.github/workflows/docs.yml`) installs the locked `docs`
dependency group, runs `mkdocs build --strict` and uploads the resulting `site/`
directory to GitHub Pages. The site therefore involves three different kinds of
third-party material. The observations below come from a local
`mkdocs build --strict` with the locked `docs` group (Material for MkDocs 9.8.0)
at commit `7b2d0cfb7ec64a0b3833ebc37dc4dc7738f6eacb`, with the licenses read
from the installed package's license files and from the notices in the built
files. A theme upgrade can change these files, so when a pull request (including
a Dependabot update) changes the `mkdocs-material` version, rebuild the site and
recheck the bundled third-party files and their notices below, then update the
recorded theme version. This is a manual review step; no test enforces it, so
routine dependency updates are not blocked.

### Build tools (not published)

The Python packages of the `docs` dependency group run on the build machine
only. They are listed in the `docs` dependency group table at the end of this
file. Apart from the theme assets described next, none of their code is copied
into `site/`.

### Third-party assets bundled into the built site (redistributed by GitHub Pages)

The Material for MkDocs theme copies these files into `site/`, so the published
site redistributes them unmodified:

| Asset in `site/` | Component | License | Evidence |
|---|---|---|---|
| `assets/stylesheets/main.*.min.css`, `assets/stylesheets/palette.*.min.css`, `assets/javascripts/bundle.*.min.js`, `assets/images/favicon.png` (copied but not referenced, because `mkdocs.yml` sets its own favicon), and the HTML layout of every page, rendered from the theme's templates | Material for MkDocs 9.8.0 (Martin Donath) | MIT | Package metadata `License-Expression: MIT` and the wheel's `LICENSE` file |
| Compiled into `assets/javascripts/bundle.*.min.js` | clipboard.js 2.0.11 | MIT | "Licensed MIT" header in the source embedded in the bundle's source map |
| Compiled into `assets/javascripts/bundle.*.min.js` | escape-html | MIT | "MIT Licensed" header in the embedded source |
| Compiled into `assets/javascripts/bundle.*.min.js` | tslib (Microsoft) | 0BSD | The 0BSD permission text in the embedded source header |
| Compiled into `assets/javascripts/bundle.*.min.js` | RxJS, focus-visible | Not verified | The source map lists them, but the embedded sources carry no license notice |
| `assets/javascripts/workers/search.*.min.js` (search worker) | lunr.js (Oliver Nightingale) | MIT | `@license MIT` header in the embedded source |
| `assets/javascripts/lunr/min/lunr.{da,de,du,es,fi,fr,hu,it,nl,no,pt,ro,ru,sv,tr}.min.js` | lunr-languages (Mihai Valentin), including the Snowball JavaScript library (Oleg Mazko) | MPL, version not stated | Each file's header links to `http://www.mozilla.org/MPL/` without a version number |
| The other 17 files in `assets/javascripts/lunr/min/` (such as `lunr.ko.min.js`, `lunr.ja.min.js`, `lunr.multi.min.js`, `lunr.stemmer.support.min.js`) | lunr-languages | Not verified | The files carry no license or copyright notice |
| `assets/javascripts/lunr/tinyseg.js` | TinySegmenter (Taku Kudo) | BSD ("new BSD licence") | File header |
| `assets/javascripts/lunr/wordcut.js` | wordcut, bundled with Node.js core modules, narwhal and Underscore | Not verified as a whole | The file contains MIT notices for the Node.js modules (Joyent), narwhal's `assert` and Underscore, but no notice for wordcut itself or its other bundled modules |
| Inline SVG icons in every page | Material Design Icons (`material/weather-night`, `material/weather-sunny` in `mkdocs.yml`) | Apache-2.0 | `material/templates/.icons/material/LICENSE` (Pictogrammers Free License: icons under Apache 2.0) |
| Inline SVG icon in every page footer | Font Awesome Free (`fontawesome/brands/github` in `mkdocs.yml`) | CC BY 4.0 (icons) | `material/templates/.icons/fontawesome/LICENSE.txt` and the attribution comment that the built pages keep inside the SVG |

The site's search index is built for English only (`lang: ['en']` in
`search/search_index.json`), but the theme copies all language files into
`site/` regardless. The MPL-licensed lunr-languages files keep their upstream
notices, which name the source repository
(https://github.com/MihaiValentin/lunr-languages). Whether publishing them in
minified form needs any further source-availability notice has not been
reviewed. The other files in `site/` are generated from this repository's
`docs/` directory and `mkdocs.yml`; the configured logo and favicon are not
bundled (see below).

### Assets loaded remotely by visitors' browsers (not redistributed)

These are not in `site/`. The built pages, or the theme's JavaScript, make the
visitor's browser fetch them from a third-party server, which therefore sees the
visitor's IP address:

| Asset | Loaded from | When | License |
|---|---|---|---|
| Roboto and Roboto Mono web fonts | `fonts.googleapis.com` stylesheet and `fonts.gstatic.com` font files | Every page: `mkdocs.yml` does not set `theme.font`, so the theme's default Google Fonts link is emitted; the built `site/` contains no font files | Not verified in this inventory; served by Google, not by this project |
| Mermaid (`mermaid@11`, exact version chosen by unpkg at view time) | `https://unpkg.com/mermaid@11/dist/mermaid.min.js` | Only on pages that contain a Mermaid diagram (26 built pages at the commit above) | Not verified in this inventory |
| resize-observer-polyfill (latest version chosen by unpkg) | `https://unpkg.com/resize-observer-polyfill` | Only in browsers without a native `ResizeObserver` | Not verified in this inventory |

The logo and favicon are loaded from `raw.githubusercontent.com` at a pinned
`cubrid-lab/brand` commit. They are CUBRID Lab material, not third-party
software, and are not assessed here.

## License categories

The inventories below contain three categories:

- **Permissive**: MIT, MIT-0, BSD-2-Clause, BSD-3-Clause, Apache-2.0 and the
  Python Software Foundation License, as declared by each package. The generator
  also accepts ISC, 0BSD, Unlicense, Public Domain and the generic "BSD License"
  classifier as permissive. Most packages fall here.
- **Weak (file-level) copyleft: MPL**: `certifi`, `hypothesis`, `pathspec`, all
  MPL-2.0. MPL-2.0 is not a permissive license. Its obligations attach to the
  MPL-covered files themselves: anyone distributing those files, modified or
  not, must make their source available under MPL-2.0 and keep their notices.
  Because MPL-2.0 is file-level, it never extends to pycubrid's own files (MPL
  §3.3, "Larger Work"). Separately, pycubrid does not distribute these
  packages, since they appear only in the development, test and
  documentation-build toolchains, so their distribution obligations do not
  arise for this project. The MPL-licensed lunr-languages files that the
  documentation site does redistribute are covered in
  [Documentation site](#documentation-site).
- **Needs review**: any package whose metadata mentions a GPL-family license, or
  a license the generator cannot classify. Multiple license classifiers do not say
  whether they combine as "or" or "and", so these are never treated as
  permissive automatically. Each one is resolved below from the package's own
  license files.

### Reviewed entries

- **docutils** (0.23, pulled in by `twine` through `readme_renderer`; development
  only). Its metadata carries Public Domain, BSD and GPL classifiers. Its
  `COPYING.rst` places most files in the public domain, with BSD-2-Clause
  exceptions including `docutils/utils/math/latex2mathml.py`,
  `docutils/__main__.py` and `docutils/utils/math/math2html.py` (relicensed from
  GPL-3.0+ to BSD-2-Clause for Docutils). `docutils/utils/smartquotes.py` also
  carries the original SmartyPants BSD-3-Clause notice, and
  `docutils/utils/_roman_numerals.py` is public domain or 0BSD (per its file
  header). The one GPL-3.0+ file it
  lists, `tools/editors/emacs/rst.el`, is not part of the installed package. As
  installed, docutils is therefore public domain plus permissive BSD terms
  (BSD-2-Clause, BSD-3-Clause and 0BSD).

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
- Commands (one fresh environment per dependency group; `--exclude pycubrid`
  drops the project itself). At the commit above these sets were the `.[dev]`
  and `.[mutation]` extras; #784 moved them to the `dev` and `mutation`
  PEP 735 dependency groups. A fresh install of each group after that change
  resolved the same 63 and 19 package names, so the tables were not regenerated.
  #971 added `jinja2` to the `test` group; a fresh `dev` install on 2026-10-16
  (CPython 3.12, uv) resolved 65 names, and only the two new rows, `Jinja2` and
  `MarkupSafe`, were copied from that run's generator output.
  On 15 October 2026, the locked build group added setuptools 84.0.0 and wheel
  0.48.0 to `dev`; their PyPI metadata declares MIT. Those two rows extend the
  existing snapshot; the remaining transitive rows have not been regenerated:

```bash
uv venv -p 3.12 /tmp/tpl-dev
uv pip install -p /tmp/tpl-dev/bin/python -e . --group dev
/tmp/tpl-dev/bin/python scripts/generate_third_party_licenses.py --exclude pycubrid

uv venv -p 3.12 /tmp/tpl-mutation
uv pip install -p /tmp/tpl-mutation/bin/python -e . --group mutation
/tmp/tpl-mutation/bin/python scripts/generate_third_party_licenses.py --exclude pycubrid
```

The `docs` dependency group table was generated separately, from the versions
locked in `uv.lock` rather than from a fresh resolution, because the `docs`
workflow installs exactly that locked set:

- Dependency declarations and lock: commit `7b2d0cfb7ec64a0b3833ebc37dc4dc7738f6eacb`
- Environment: CPython 3.12.13 on Linux x86_64 (glibc 2.35), uv 0.11.7,
  generated 2026-11-12
- Commands (the same export and install that `.github/workflows/docs.yml` runs;
  the export omits the project itself, so the 31 rows are the whole locked
  `docs` set):

```bash
uv export --locked --no-build --only-group docs --no-emit-project --format requirements-txt --output-file /tmp/tpl-docs-requirements.txt
uv venv -p 3.12 /tmp/tpl-docs
uv pip install -p /tmp/tpl-docs/bin/python --require-hashes --only-binary :all: -r /tmp/tpl-docs-requirements.txt
/tmp/tpl-docs/bin/python scripts/generate_third_party_licenses.py --exclude pycubrid
```

`scripts/generate_third_party_licenses.py` uses only the standard library. It
reads the PEP 639 `License-Expression` field, then `License ::` classifiers, then
a short `License` field, and never guesses a license. `tests/test_third_party_licenses.py`
fails when a dependency declared in `pyproject.toml` is missing from these tables,
when a recorded version falls outside a declared version range, when a row's
category disagrees with what the generator would assign to its license, or when
a reviewed entry loses its review. Exact `==` pins are checked for presence only:
they are authoritative in `pyproject.toml`, so a routine pin bump does not
require regenerating this snapshot. Transitive rows are kept accurate by
regenerating the tables, not by the test.

## Development / test dependencies: `dev` dependency group (62 packages, not distributed)

| Name | Version | License | Category | URL |
|---|---|---|---|---|
| ast_serialize | 0.12.1 | MIT | Permissive | https://github.com/mypyc/ast_serialize |
| bandit | 1.9.4 | Apache-2.0 | Permissive | https://github.com/PyCQA/bandit |
| build | 1.6.1 | MIT | Permissive | https://build.pypa.io |
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
| Jinja2 | 3.1.6 | BSD License | Permissive | https://github.com/pallets/jinja/ |
| jeepney | 0.9.0 | MIT | Permissive | https://gitlab.com/takluyver/jeepney |
| keyring | 25.7.0 | MIT | Permissive | https://github.com/jaraco/keyring |
| librt | 0.16.0 | MIT | Permissive | https://github.com/mypyc/librt |
| markdown-it-py | 4.2.0 | MIT License | Permissive | https://github.com/executablebooks/markdown-it-py |
| MarkupSafe | 3.0.4 | BSD-3-Clause | Permissive | https://github.com/pallets/markupsafe/ |
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
| setuptools | 84.0.0 | MIT | Permissive | https://setuptools.pypa.io/ |
| twine | 7.0.0 | Apache-2.0 | Permissive | https://twine.readthedocs.io/ |
| typing_extensions | 4.16.0 | PSF-2.0 | Permissive | https://github.com/python/typing_extensions |
| urllib3 | 2.8.0 | MIT | Permissive | - |
| virtualenv | 21.14.6 | MIT | Permissive | https://github.com/pypa/virtualenv |
| certifi | 2026.7.22 | Mozilla Public License 2.0 (MPL 2.0) | Weak copyleft (MPL) | https://github.com/certifi/python-certifi |
| hypothesis | 6.168.5 | MPL-2.0 | Weak copyleft (MPL) | https://hypothesis.works |
| pathspec | 1.1.1 | Mozilla Public License 2.0 (MPL 2.0) | Weak copyleft (MPL) | https://github.com/cpburnz/python-pathspec |
| docutils | 0.23 | BSD License / GNU General Public License (GPL) / Public Domain | Needs review | https://docutils.sourceforge.io |

## Mutation-testing dependencies: `mutation` dependency group (19 packages, not distributed)

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

## Documentation-site build dependencies: `docs` dependency group (31 packages, not distributed)

| Name | Version | License | Category | URL |
|---|---|---|---|---|
| babel | 2.18.0 | BSD License | Permissive | https://github.com/python-babel/babel |
| backrefs | 8.1 | MIT | Permissive | https://github.com/facelessuser/backrefs |
| charset-normalizer | 3.5.2 | MIT | Permissive | - |
| click | 8.5.0 | BSD-3-Clause | Permissive | https://github.com/pallets/click/ |
| colorama | 0.4.6 | BSD License | Permissive | https://github.com/tartley/colorama |
| ghp-import | 2.1.0 | Apache Software License | Permissive | https://github.com/c-w/ghp-import |
| idna | 3.20 | BSD-3-Clause | Permissive | https://github.com/kjd/idna |
| Jinja2 | 3.1.6 | BSD License | Permissive | https://github.com/pallets/jinja/ |
| Markdown | 3.11.1 | BSD-3-Clause | Permissive | https://Python-Markdown.github.io/ |
| markdown-it-py | 4.2.0 | MIT License | Permissive | https://github.com/executablebooks/markdown-it-py |
| MarkupSafe | 3.0.4 | BSD-3-Clause | Permissive | https://github.com/pallets/markupsafe/ |
| mdurl | 0.1.2 | MIT License | Permissive | https://github.com/executablebooks/mdurl |
| mergedeep | 1.3.4 | MIT License | Permissive | https://github.com/clarketm/mergedeep |
| mkdocs | 1.6.1 | BSD-2-Clause | Permissive | https://github.com/mkdocs/mkdocs |
| mkdocs-get-deps | 0.2.2 | MIT | Permissive | https://github.com/mkdocs/get-deps |
| mkdocs-material | 9.8.0 | MIT | Permissive | https://github.com/squidfunk/mkdocs-material |
| mkdocs-material-extensions | 1.3.1 | MIT | Permissive | https://github.com/facelessuser/mkdocs-material-extensions |
| packaging | 26.3 | Apache-2.0 OR BSD-2-Clause | Permissive | https://github.com/pypa/packaging |
| paginate | 0.5.7 | MIT License | Permissive | https://github.com/Signum/paginate |
| platformdirs | 4.13.0 | MIT | Permissive | https://github.com/tox-dev/platformdirs |
| Pygments | 2.21.0 | BSD-2-Clause | Permissive | https://pygments.org |
| pymdown-extensions | 12.3 | MIT | Permissive | https://github.com/facelessuser/pymdown-extensions |
| python-dateutil | 2.9.0.post0 | Apache Software License / BSD License | Permissive | https://github.com/dateutil/dateutil |
| PyYAML | 6.0.3 | MIT License | Permissive | https://github.com/yaml/pyyaml |
| pyyaml_env_tag | 1.1 | MIT | Permissive | https://github.com/waylan/pyyaml-env-tag |
| requests | 2.34.2 | Apache Software License | Permissive | https://github.com/psf/requests |
| six | 1.17.0 | MIT License | Permissive | https://github.com/benjaminp/six |
| urllib3 | 2.8.0 | MIT | Permissive | - |
| watchdog | 6.0.0 | Apache Software License | Permissive | https://github.com/gorakhargosh/watchdog/ |
| certifi | 2026.10.23 | Mozilla Public License 2.0 (MPL 2.0) | Weak copyleft (MPL) | https://github.com/certifi/python-certifi |
| pathspec | 1.1.1 | Mozilla Public License 2.0 (MPL 2.0) | Weak copyleft (MPL) | https://github.com/cpburnz/python-pathspec |
