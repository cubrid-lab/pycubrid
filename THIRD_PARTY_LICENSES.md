# Third-Party Software Licenses

This file lists the third-party open-source software involved in building and testing **pycubrid**.

**pycubrid has ZERO runtime dependencies** (`[project] dependencies = []`) — `pip install pycubrid` installs nothing else. The table below covers the development/testing toolchain only (`pip install -e ".[dev]"`), none of which ships in the wheel.
> **CUBRID server license, for the record.** The CUBRID server engine is
> distributed under Apache License 2.0 and the official APIs/connectors under
> BSD (upstream `COPYING`, http://www.cubrid.org/cubrid) — the frequently cited
> GPL v2+ no longer applies. This project is an independent wire-protocol client
> that neither includes nor links any CUBRID server code; the `cubrid/cubrid`
> Docker image is used for CI verification only.

All listed dependencies are distributed under permissive licenses (MIT, BSD-2/3-Clause, Apache-2.0, ISC, PSF, MPL-2.0). No dependency is copyleft/GPL, and none conflicts with this project's MIT license. MPL-2.0 packages appear in the development toolchain only and are not distributed with the package.

## Development / test-only dependencies (not distributed)

| Name                     | Version | License                              | URL                                                                   |
|--------------------------|---------|--------------------------------------|-----------------------------------------------------------------------|
| sortedcontainers         | 2.4.0   | Apache Software License              | http://www.grantjenks.com/docs/sortedcontainers/                      |
| bandit                   | 1.9.4   | Apache-2.0                           | https://bandit.readthedocs.io/                                        |
| coverage                 | 7.16.0  | Apache-2.0                           | https://github.com/coveragepy/coveragepy                              |
| pytest-asyncio           | 1.4.0   | Apache-2.0                           | https://github.com/pytest-dev/pytest-asyncio                          |
| stevedore                | 5.8.0   | Apache-2.0                           | https://docs.openstack.org/stevedore                                  |
| packaging                | 26.3    | Apache-2.0 OR BSD-2-Clause           | https://github.com/pypa/packaging                                     |
| colorama                 | 0.4.6   | BSD License                          | https://github.com/tartley/colorama                                   |
| nodeenv                  | 1.10.0  | BSD License                          | https://github.com/ekalinin/nodeenv                                   |
| Pygments                 | 2.21.0  | BSD-2-Clause                         | https://pygments.org                                                  |
| ast_serialize            | 0.11.0  | MIT                                  | https://github.com/mypyc/ast_serialize                                |
| cachetools               | 7.1.8   | MIT                                  | https://github.com/tkem/cachetools/                                   |
| cfgv                     | 3.5.0   | MIT                                  | https://github.com/asottile/cfgv                                      |
| filelock                 | 3.32.6  | MIT                                  | https://github.com/tox-dev/py-filelock                                |
| identify                 | 2.6.19  | MIT                                  | https://github.com/pre-commit/identify                                |
| iniconfig                | 2.3.0   | MIT                                  | https://github.com/pytest-dev/iniconfig                               |
| librt                    | 0.15.0  | MIT                                  | https://github.com/mypyc/librt                                        |
| mypy                     | 2.3.1   | MIT                                  | https://www.mypy-lang.org/                                            |
| mypy_extensions          | 1.1.0   | MIT                                  | https://github.com/python/mypy_extensions                             |
| platformdirs             | 4.11.8  | MIT                                  | https://github.com/tox-dev/platformdirs                               |
| pre_commit               | 4.6.2   | MIT                                  | https://github.com/pre-commit/pre-commit                              |
| pyproject-api            | 1.11.0  | MIT                                  | https://pyproject-api.readthedocs.io                                  |
| pytest                   | 9.1.1   | MIT                                  | https://docs.pytest.org/en/latest/                                    |
| pytest-cov               | 7.1.0   | MIT                                  | https://pytest-cov.readthedocs.io/en/latest/changelog.html            |
| ruff                     | 0.16.5  | MIT                                  | https://docs.astral.sh/ruff                                           |
| tox                      | 4.61.4  | MIT                                  | https://tox.wiki                                                      |
| virtualenv               | 21.7.9  | MIT                                  | https://github.com/pypa/virtualenv                                    |
| PyYAML                   | 6.0.3   | MIT License                          | https://pyyaml.org/                                                   |
| exceptiongroup           | 1.3.1   | MIT License                          | https://github.com/agronholm/exceptiongroup/blob/main/CHANGES.rst     |
| markdown-it-py           | 4.2.0   | MIT License                          | https://github.com/executablebooks/markdown-it-py                     |
| mdurl                    | 0.1.2   | MIT License                          | https://github.com/executablebooks/mdurl                              |
| pluggy                   | 1.6.0   | MIT License                          | UNKNOWN                                                               |
| python-discovery         | 1.6.0   | MIT License                          | https://github.com/tox-dev/python-discovery                           |
| rich                     | 15.0.0  | MIT License                          | https://github.com/Textualize/rich                                    |
| tomli_w                  | 1.2.0   | MIT License                          | https://github.com/hukkin/tomli-w                                     |
| hypothesis               | 6.168.0 | MPL-2.0                              | https://hypothesis.works                                              |
| pathspec                 | 1.1.1   | Mozilla Public License 2.0 (MPL 2.0) | https://python-path-specification.readthedocs.io/en/latest/index.html |
| typing_extensions        | 4.16.0  | PSF-2.0                              | https://github.com/python/typing_extensions                           |
| backports.asyncio.runner | 1.2.0   | Python Software Foundation License   | https://github.com/samypr100/backports.asyncio.runner                 |
| distlib                  | 0.4.3   | Python Software Foundation License   | https://github.com/pypa/distlib                                       |
