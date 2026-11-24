# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Changed

- **Unused protocol internals removed** — Dead writer helpers, header functions and constants in `pycubrid.packet` and `pycubrid.constants`, plus unread connection state, are gone; these are internal, non-public names and runtime behavior is unchanged ([#1326](https://github.com/cubrid-lab/pycubrid/issues/1326), [PR #1334](https://github.com/cubrid-lab/pycubrid/pull/1334)) by @yeongseon.

### Fixed

- **`TimingStats` uses singular wording for one call** — `repr(TimingStats())` now prints `1 call` instead of `1 calls`; zero and multi-call output is unchanged ([#1292](https://github.com/cubrid-lab/pycubrid/issues/1292), [PR #1293](https://github.com/cubrid-lab/pycubrid/pull/1293)) by @RiccardoBifulco (first contribution 🎉).
- **Demo recorder rejects a non-numeric `DEMO_PORT` clearly** — `demos/record_demo.py` now reports a missing, non-numeric, blank or out-of-range `DEMO_PORT` with its own `DEMO_HOST and a valid DEMO_PORT are required` message instead of a raw `int()` error, before any connection is opened; maintainer demo tooling only, the driver is unchanged ([#1311](https://github.com/cubrid-lab/pycubrid/issues/1311), [PR #1317](https://github.com/cubrid-lab/pycubrid/pull/1317)) by @yeongseon.
- **Select-only execute profile reads seeded rows** — `scripts/profile_execute.py --operation select` now inserts 20 rows into its scratch table before the profiled SELECT instead of measuring an empty result; `--operation all` is unchanged. Contributor profiling tool only, the driver is unchanged ([#1310](https://github.com/cubrid-lab/pycubrid/issues/1310), [PR #1318](https://github.com/cubrid-lab/pycubrid/pull/1318)) by @yeongseon.

### Documentation

- **Roadmap points at the issue tracker** — `ROADMAP.md` is now a short page that links to the open issues, the `priority:` and `size:` labels, milestones, `good first issue` and `help wanted` lists, so it no longer lists dated plans that can go stale ([#1330](https://github.com/cubrid-lab/pycubrid/issues/1330), [PR #1335](https://github.com/cubrid-lab/pycubrid/pull/1335)) by @yeongseon.

- **Lesson screenshots** — the first-query and commit-and-rollback lessons each show a terminal screenshot of the real program output, in English and Korean; the text outputs are unchanged ([#1307](https://github.com/cubrid-lab/pycubrid/issues/1307), [PR #1319](https://github.com/cubrid-lab/pycubrid/pull/1319)) by @yeongseon.

- **Commit type and release version** — `CONTRIBUTING.md` and its Korean page explain which commit types request a release, and that changes confined to tooling outside the published wheel use `test`, `ci`, `chore` or `docs` ([PR #1322](https://github.com/cubrid-lab/pycubrid/pull/1322)) by @yeongseon.

### CI

- **Manual Python 3.15 packaging check removed** — The `python-canary.yml` workflow is gone because Python 3.15 is supported and `integration-full.yml` already tests it; the related policy docs and workflow-pin test are updated ([#1331](https://github.com/cubrid-lab/pycubrid/issues/1331), [PR #1336](https://github.com/cubrid-lab/pycubrid/pull/1336)) by @yeongseon.

- Standardize the package publisher as `publish.yml` with job `publish-production` and environment `production`, including release tooling and recovery documentation ([#1313](https://github.com/cubrid-lab/pycubrid/issues/1313)).

- **Official-driver oracle builds with GCC 14 and later** — The differential's CUBRIDdb oracle build now passes two flags that keep GCC 14+ from rejecting the unchanged upstream C source ([#1302](https://github.com/cubrid-lab/pycubrid/issues/1302)) by @yeongseon.

- **Example-only pull requests run the live integration job** — A pull request that changes only `examples/**` now selects the live CUBRID lane, so the example smoke test runs against a real server before merge ([PR #1321](https://github.com/cubrid-lab/pycubrid/pull/1321)) by @yeongseon.

- **Unused release and maintenance tooling removed** — The release-prep script that Release Please replaced, the weekly `maintenance.yml` workflow that other CI already covers and the unreachable agent-review bootstrap path are gone, and CI path filters now list only existing files ([#1328](https://github.com/cubrid-lab/pycubrid/issues/1328), [PR #1333](https://github.com/cubrid-lab/pycubrid/pull/1333)) by @yeongseon.

### Tests

- **Live smoke test for the example scripts** — A new integration test runs `examples/01_connect.py`, `02_crud.py`, `04_lob.py` and `05_batch.py` against a real CUBRID and checks their results through a separate connection; it refuses to touch pre-existing `example_*` tables ([#1312](https://github.com/cubrid-lab/pycubrid/issues/1312), [PR #1320](https://github.com/cubrid-lab/pycubrid/pull/1320)) by @yeongseon.

- **Timing-disabled tests now assert something** — The cursor execute and fetch tests for disabled timing check that no timing stats exist, so recording timing while it is off now fails them; three unused test helpers are removed. Test-only; the driver is unchanged ([#1327](https://github.com/cubrid-lab/pycubrid/issues/1327), [PR #1332](https://github.com/cubrid-lab/pycubrid/pull/1332)) by @yeongseon.

## [1.17.1] - 2026-11-19

This maintenance update improves source builds, setup and migration guidance, and core DB-API regression tests. The changes currently recorded below do not alter normal driver runtime behavior.

### Changed

- **Moved the dated live microbenchmark run out of the repository** — The raw 2026-10-14 pytest-benchmark data is removed from `main` and stays linked at commit `88d2c5b`; the Performance guide (English and Korean) now says `cubrid-benchmark` owns comparative results. The weekly trend job and benchmark tools are kept ([#1209](https://github.com/cubrid-lab/pycubrid/issues/1209), [#1250](https://github.com/cubrid-lab/pycubrid/pull/1250)) by @yeongseon.

- **Removed the `make changelog` target and `cliff.toml`** — The target would have overwritten the curated `CHANGELOG.md` that release automation reads, and nothing called it ([#1109](https://github.com/cubrid-lab/pycubrid/issues/1109), [#1225](https://github.com/cubrid-lab/pycubrid/pull/1225)) by @yeongseon.

- **Removed unused code and development tooling** — `PacketWriter.add_short`, three test-only cursor helpers, the `tox` and `wheel` dev dependencies, `make check-all` (run `make check`), the JSON GIF renderer and the Mermaid init script are gone. Public exports and driver behavior are unchanged ([#1168](https://github.com/cubrid-lab/pycubrid/issues/1168), [#1228](https://github.com/cubrid-lab/pycubrid/pull/1228), [#1229](https://github.com/cubrid-lab/pycubrid/pull/1229)) by @yeongseon.

### Fixed

- **Source builds** — Declared setuptools 77 or newer, because earlier versions reject the package's SPDX license metadata. The separate `wheel` build requirement is dropped; installing a published wheel is unaffected ([#1108](https://github.com/cubrid-lab/pycubrid/issues/1108), [PR #1148](https://github.com/cubrid-lab/pycubrid/pull/1148)) by @yeongseon.

### Documentation

- **README, PyPI page and first steps** — The README demo and Quick Start video now show `pycubrid==1.17.0` installed from PyPI, the PyPI project page uses absolute URLs so its demo and links work, and the context-manager example runs a read-only `SELECT 1 + 1` instead of creating a `users` table. The German, Hindi, Russian and Chinese READMEs gain a "Start here" section ([#1105](https://github.com/cubrid-lab/pycubrid/issues/1105), [#1251](https://github.com/cubrid-lab/pycubrid/pull/1251), [#1106](https://github.com/cubrid-lab/pycubrid/issues/1106), [#1215](https://github.com/cubrid-lab/pycubrid/pull/1215), [#1086](https://github.com/cubrid-lab/pycubrid/issues/1086), [#1226](https://github.com/cubrid-lab/pycubrid/pull/1226), [#1089](https://github.com/cubrid-lab/pycubrid/issues/1089), [#1214](https://github.com/cubrid-lab/pycubrid/pull/1214)) by @yeongseon.

- **Help and reporting path** — The issue chooser sends usage questions to GitHub Discussions, and SUPPORT.md, the README, both documentation home pages, Troubleshooting and the FAQ give the same routes for questions, bug reports and private security reports ([#1090](https://github.com/cubrid-lab/pycubrid/issues/1090), [#1227](https://github.com/cubrid-lab/pycubrid/pull/1227), [#1093](https://github.com/cubrid-lab/pycubrid/issues/1093), [#1221](https://github.com/cubrid-lab/pycubrid/pull/1221)) by @yeongseon.

- **Troubleshooting for Docker and first connections** — Docker examples wait for a real `testdb` login and bind port 33000 locally; quickstart-lab commands sit beside the `docker compose` ones; the database-not-found entry shows the actual `DatabaseError` and the `createdb` locale ([#1083](https://github.com/cubrid-lab/pycubrid/issues/1083), [PR #1146](https://github.com/cubrid-lab/pycubrid/pull/1146), [#1084](https://github.com/cubrid-lab/pycubrid/issues/1084), [#1219](https://github.com/cubrid-lab/pycubrid/pull/1219), [#1082](https://github.com/cubrid-lab/pycubrid/issues/1082), [#1218](https://github.com/cubrid-lab/pycubrid/pull/1218)) by @yeongseon.

- **Connection errors in Troubleshooting** — The port 33000 and timeout entries now start with the `OperationalError` that ends the traceback (`failed to connect to CUBRID broker`, `could not connect to <host>:<port>`, `read timeout during connect handshake`); anchors are unchanged ([#1080](https://github.com/cubrid-lab/pycubrid/issues/1080), [PR #1212](https://github.com/cubrid-lab/pycubrid/pull/1212)) by @Mr-Neutr0n.

- **Performance and Compat migration guides** — The Performance guide shows two tested ways to split large inputs across `executemany(prepared=True)`. The Compat guide shows how to read the isolation level and lock timeout with ordinary SQL, and no longer claims that rewrapping a LOB handle from another connection always raises `InterfaceError`; read the bytes and write a new LOB ([#1013](https://github.com/cubrid-lab/pycubrid/issues/1013), [PR #1152](https://github.com/cubrid-lab/pycubrid/pull/1152), [#1129](https://github.com/cubrid-lab/pycubrid/issues/1129), [PR #1136](https://github.com/cubrid-lab/pycubrid/pull/1136), [#1130](https://github.com/cubrid-lab/pycubrid/issues/1130), [PR #1141](https://github.com/cubrid-lab/pycubrid/pull/1141)) by @yeongseon.

- **Working documentation links** — Published pages and translated READMEs use repository URLs for files such as `SECURITY.md` instead of relative links that returned 404, and the strict MkDocs build detects a new broken link ([#1088](https://github.com/cubrid-lab/pycubrid/issues/1088), [PR #1147](https://github.com/cubrid-lab/pycubrid/pull/1147)) by @yeongseon.

- **Windows activation command** — Step 1 (English and Korean) also shows the PowerShell and cmd forms, marked unverified, and later lessons link back to it ([#1092](https://github.com/cubrid-lab/pycubrid/issues/1092), [PR #1159](https://github.com/cubrid-lab/pycubrid/pull/1159)) by @srijavuppala.

- **Runnable example scripts** — The Examples page names the five scripts in `examples/`, their environment variables and defaults, the run command and the tables left behind, with a `DROP TABLE` cleanup ([#1087](https://github.com/cubrid-lab/pycubrid/issues/1087), [PR #1208](https://github.com/cubrid-lab/pycubrid/pull/1208)) by @CloudCorpRecords (first contribution 🎉).

- **Documentation-site license inventory** — `THIRD_PARTY_LICENSES.md` now covers the build tools, redistributed Material for MkDocs theme files and assets fetched from other servers; licenses the shipped files do not state are marked not verified. The wheel and sdist are unchanged ([#1110](https://github.com/cubrid-lab/pycubrid/issues/1110), [#1220](https://github.com/cubrid-lab/pycubrid/pull/1220)) by @yeongseon.

- **Contributor and maintainer guidance** — Contribution rules, issue handoff and development states, when to freeze release candidates (only when their review starts) and the credit of contributors in unreleased notes now live in the repository, without project boards, contest deadlines or deprecated organization roadmap and board links and obsolete ecosystem roadmap references in translations. The contributing guide (English and Korean) gains a rule for temporary tooling ([#1169](https://github.com/cubrid-lab/pycubrid/issues/1169), [#1224](https://github.com/cubrid-lab/pycubrid/pull/1224)), and `pycubrid.connection` gains a module docstring ([#1162](https://github.com/cubrid-lab/pycubrid/pull/1162), [#1199](https://github.com/cubrid-lab/pycubrid/pull/1199), [#1200](https://github.com/cubrid-lab/pycubrid/pull/1200), [#1203](https://github.com/cubrid-lab/pycubrid/pull/1203), [#1213](https://github.com/cubrid-lab/pycubrid/pull/1213), [#1216](https://github.com/cubrid-lab/pycubrid/pull/1216), [#1230](https://github.com/cubrid-lab/pycubrid/pull/1230), [#1231](https://github.com/cubrid-lab/pycubrid/pull/1231), [#1237](https://github.com/cubrid-lab/pycubrid/pull/1237)) by @yeongseon.

- **Rewrite of the published release notes** — Notes from v0.1.0 to v1.17.0 are easier to scan and read: they standardize a reader-focused format, clarify initial, protocol foundations, connection and cursor support, CAS reconnection, DML batching, support boundaries, pooling, decoding, parameter compatibility, LOB safeguards, schema features, asyncio and timing improvements, SQLAlchemy setup and the Compat policy reversal, merge duplicate entries, trim tooling narration, pin links to release tags and restore verified credits for authors. v0.1.0 to v0.4.0 are marked never published to PyPI ([#1178](https://github.com/cubrid-lab/pycubrid/pull/1178), [#1181](https://github.com/cubrid-lab/pycubrid/pull/1181), [#1183](https://github.com/cubrid-lab/pycubrid/pull/1183), [#1184](https://github.com/cubrid-lab/pycubrid/pull/1184), [#1185](https://github.com/cubrid-lab/pycubrid/pull/1185), [#1186](https://github.com/cubrid-lab/pycubrid/pull/1186), [#1187](https://github.com/cubrid-lab/pycubrid/pull/1187), [#1188](https://github.com/cubrid-lab/pycubrid/pull/1188), [#1189](https://github.com/cubrid-lab/pycubrid/pull/1189), [#1190](https://github.com/cubrid-lab/pycubrid/pull/1190), [#1191](https://github.com/cubrid-lab/pycubrid/pull/1191), [#1192](https://github.com/cubrid-lab/pycubrid/pull/1192), [#1193](https://github.com/cubrid-lab/pycubrid/pull/1193), [#1194](https://github.com/cubrid-lab/pycubrid/pull/1194), [#1195](https://github.com/cubrid-lab/pycubrid/pull/1195), [#1196](https://github.com/cubrid-lab/pycubrid/pull/1196), [#1197](https://github.com/cubrid-lab/pycubrid/pull/1197), [#1198](https://github.com/cubrid-lab/pycubrid/pull/1198), [#1232](https://github.com/cubrid-lab/pycubrid/pull/1232), [#1233](https://github.com/cubrid-lab/pycubrid/pull/1233), [#1234](https://github.com/cubrid-lab/pycubrid/pull/1234), [#1235](https://github.com/cubrid-lab/pycubrid/pull/1235), [#1236](https://github.com/cubrid-lab/pycubrid/pull/1236), [#1239](https://github.com/cubrid-lab/pycubrid/pull/1239), [#1240](https://github.com/cubrid-lab/pycubrid/pull/1240), [#1241](https://github.com/cubrid-lab/pycubrid/pull/1241), [#1242](https://github.com/cubrid-lab/pycubrid/pull/1242), [#1243](https://github.com/cubrid-lab/pycubrid/pull/1243), [#1244](https://github.com/cubrid-lab/pycubrid/pull/1244), [#1259](https://github.com/cubrid-lab/pycubrid/pull/1259), [#1260](https://github.com/cubrid-lab/pycubrid/pull/1260), [#1261](https://github.com/cubrid-lab/pycubrid/pull/1261), [#1262](https://github.com/cubrid-lab/pycubrid/pull/1262), [#1264](https://github.com/cubrid-lab/pycubrid/pull/1264), [#1270](https://github.com/cubrid-lab/pycubrid/pull/1270), [#1271](https://github.com/cubrid-lab/pycubrid/pull/1271), [#1272](https://github.com/cubrid-lab/pycubrid/pull/1272), [#1273](https://github.com/cubrid-lab/pycubrid/pull/1273), [#1274](https://github.com/cubrid-lab/pycubrid/pull/1274), [#1275](https://github.com/cubrid-lab/pycubrid/pull/1275), [#1276](https://github.com/cubrid-lab/pycubrid/pull/1276)) by @yeongseon. The style guide now asks for one or two sentences per entry, and the 1.17.1 notes follow it ([#1277](https://github.com/cubrid-lab/pycubrid/pull/1277), [#1278](https://github.com/cubrid-lab/pycubrid/pull/1278)) by @yeongseon.

### CI

- **Release notes list each change once** — The release composer removes correlated duplicate changelog entries when a curated change cites the same pull request, and drops a bare bullet repeated for one squash commit when exactly one other bullet names it with a pull request link. Ambiguous cases are kept ([#1111](https://github.com/cubrid-lab/pycubrid/issues/1111), [#1182](https://github.com/cubrid-lab/pycubrid/pull/1182), [#1238](https://github.com/cubrid-lab/pycubrid/pull/1238)) by @yeongseon.

- **Weekly checks and release tests** — The cookbook check runs the moved AI-agent tests ([#1222](https://github.com/cubrid-lab/pycubrid/issues/1222), [#1223](https://github.com/cubrid-lab/pycubrid/pull/1223)), and the mutation lane collects its baseline again after copying the files the tests read into mutmut's sandbox and deselecting three tests that inspect rewritten functions ([#1107](https://github.com/cubrid-lab/pycubrid/issues/1107), [#1217](https://github.com/cubrid-lab/pycubrid/pull/1217)). Release-script-only pull requests run a focused `release-tests` job instead of the CUBRID lanes ([#1157](https://github.com/cubrid-lab/pycubrid/issues/1157), [#1158](https://github.com/cubrid-lab/pycubrid/pull/1158)) by @yeongseon.

### Tests

- **Independent DB-API and test-inventory regressions** — New sync and async tests cover result metadata, commit and rollback visibility, isolation levels, collection decoding and multibyte CLOB contents without Compat fixtures; Compat-specific tests remain until its separately gated removal ([#1123](https://github.com/cubrid-lab/pycubrid/issues/1123), [PR #1131](https://github.com/cubrid-lab/pycubrid/pull/1131)) by @yeongseon. New offline tests check integration-lane test classification ([#1079](https://github.com/cubrid-lab/pycubrid/issues/1079), [PR #1201](https://github.com/cubrid-lab/pycubrid/pull/1201)) by @Noah-Mengesha (first contribution 🎉).

## [1.17.0] - 2026-11-05

Creating a `pycubrid.compat` connection now emits a `FutureWarning`, and this release explicitly withdraws the earlier promise that the `pycubrid.compat` names shipped in v1.8.0 would stay supported for all of 1.x. They may be removed in a future **minor** release after the notice and migration steps described below; **no removal version or date has been set**. The ordinary `pycubrid` and `pycubrid.aio` APIs keep the major-version compatibility policy.

### Upgrade notes

- **The 1.x support guarantee for the 1.8.0 `pycubrid.compat` names is withdrawn (#1124).** The 1.9.0 Upgrade note and the release policy promised that these names would stay supported for the rest of 1.x. That is a deliberate reversal, not a clarification, and the names were never provisional: they remain supported until they are retired through the process below. A later 1.x MINOR release may remove these namespaces only after an earlier published MINOR release has shipped a visible warning and a migration guide (this release is that warning), and at least 28 calendar days after that release reaches PyPI. No removal version or date is set, so removal is possible but not committed. A range such as `pycubrid>=1.8.0,<2.0` can install the removing release automatically; if you still use these namespaces when a removal is announced, pin below that version. For `pycubrid` and `pycubrid.aio`, breaking changes stay MAJOR-only, and PATCH releases stay compatible except for a security fix that cannot be made compatibly (it changes no more than the fix requires and carries its own Upgrade note). `pycubrid.compat` now receives bug and security fixes only. See the [release policy](https://github.com/cubrid-lab/pycubrid/blob/v1.17.0/RELEASE_POLICY.md#deprecation-based-retirement-in-a-minor-release) by @yeongseon.
- **Creating a `pycubrid.compat` connection now emits `FutureWarning` (#1120).** Your code keeps working, and Python shows the warning once per calling line by default. Importing the compat modules, and using `pycubrid` or `pycubrid.aio`, does not warn. If you run with warnings as errors (`python -W error`, or `filterwarnings = error` in pytest), compat connections now raise `FutureWarning` until you migrate or add a narrow filter for that message. Migrating is more than a rename: Compat starts in autocommit and `pycubrid.connect()` does not, so code that never calls `commit()` would lose writes. Follow the [migration guide](https://github.com/cubrid-lab/pycubrid/blob/v1.17.0/docs/COMPAT_MIGRATION.md), which lists what has no ordinary replacement. Contributed by @yeongseon.

### Added

* **compat:** warn that the compat namespaces are deprecated ([#1128](https://github.com/cubrid-lab/pycubrid/issues/1128)) ([24c4518](https://github.com/cubrid-lab/pycubrid/commit/24c4518fd848876d2da3b8998c4c9e7116584f01)) by @yeongseon.

### Deprecated

- **`pycubrid.compat` (#1120)** — `pycubrid.compat.native`, `pycubrid.compat.cubriddb` and `pycubrid.compat.cursors`, the opt-in imitation of the official `_cubrid` and `CUBRIDdb` APIs, are deprecated in favor of `pycubrid` and `pycubrid.aio`. Every factory warns at the caller's line before any socket work; see Upgrade notes for the effects. Contributed by @yeongseon.

### Documentation

* **release:** adopt deprecation-based MINOR retirement for Compat ([#1125](https://github.com/cubrid-lab/pycubrid/issues/1125)) ([2c52d45](https://github.com/cubrid-lab/pycubrid/commit/2c52d459999ac5a2332f2de51921bd01e4775ab1)) by @yeongseon.

## [1.16.0] - 2026-11-05

This release adds a limited `schema_info()` compatibility surface to the opt-in `pycubrid.compat.native` connection. Only the verified first-row CLASS/VCLASS and ATTRIBUTE/CLASS_ATTRIBUTE cases are supported; **this is not broad official driver parity**. The limitations and live CUBRID versions tested are detailed below.

### Added

- **Native `connection.schema_info()` first-row subset (#1011)** — the opt-in
  `pycubrid.compat.native.connection` gains positional-only
  `schema_info(schema_type, class_name[, attr_name])`, matching the pinned official
  `_cubrid.connection.schema_info`: it returns the first CLASS/VCLASS (pattern flag 1) or
  ATTRIBUTE/CLASS_ATTRIBUTE (flag 2) row as a `list` of `str`/`int`/`None`, or `None` when no
  row exists, with the CCI DOMAIN mask (a `SET(INT)` column reports `8`). Other in-range schema
  types raise `NotSupportedError` before any request, out-of-range types `InterfaceError`, and
  argument errors the official exception classes; unencodable text raises `DataError`. The
  owned FC9 handle is always closed on the connection's current physical session, without
  retry or reconnect, also on the no-row and error paths that the official extension leaves
  open. A live differential against the official driver on CUBRID 10.2 and 11.4 covers the
  supported types. The ordinary `get_schema_info()`, `fetch_schema_info()` and
  `close_schema_info()` are unchanged ([#1057](https://github.com/cubrid-lab/pycubrid/pull/1057)) ([4e110ec](https://github.com/cubrid-lab/pycubrid/commit/4e110ec29c1c5679383bba3e8c7bf65f835cb3e1)). Contributed by @yeongseon.

### Documentation

- **Troubleshooting logging example works on a fresh database (#1085)** — the English and Korean
  examples query the built-in `db_class` catalog and close their cursor and connection ([#1104](https://github.com/cubrid-lab/pycubrid/pull/1104)) ([818dd04](https://github.com/cubrid-lab/pycubrid/commit/818dd04dfdd43c86223e5cdb6a04ba0eff866a6f)). Contributed by @goktugekin.
- **Docs FAQ names supported Python versions (#1094)** — the English and Korean site FAQs now
  state Python 3.11–3.15 support, the standard GIL build requirement, and the Python 3.10
  fallback to 1.9.x. The Support Matrix remains the authoritative compatibility reference ([#1099](https://github.com/cubrid-lab/pycubrid/pull/1099)) ([5b6a814](https://github.com/cubrid-lab/pycubrid/commit/5b6a814ac6fdcaf4c41afdc4cfe8d82a366a51e0)). Contributed by @goktugekin (first contribution 🎉).
- **README and docs site show the transparent brand icon (#1096)** — the icon URL pinned in the README, its translations and `mkdocs.yml` (logo and favicon) now points at brand revision `e55990a`, where the Driver icon's layer gaps are real transparency instead of white strokes. Documentation only; behavior is unchanged ([#1097](https://github.com/cubrid-lab/pycubrid/pull/1097)) ([d9e2e1b](https://github.com/cubrid-lab/pycubrid/commit/d9e2e1b8cb5c02a4af205e2d7b61469f738b7c25)). Contributed by @yeongseon.
- **Troubleshooting lists the connection-closed errors the driver raises (#1095)** — the troubleshooting guide now names the errors raised when a connection has been closed ([#1095](https://github.com/cubrid-lab/pycubrid/issues/1095)) ([232c15e](https://github.com/cubrid-lab/pycubrid/commit/232c15ebd3129dbe345da8d3bb426f7eb6dc4c04)). Contributed by @bfs2021 (first contribution 🎉).
- **Bug report template asks for environment details (#1102)** — the bug report guidance now states which environment information to include ([#1102](https://github.com/cubrid-lab/pycubrid/issues/1102)) ([e588438](https://github.com/cubrid-lab/pycubrid/commit/e588438a7a8e004f4fdda430ca1f746e7b59344b)). Contributed by @Vamsi-2581 (first contribution 🎉).
- **Contributor, agent and site-theme housekeeping (#1062, #1067, #1103, #1117)** — issue titles follow the organization writing standard ([#1067](https://github.com/cubrid-lab/pycubrid/issues/1067) ([629a3ac](https://github.com/cubrid-lab/pycubrid/commit/629a3ac3b9e8492207db9ac092d78d5abcefdd7c))), agents are asked for human-readable communication and self-explanatory comments ([#1062](https://github.com/cubrid-lab/pycubrid/issues/1062) ([83fbbca](https://github.com/cubrid-lab/pycubrid/commit/83fbbca04828b15ea24fe4d3e962d187deb77940))), the site theme takes its Python yellow from the brand tokens ([#1103](https://github.com/cubrid-lab/pycubrid/issues/1103) ([c18d35a](https://github.com/cubrid-lab/pycubrid/commit/c18d35a3057e879989868163481068a351bdcbaa))), and the `schema_info` changelog note moved to the unreleased section ([#1117](https://github.com/cubrid-lab/pycubrid/issues/1117) ([28b7cdc](https://github.com/cubrid-lab/pycubrid/commit/28b7cdcc16917792393aaccaa0974b905ab178c6))). No driver behavior changed. Contributed by @yeongseon.


### Tests

- **Release and profiling script tests** — Added offline tests that pin how the benchmark regression script parses its input and reports in report-only mode, when the fetch profiler loop terminates, the call shapes of the execute profiler's DML helpers, the fail-closed exits of `extract_release_notes`, and the license and URL lookup order of the inventory generator ([#1073](https://github.com/cubrid-lab/pycubrid/pull/1073), [#1074](https://github.com/cubrid-lab/pycubrid/pull/1074), [#1075](https://github.com/cubrid-lab/pycubrid/pull/1075), [#1100](https://github.com/cubrid-lab/pycubrid/pull/1100), [#1101](https://github.com/cubrid-lab/pycubrid/pull/1101)) by @kcncb (first contribution 🎉).

## [1.15.0] - 2026-10-27

This release improves reproducible test-result reporting and documentation presentation. `make verify` now records per-phase summaries, artifacts and interrupted or incomplete outcomes; it does not change ordinary driver query behavior.

### Added

- **`make verify` writes an aggregate summary under `test-results/verify/` (#1053)** — the
  recipe now `exec`s `scripts/run_verify.py`, which runs `check`, `test` and `integration`
  in order and always writes `summary.json` (overall `PASS`/`FAIL`/`INCOMPLETE` and one entry
  per phase, `NOT_RUN` for a phase that never started), `junit/<lane>.xml` copies and the live
  lane's `environment.json`, also after a failure or a signal. Configured test passwords are
  masked in every copy; a missing or unreadable lane summary or a failed copy is never `PASS`.
  The runner forwards the first SIGINT, SIGTERM or SIGHUP to the running phase as SIGTERM and
  exits 128 + N; `make integration` now `exec`s its runner and also cleans up on SIGHUP.
  `make -n verify` writes nothing. Development tooling only; the driver API and runtime are
  unchanged ([#1054](https://github.com/cubrid-lab/pycubrid/pull/1054)) ([a7e0be2](https://github.com/cubrid-lab/pycubrid/commit/a7e0be2a80f79331cbdf4fec8770c5c3ab645ea4)). Contributed by @yeongseon.

### Documentation

- **README and docs site show the updated brand icon (#1058, #1060, #1069, #1071)** — the icon URL pinned in the README, its translations and `mkdocs.yml` (logo and favicon) was moved to a newer brand revision four times during this release: `8aa81b7` (approved CUBRID Lab icon, [#1059](https://github.com/cubrid-lab/pycubrid/pull/1059), ([a3f7d6b](https://github.com/cubrid-lab/pycubrid/commit/a3f7d6bb30652a592d43a8f492cc88bdcd5d459c))), `38b4646` (redesigned Driver icon, [#1061](https://github.com/cubrid-lab/pycubrid/pull/1061), ([675739e](https://github.com/cubrid-lab/pycubrid/commit/675739ee5fc541fac02f6b289aa533eeb42daba8))), `538bad3` (Python Yellow keyline that keeps the cube silhouette of the organization emblem, [#1070](https://github.com/cubrid-lab/pycubrid/pull/1070), ([223d83a](https://github.com/cubrid-lab/pycubrid/commit/223d83a85e187d766f3b256790b3f15671ecd5d0))) and finally `3a385be` (Python Yellow icons draw white role marks on a bare cube like every other language, [#1072](https://github.com/cubrid-lab/pycubrid/pull/1072), ([5dcd360](https://github.com/cubrid-lab/pycubrid/commit/5dcd3608bac3d1e3f71ba85e636a5de6eb974c79))), which this release ships. Documentation only; behavior is unchanged. Contributed by @yeongseon.

- **Profilers warn that they write to the target database** ([#1063](https://github.com/cubrid-lab/pycubrid/issues/1063)) ([1789ebc](https://github.com/cubrid-lab/pycubrid/commit/1789ebc32fab0a2648cbad21f567e3da32ed1e53)), refs [#918](https://github.com/cubrid-lab/pycubrid/issues/918). Contributed by @paikend.

### CI

- **CI installs third-party dependencies from a committed `uv.lock`** — CI, full
  integration and documentation jobs install locked, hash-checked wheels with source builds
  disabled, and build the trusted checkout separately with its locked backend. Dependabot
  updates `pyproject.toml` and `uv.lock` together through its uv ecosystem. Python 3.15 uses a
  documented pure Python PyYAML 6.0.3 wheel until upstream provides a compatible wheel. No
  driver API or runtime behaviour changes ([#981](https://github.com/cubrid-lab/pycubrid/pull/981)) by @ryanduguid.

### Tests

- **Makefile tests are isolated from the outer `make` and the repository's results (#1052)** —
  the tests that run the real Makefile now drop `MAKEFLAGS`, `MFLAGS`, `MAKEOVERRIDES`,
  `MAKELEVEL` and every variable the Makefile assigns from the child environment, so
  `make test PYTHON=.venv/bin/python` (or an exported `CUBRID_TEST_PORT`) no longer replaces
  the defaults they assert. The test that runs the real `make test` recipe writes its results
  under its temporary directory instead of deleting the checkout's `test-results/test/`,
  which a `make verify` in the same checkout reads. No Makefile, driver or runtime change. Contributed by @yeongseon.

## [1.14.0] - 2026-10-25

This release changes developer verification: `make verify` now runs the live CUBRID integration lane after static and offline checks. A machine running that command therefore needs Docker and a working test broker; `make test` stays offline. The specific CI and failure behavior is documented below.

### Changed

- **`make verify` now runs the live integration lane (#1022)** — it runs `make check`, the
  offline `make test` and then the managed `make integration` lane, so it needs Docker and fails
  when CUBRID never becomes ready or every live test skips. The live lane starts only after
  the offline tests pass. `make test` stays offline and needs no Docker or database; use
  `make check test` for the former offline-only `make verify`. Development tooling only; the
  driver API and runtime are unchanged ([#1050](https://github.com/cubrid-lab/pycubrid/pull/1050)) ([4f63593](https://github.com/cubrid-lab/pycubrid/commit/4f63593d3d7e456edececf75ba566d9ed65f758f)). Contributed by @yeongseon.

### CI

- **Every new issue starts in `status: needs triage` (#1033)** — the issue forms add the
  label and `issue-triage.yml` adds it to every opened issue instead of only to issues with
  incomplete titles or labels; the unused `scripts/check_issue_metadata.py` check is removed.
  Priority and size are still set by a maintainer, who removes the label after triage. Contributed by @yeongseon.

## [1.13.5] - 2026-10-25

This documentation-only release clarifies contributor attribution and the validation evidence expected for repository changes. No driver runtime or public API change is recorded for this release.

### Documentation

* **agents:** clarify commit attribution and validation evidence ([#1048](https://github.com/cubrid-lab/pycubrid/issues/1048)) ([48d8d0e](https://github.com/cubrid-lab/pycubrid/commit/48d8d0eeca8341422d29622a8bb2148e0327a2ed)) by @yeongseon.

## [1.13.4] - 2026-10-24

This maintenance release improves the safety of stored test-run metadata by masking common password and secret patterns; it also records the corresponding documentation changes. Redaction is best-effort and does not replace keeping credentials out of command lines.

### Fixed

- **`environment.json` masks credentials in the saved command (#1042)** —
  `scripts/report_results.py` copied the `--command` text verbatim, so an authenticated URL
  or a password assignment on the command line could be written to `test-results/` and its
  uploaded artifact. The password of a `scheme://user:password@host` URL and the value of a
  `password`, `passwd`, `pwd`, `token`, `secret` or `api_key` key (`--password=x`,
  `--password x`, `CUBRID_PASSWORD=x`) are now saved as `***`; any other command text is
  unchanged. This is best-effort masking: keep real credentials off command lines. The
  schema, statuses and exit codes are unchanged ([#1046](https://github.com/cubrid-lab/pycubrid/pull/1046), [80e9c07](https://github.com/cubrid-lab/pycubrid/commit/80e9c07caf8ccaabcbdc8caa0f0c4b03d197af0f)). Contributed by @yeongseon.

### Documentation

* **examples:** add a bounded opt-in prepared executemany example and document prepared executemany batches ([#1039](https://github.com/cubrid-lab/pycubrid/issues/1039)) ([1f1ddd9](https://github.com/cubrid-lab/pycubrid/commit/1f1ddd994403e19eb8bfe0176cd9a4531189a62d)), refs [#891](https://github.com/cubrid-lab/pycubrid/issues/891) by @WilliamK112.

## [1.13.3] - 2026-10-24

This is the next actually published release after v1.13.1. **v1.13.2 was never published**, so the release includes that candidate's intended changes along with the fixes documented below; do not treat v1.13.2 as an available PyPI or GitHub version.

### Upgrade notes

- **1.13.2 was never published (#1016).** Its cookbook candidate verification failed (the
  pinned cookbook's `fundamentals/pandas/03_clean_and_transform` golden mismatched under
  pandas 3.1.0), so no 1.13.2 tag, GitHub Release or PyPI version exists. 1.13.3 contains
  every 1.13.2 change below; upgrade from 1.13.1 directly to 1.13.3.

### Added

- **`make format-check` and `make verify` (#1007)** — `make format-check` runs only
  `ruff format --check`; `make verify` runs `make check` and then `make test`, without
  Docker. `scripts/run_integration.py` now runs the Docker integration lanes. Contributed by @yeongseon.
- **`make docker-clean` (#1006)** — runs `docker compose down -v`: it removes the CUBRID
  container, its network and the `cubrid-data` and `cubrid-databases` volumes with all test
  data. It is now the only Make target that deletes them. Contributed by @yeongseon.

### Changed

- **`make test` and `make integration` write `test-results/` (#1024)** — each run now leaves
  `junit.xml`, `coverage.xml`, `summary.json` and `environment.json` (plus `service.log` after
  a failed integration run) under `test-results/<lane>/`, and `scripts/report_results.py`
  prints a one-line `PASS`, `FAIL` or `INCOMPLETE` verdict; a skipped or unexecuted lane is
  never `PASS`. Exit codes and CI are unchanged. The directory is gitignored and `make clean`
  removes it. `INTEGRATION_RESULTS` no longer defaults to `integration-results.xml` in the
  repository root. Contributed by @yeongseon.
- **Makefile targets have one meaning each (#1007)** — `make lint` runs only `ruff check` and
  `make format` is the only target that modifies files. `make check` is the complete static
  gate (`lint`, `format-check`, `typecheck`, `security`, `docs-reason-check`). `make doctor`
  exits non-zero when a required tool is missing or fails `--version`. The tool variables
  (`PIP`, `PYTEST`, `RUFF`, `MYPY`, `BANDIT`, `PRE_COMMIT`) can be overridden with `?=`, and
  `make help` groups the targets by purpose. Contributed by @yeongseon.
- **`make integration` and `make integration-tls` call `scripts/run_integration.py`
  (#1007)** — the script owns the Compose lifecycle, readiness, signal handling, cleanup and
  the every-test-skipped audit. Test selection, the pinned endpoint and volume handling are
  unchanged (cleanup runs `docker compose down`, never `-v`), and cleanup now also runs when
  `docker compose up` fails. Contributed by @yeongseon.
- **`make docker-down` keeps the test data (#1006)** — it now runs `docker compose down`,
  which removes the container and network but keeps the named data volumes, so the next
  `make docker-up` opens the same database. It used to run `docker compose down -v` and
  delete them; use `make docker-clean` for that. `make docker-up`, `make docker-down` and
  `make docker-clean` pass `CUBRID_TEST_PORT` to Compose. Contributed by @yeongseon.
- **The Compose broker listens only on `127.0.0.1` (#1006)** — `docker-compose.yml`
  publishes it as `127.0.0.1:${CUBRID_TEST_PORT:-33000}:33000` instead of on every host
  interface, so the test database is no longer reachable from other machines. Connections
  to `localhost` or `127.0.0.1` on the host, `make integration` and the readiness probe are
  unchanged. The readiness probe's stale-host hint now names `make docker-clean` as the
  destructive fix. Contributed by @yeongseon.

### Deprecated

- **`make check-all` (#1007)** — an alias of `make check` that prints a notice on stderr.
  It will be removed after the next minor release; use `make check`. Contributed by @yeongseon.

### Fixed

- **`make integration` no longer attaches to or removes the `make docker-up` lab (#1021)** —
  it used the default Compose project, so a run could attach to a manually started CUBRID and
  remove its container and data volumes. Every `docker compose` call now uses a run-unique
  project `pycubrid-it-<timestamp>-<pid>` (override with `INTEGRATION_PROJECT=<name>`; an
  exported `COMPOSE_PROJECT_NAME` is ignored). The run refuses a project that already has a
  container, volume or network (exit 1) and a second run of the same name (POSIX `flock`),
  and cleans up only its own project with `docker compose -p <project> down -v`, so each run
  starts from an empty database and leaves no volumes. Readiness, the lane audit and exit
  statuses are unchanged ([#1027](https://github.com/cubrid-lab/pycubrid/pull/1027), [588cd8a](https://github.com/cubrid-lab/pycubrid/commit/588cd8affe771e1ca7b741b7022d84fa94e69a58)). Contributed by @yeongseon.

### Documentation

- **CAS architecture and protocol concepts page (#1015)** — a new advanced overview,
  `docs/CAS_OVERVIEW.md` with its Korean translation, listed under Reference in the site
  navigation. It separates the Broker, the CAS process, the CAS wire protocol and CCI,
  draws the pycubrid pure-Python path next to the `CUBRIDdb` → `_cubrid` → CCI path,
  maps `connect()`, `execute()`, fetch, commit and close to function codes, and covers
  the frame, the literal versus `executemany(..., prepared=True)` binding paths,
  `CHECK_CAS`, unknown outcomes without replay, client-side TLS, and CUBRID CAS (Common
  Application Server) versus Compare-And-Swap. `PROTOCOL.md` and `ARCHITECTURE.md` stay
  authoritative; the README, the site index, the connection guide and both reference
  pages link to the overview. No driver behavior changed ([#1019](https://github.com/cubrid-lab/pycubrid/pull/1019), [dd70ef1](https://github.com/cubrid-lab/pycubrid/commit/dd70ef1f2d05aa7e9cf2d619eedd28a294e24b73)). Contributed by @yeongseon.

* **agents:** require repository-local issue authoring checks and enforce repository-local issue drafting ([#1014](https://github.com/cubrid-lab/pycubrid/issues/1014)) ([03ca2f9](https://github.com/cubrid-lab/pycubrid/commit/03ca2f96de47865cd3f596755b3bda8ae1156bae)) by @yeongseon.
* **triage:** clarify PR overlap and execution precedence ([d2c9318](https://github.com/cubrid-lab/pycubrid/commit/d2c93180672105b46cf1d29797cc5808386a85d6)), refs [#1025](https://github.com/cubrid-lab/pycubrid/issues/1025) by @yeongseon.

### CI

* **release:** verify against the current cookbook and restart after the abandoned 1.13.2 ([#1040](https://github.com/cubrid-lab/pycubrid/issues/1040)) ([be7817e](https://github.com/cubrid-lab/pycubrid/commit/be7817e17ec59397af105dfe193eee550afdd38c)), refs [#1016](https://github.com/cubrid-lab/pycubrid/issues/1016) by @yeongseon.

- **Every live CI job runs the Make contract or documents why not (#1023)** — each live-CUBRID
  job in `ci.yml`, `integration-full.yml` and `bug-hunt.yml` now carries a `# Not using make
  integration because ...` comment, with the reasons listed per job in docs/DEVELOPMENT.md,
  and `tests/test_workflow_make_contract.py` fails when a new live job has neither. No job,
  step, permission, secret or trigger changed. Contributed by @yeongseon.

- **CI publishes the test-results summary and uploads it on every run (#1024)** — the offline,
  integration and TLS integration jobs of `ci.yml` and `integration-full.yml` add the
  one-line summary to the job summary and upload `test-results/` as a 7-day artifact per
  matrix entry, even when the job fails. Test selection, coverage, Codecov, permissions,
  secrets and triggers are unchanged. Contributed by @yeongseon.

- **The lint lanes run `make check` (#1007)** — `ci.yml`, its release copy in
  `integration-full.yml` and `maintenance.yml` call `make check` instead of `make lint` (and,
  in `maintenance.yml`, a separate Bandit command) by @yeongseon.
- **mypy runs once per code pull request and not on docs-only ones (#1009)** — the always-on
  `lint` job in `ci.yml` (and its copy in `integration-full.yml`) no longer runs `typecheck`;
  mypy runs only in the `typecheck` job, which runs for code changes and is still required by
  the CI gate. `make check` and `maintenance.yml` are unchanged. Contributed by @yeongseon.

### Tests

- **Offline tests for repository tooling** — new offline tests cover the benchmark regression report, the connection profiling cycles, the README translation-sync policy and the readiness retries of `scripts/wait_for_cubrid.py` (now in the tooling CI lane). Two behavior changes: the prepared-batch measurement CLI rejects non-positive `--rows` before any database work, and `scripts/generate_llms_full.py` fails before overwriting anything when a listed source page is missing ([#1029](https://github.com/cubrid-lab/pycubrid/pull/1029), [#1030](https://github.com/cubrid-lab/pycubrid/pull/1030), [#1031](https://github.com/cubrid-lab/pycubrid/pull/1031), [#1034](https://github.com/cubrid-lab/pycubrid/pull/1034), [#1037](https://github.com/cubrid-lab/pycubrid/pull/1037), [#1038](https://github.com/cubrid-lab/pycubrid/pull/1038)) by @WilliamK112 (first contribution 🎉).

## [1.13.1] - 2026-10-21

This patch release fixes several result, error and session behaviors: empty `BIT`/`BIT VARYING` values, the error for a `?` inside a quoted identifier, subnormal and `-0.0` floats, connection contexts, and LOB handles. **Review the upgrade notes and the `Fixed` entries:** code that tests `v is None` for an empty `BIT` value, or that classifies errors by `sqlstate`, may be affected. The release also adds a beginner quickstart and strengthens the documentation checks and Docker test setup; these change no driver behavior.

### Upgrade notes

- **Empty `BIT`/`BIT VARYING` values (#994)** — code that tests `v is None` on a `BIT` or
  `BIT VARYING` column to detect an empty value now sees `b""` for `X''`; `NULL` is still
  `None`. With `decode_collections=True`, an empty `BIT`/`BIT VARYING` collection element
  is `b""` too (#998) by @yeongseon and @paikend.

### Changed

- **The execute and fetch profilers no longer drop an existing `_profile_tmp` table (#950)** — `scripts/profile_execute.py` and `scripts/profile_fetch.py` used to run `DROP TABLE IF EXISTS _profile_tmp` before every run, which could delete an unrelated table of that name or another run's table. Each run now creates and drops only its own `_profile_tmp_<12 hex digits>` table (a name collision fails the run and drops nothing). After a failed workload the run rolls back, drops its table and closes the cursor and connection, keeping the workload error; a cleanup failure after a successful workload raises `ScratchCleanupError`. A killed run or lost connection can still leave the table behind. The driver is unchanged ([#962](https://github.com/cubrid-lab/pycubrid/issues/962), [b01a663](https://github.com/cubrid-lab/pycubrid/commit/b01a66336f1e7d608397daa20fb9e73c1e7f8cf4)). Contributed by @yeongseon.

### Fixed

- **An empty `BIT VARYING` value is fetched as `b""`, not `None` (#994)** — the broker sends NULL and an empty bit string (`X''`, or `b""` bound on the literal path) differently, but the row decoder read both as NULL, so a read-modify-write turned a stored empty value into NULL. A `BIT`/`VARBIT` cell of size 0 now decodes as `b""`, sync and async, including after a CALL or NULL-typed cell header, as the official JDBC driver reads it. Size-0 cells of every other type still decode as `None`. The prepared path still rejects `b""`, because the broker reads a zero-length bind as NULL ([#995](https://github.com/cubrid-lab/pycubrid/pull/995), [87ed36a](https://github.com/cubrid-lab/pycubrid/commit/87ed36ae0c7ebd1895791591794a20307e11fe9a)). Contributed by @yeongseon and @paikend.

- **An empty `BIT VARYING` collection element is decoded as `b""`, not `None` (#998)** — with `decode_collections=True`, `SET{X''}` decoded to `frozenset({None})` and `SEQUENCE{X'', NULL, X'01'}` to `[None, None, b'\x01']`. A `BIT`/`VARBIT` element of length 0 now decodes as `b""`, sync and async, matching row cells (#994). Zero-length elements of other types still decode as `None`; collection size and consumed-bytes validation and raw collection bytes (`decode_collections=False`) are unchanged ([#999](https://github.com/cubrid-lab/pycubrid/pull/999), [224b4be](https://github.com/cubrid-lab/pycubrid/commit/224b4be6bfe809a693bfcc440fb61ef5c9ba1feb)). Contributed by @yeongseon.

- **A `?` inside a quoted identifier raises an explained `ProgrammingError` (#990)** — the CUBRID broker counts every `?` outside comments and single-quoted strings as a bind marker, including one inside a quoted identifier (or a bracket identifier such as `[a?b]`), so `execute('SELECT 1 AS "a?b"')` failed with a `DatabaseError` that had no message (`errno=-1007`, CAS code `CAS_ER_NUM_BIND`). A CAS-indicator `-1007` reply now raises `ProgrammingError` whose message names this cause. `errno`, `code` and `error_source` are unchanged, but `sqlstate` changes from `HY000` to `42000` with the new class. A server-indicator `-1007` is not affected, and the statement itself still fails ([#991](https://github.com/cubrid-lab/pycubrid/pull/991), [bb07791](https://github.com/cubrid-lab/pycubrid/commit/bb0779125793404ab90ff8e818e7ff33d5ec52a3)). Contributed by @paikend.

- **Subnormal floats and `-0.0` round-trip through the literal path (#988)** — `execute()` and the default `executemany()` rendered a `float` with `float.__repr__`. CUBRID rejects every subnormal literal (`5e-324` failed with -493 "exceeds limit of double") and reads the literal `-0.0` as NUMERIC, so a `DOUBLE` column stored `0.0`; the prepared paths were already correct. A subnormal value is now sent as the exact product of two normal doubles, and `-0.0` as the DOUBLE literal `-0.0e0`. Every other `float` renders as before, so stored NUMERIC and string values do not change, with two visible exceptions: `-0.0` into a string column now stores `-0` instead of `0.0`, and `SELECT ?` bound to `-0.0` now returns the `float` `-0.0` instead of `Decimal('0.0')` ([#989](https://github.com/cubrid-lab/pycubrid/pull/989), [2ed16bc](https://github.com/cubrid-lab/pycubrid/commit/2ed16bc1b117fc3837a39924d9d7925be08f3bf3)). Contributed by @paikend. The `sqlstate` and result-type changes of this and the previous entry are recorded in the parameter-binding documentation ([#993](https://github.com/cubrid-lab/pycubrid/issues/993), [72f15c1](https://github.com/cubrid-lab/pycubrid/commit/72f15c14eb8c3ad85dac9e8aa781df6fc0f0e010), refs [#992](https://github.com/cubrid-lab/pycubrid/issues/992)) by @yeongseon.

- **Tasks waiting on async connection setup keep `error_source` (#952)** — when `AsyncConnection` setup failed with a `DatabaseError`, each waiting task received a copy with the same class, message, `code`, `errno` and `sqlstate` but with `error_source` set to `None`, even when the original reported `"server"` or `"cas"`. The copy now keeps `error_source` exactly (including `None`), also when a subclass with another constructor falls back to the nearest `pycubrid.exceptions` class. Each waiter still gets its own exception instance chained to the original as `__cause__`, and cancellation handling is unchanged ([#961](https://github.com/cubrid-lab/pycubrid/issues/961), [4cde43a](https://github.com/cubrid-lab/pycubrid/commit/4cde43afd2d84a4bdd4003ebcecdac01efc7f095), refs [#952](https://github.com/cubrid-lab/pycubrid/issues/952)). Contributed by @yeongseon.

- **Leaving a connection context keeps the original error of a retired session (#951)** — when the body of `with conn:` or `async with conn:` failed in a way that had already retired the physical session (a transport failure, or a cancelled `AsyncConnection` request), the exit's rollback raised `InterfaceError('connection is closed')` and replaced the original error, and a cancelled asyncio task finished failed instead of cancelled. The exit now only closes a retired session (no rollback, reconnect or statement replay), so the original `OperationalError` or `asyncio.CancelledError` propagates. A rollback that fails on a live session is logged as a `pycubrid` warning and noted on the body exception instead of replacing it. Commit on success, a commit failure on a successful body, rollback on a healthy failure and a new cancellation during cleanup are unchanged ([#959](https://github.com/cubrid-lab/pycubrid/issues/959), [a535d9c](https://github.com/cubrid-lab/pycubrid/commit/a535d9cd5c9438289e03635532c6f7bf1afb14df)). Contributed by @yeongseon.

- **A `Lob` is not sent on a replaced physical session (#932)** — after `ping(reconnect=True)` or a failed out-of-transaction `CHECK_CAS` probe replaced the session, `Lob.read()` and `Lob.write()` sent the old session's handle to the new CAS. They now raise `InterfaceError` before any request is sent, without reconnecting or retrying; create the LOB again or fetch its row again. A `Lob` created or wrapped on the current session, partial-write handling, async connections (no LOB support) and `compat.native` lobs are unchanged ([#934](https://github.com/cubrid-lab/pycubrid/issues/934), [03d7f70](https://github.com/cubrid-lab/pycubrid/commit/03d7f70871ca28c00dd3627f76bfbc77fbf06b0e)). Contributed by @yeongseon.

- **Corrupted packed LOB handles in a reply are rejected (#933)** — a fetched BLOB/CLOB cell whose packed handle had a negative locator size, a locator shorter than the rest of the cell, or an embedded type other than the cell's server DB_TYPE (BLOB `33`, CLOB `34`) was accepted and returned with a wrong or shortened locator. These now raise `ValueError` (as a header shorter than 16 bytes already did through `struct.error`), which the connection reports as `OperationalError('malformed response from broker')` and closes, as for other framing damage (#383), even when an earlier cell of the reply holds a value that would raise `DataError`. The rules follow the CUBRID 10.2 and 11.4 broker source (`add_res_data_lob_handle`) and live handles from both versions. Valid handles, a value without an external file (size `-1`, empty locator) and bytes after the cell are unchanged. Contributed by @yeongseon.

### Documentation

- **A release review checklist (#960)** — `RELEASING.md` now has a checklist to complete before merging a release PR: the candidate's base and final head, each required check's state at that head (only a successful check counts), the review evidence, the impact classification under `RELEASE_POLICY.md`, and whether there is a user-facing reason to publish (a candidate may stay pending; a documentation-only release stays possible). A release counts as complete when the publisher summaries show publication and post-publication verification for the same tag, SHA and version (`published and verified`, or a published run followed by a successful `verify-only` recovery). The step on starting CI after the final bot update now describes what happened for candidate #931 (runs waiting as `action_required`, no check runs). `CONTRIBUTING.md`, its Korean page and `AGENTS.md` link to the checklist; release workflows and gates are unchanged ([#968](https://github.com/cubrid-lab/pycubrid/issues/968), [4d5d84c](https://github.com/cubrid-lab/pycubrid/commit/4d5d84ccca773ec56090d30ada19364627ece29f)). Contributed by @yeongseon.

- **The documentation site says it is built from `main` (#960)** — every page, English and Korean, now opens with a notice that it is development documentation for the `main` branch at a linked commit, that it can describe unpublished changes, and that a published version's documentation is the `docs/` folder at its tag. The label never comes from `pycubrid.__version__`, the release manifest or a tag name, and the site shows no hardcoded latest version. The quickstart says what its examples were tested with (pycubrid 1.13.0 and CUBRID 11.4, with dates); the support matrix and `llms.txt` say that `__version__` on `main` is not proof of publication. The deploy workflow is unchanged ([#967](https://github.com/cubrid-lab/pycubrid/issues/967), [36f2532](https://github.com/cubrid-lab/pycubrid/commit/36f253215dd71468212aceb0afcf1d2e52c8bc6e), refs [#960](https://github.com/cubrid-lab/pycubrid/issues/960)). Contributed by @yeongseon.

- **Removing the Docker lab also removes its volume (#964)** — the `cubrid/cubrid` images declare a `VOLUME` for the database directory, so `docker rm -f pycubrid-quickstart` left an anonymous volume holding the tutorial's `testdb` behind after every lab. The quickstart and Finish Safely lessons (English and Korean) now use `docker rm -f -v pycubrid-quickstart`, and Finish Safely explains that `-v` deletes that volume and that without it the volume stays (`docker volume ls`). The lab image, port, database and tutorial programs are unchanged ([#966](https://github.com/cubrid-lab/pycubrid/issues/966), [4efe130](https://github.com/cubrid-lab/pycubrid/commit/4efe1307031b43a300bbb7fd675292e14cedc933), refs [#964](https://github.com/cubrid-lab/pycubrid/issues/964)). Contributed by @yeongseon.

- **Focused beginner lessons and a reader-based navigation (#958)** — the quickstart's steps are split into lessons (the navigation lists lessons 1 to 5), in English and Korean, each with prerequisites, program, command, expected output and next step: `docs/first-query.md` (the first query and its expected `(2,)`, and the default absence of a connect deadline with a link to `connect_timeout`), `docs/save-a-row.md` (insert with `?` parameters, commit, read from a new connection), `docs/commit-and-rollback.md` (an uncommitted row, then `rollback()`) and `docs/finish-safely.md` (owner- and comment-checked cleanup, removing the Docker lab, and what to do after an interrupted or repeated run). The programs now save and roll back in separate files (`first_saved_row.py`, `first_rollback.py`); the cleanup program is unchanged. The quickstart stays the canonical start and every old heading and anchor still resolves. The site navigation is grouped by reader (Start here, Usage, Reference, Help, Operations, Development and contributing), every page shows previous and next links, and `docs/DEVELOPMENT.md` documents the lesson convention and a page-role map ([#963](https://github.com/cubrid-lab/pycubrid/issues/963), [aca7dc2](https://github.com/cubrid-lab/pycubrid/commit/aca7dc20ee96cab2e5454945d6a255cb32e15720), [#965](https://github.com/cubrid-lab/pycubrid/issues/965), [5d23755](https://github.com/cubrid-lab/pycubrid/commit/5d237551c56f4f1f756c027d5c24021c83f1d29e), refs [#958](https://github.com/cubrid-lab/pycubrid/issues/958)). Contributed by @yeongseon.

- **A first-connection path for CUBRID beginners (#955)** — `docs/quickstart.md` ("Your First CUBRID Connection") now explains that pycubrid is a connector to a separately running CUBRID server, then offers two prerequisites: the five values to collect for an existing database, or a disposable CUBRID 11.4 lab in Docker with a readiness check and removal command. It continues with a virtual environment, `first_query.py` (prints `(2,)`) and `first_saved_row.py`, which creates its own marked table, commits one row, rolls one back and reads the saved row from a second connection; `cleanup_quickstart.py` drops only a table that carries the tutorial's comment. Repeat and interrupted runs and the observed first-connection errors (wrong interpreter, unreachable broker, wrong database, unknown user, wrong password) are documented. The README is reordered into Start here, Usage, Reference and Development, keeping the old heading text and anchors, and the Python 3.10 note moves to Compatibility. The Korean pages match ([#956](https://github.com/cubrid-lab/pycubrid/issues/956), [bc4bffd](https://github.com/cubrid-lab/pycubrid/commit/bc4bffdf40f3d829b300932145ce7790f9fb5b1f)). Contributed by @yeongseon.

- **Troubleshooting clarifies sync and async read timeouts (#957)** — the English and Korean troubleshooting pages clarify the read timeouts of the sync and async connections ([#957](https://github.com/cubrid-lab/pycubrid/issues/957), [c324703](https://github.com/cubrid-lab/pycubrid/commit/c324703e05d2162cf27ba40a2ddbb5b83a6c61cb)) by @petrovicistefan (first contribution 🎉).

- **A block-quoted issue number no longer renders as a heading on the site (#941)** — in `docs/API_REFERENCE.md`, a block quote line began with `#492`, which the MkDocs site rendered as an `<h1>`. It is now escaped as `\#492`; GitHub shows the same text. Contributed by @yeongseon.

- **Wrapped issue numbers no longer render as headings on the site (#938)** — 33 paragraph lines under `docs/` began with an issue number (for example `#750 gives each ...`), which Python-Markdown, unlike GitHub, renders as an `<h1>`. Their leading `#` is now escaped as `\#`, with wording and line breaks unchanged, and `scripts/check_docs_links.py` reports such a line under `docs/` from now on ([#942](https://github.com/cubrid-lab/pycubrid/issues/942), [52c4ed4](https://github.com/cubrid-lab/pycubrid/commit/52c4ed435f1d508fcc569de1b2178f9926cdaa3b), refs [#938](https://github.com/cubrid-lab/pycubrid/issues/938)). Contributed by @yeongseon.

- **A maintainer's review is the standalone-agent alternative to Copilot (#943)** — `AGENTS.md` and `CONTRIBUTING.md` now say that a standalone agent PR waits for a substantive submitted GitHub Copilot review or a maintainer's review, recorded on the PR, instead of "another person's" review (maintainer decision, 2026-10-15). A reviewer agent run by the implementer does not count. The Korean contributor guide gains the review-route paragraph it lacked ([#944](https://github.com/cubrid-lab/pycubrid/issues/944), [b99f02d](https://github.com/cubrid-lab/pycubrid/commit/b99f02d410cf0fcc8d946d08a4acf3d17e210b83), refs [#943](https://github.com/cubrid-lab/pycubrid/issues/943)). Contributed by @yeongseon.

- **Good-first-issue access is clarified for human contributors** — `CONTRIBUTING.md`, `AGENTS.md`, the issue forms, the PR template and the Korean contributor guide say how human contributors may take a good first issue ([#929](https://github.com/cubrid-lab/pycubrid/issues/929), [5b0bc2a](https://github.com/cubrid-lab/pycubrid/commit/5b0bc2a7672b719e000286e86bcf4fa77a7a2416)) by @yeongseon.

- **Agent guidance split into task guides (#926)** — the root `AGENTS.md` (733 lines, 45,359 bytes) keeps the project overview, commands, code conventions and every review, release, issue, label, documentation and commit rule, and gains a task routing table. The architecture map, wire protocol summary, CI and test layout, documentation map and performance pointers moved unchanged to `docs/agents/`, leaving the root file at 402 lines and 25,807 bytes. `scripts/check_agents_docs.py` checks the links, anchors, guide reachability, `make` targets and script paths of these files, and with `--retained-from REF` that no line of an earlier `AGENTS.md` was dropped. No code, CI or release behavior changes. Contributed by @yeongseon.

- **Abandoned release without a tag** — RELEASING.md's abandoned-version recovery now says to set the top-level `last-release-sha` in `release-please-config.json` to the abandoned merge SHA before preparing the next release; otherwise release-please 17.6.0 falls back to `bootstrap-sha` and lists every commit since then again. It also says to fold the unpublished section under `autorelease: review` and to remove the override after the next release is tagged. sqlalchemy-cubrid documents the same recovery (cubrid-lab/sqlalchemy-cubrid#912) ([#930](https://github.com/cubrid-lab/pycubrid/issues/930), [2e97ee9](https://github.com/cubrid-lab/pycubrid/commit/2e97ee94357252084e80abdc7b8535c5ecf37eb6)) by @yeongseon.

### CI

- **The CUBRID 11.4 Docker test database is kept in a named volume (#1002)** — `cubrid/cubrid:11.4` keeps its databases in `/home/cubrid/CUBRID/databases`, but `docker-compose.yml` mounted its named volume only at `/var/lib/cubrid`, where images up to 11.3 keep theirs. With `CUBRID_VERSION=11.4` the database lived in an anonymous volume, so `docker compose down` (also run at the end of every `make integration`) lost it, and each run left a dangling volume. A second named volume, `cubrid-databases`, now holds that path; on CUBRID 10.2, 11.2 and 11.4 a table created before `down` is still there after `up`, `make docker-fix-db-host` works on 11.4, and `docker compose down -v` leaves no volume behind. Versions up to 11.3 still share `cubrid-data`; `docs/DEVELOPMENT.md` says to run `make docker-down` before switching `CUBRID_VERSION`, since opening a database created by another CUBRID version is not verified. An 11.4 database kept in an earlier anonymous volume is not moved; it was already lost on the next `down` and `up` ([#1004](https://github.com/cubrid-lab/pycubrid/pull/1004), [ec2d9ed](https://github.com/cubrid-lab/pycubrid/commit/ec2d9ed1720519b9f3c058cacaf730fbc9ad0195)). Contributed by @yeongseon.

- **The Docker test database keeps working after its container is recreated (#1000)** — the image registers the database in `databases.txt` (kept on the `cubrid-data` volume by 10.2 and 11.2) under `CUBRID_DB_HOST` or, when that is unset, the creating container's ID. `docker-compose.yml` did not set it, so after `docker compose down` (also run at the end of every `make integration`) a new container could not resolve the recorded host: every connection failed with -353 and the next `make integration` retried 36 times before giving up. `docker-compose.yml` now sets `CUBRID_DB_HOST: localhost` (checked on CUBRID 10.2 and 11.2 across `down` and `up`, and with two full `make integration` runs on a repaired 11.2 volume; 11.4 keeps its databases outside that volume, see #1002 above). A volume created earlier keeps the old ID: `make docker-fix-db-host` rewrites only `testdb`'s host field to `localhost`, keeps the data and, on its first run, saves the previous file as `databases.txt.before-fix-db-host` (a later run keeps that copy; [#1003](https://github.com/cubrid-lab/pycubrid/issues/1003), [4843d47](https://github.com/cubrid-lab/pycubrid/commit/4843d47ccfb9c3d9f299f58ab42e86658f849900), refs [#1000](https://github.com/cubrid-lab/pycubrid/issues/1000)), while `make docker-down` (`docker compose down -v`) deletes the volume and its data. `scripts/wait_for_cubrid.py` now stops at the first -353 whose hosts all fail `getent hosts` inside the container (`CUBRID_TEST_DOCKER_CONTAINER`, passed by `make integration` and `make integration-tls`), naming both fixes; otherwise the existing retries remain. CI starts CUBRID as a service, not with Compose, and the driver is unchanged ([#1001](https://github.com/cubrid-lab/pycubrid/pull/1001), [48cbd78](https://github.com/cubrid-lab/pycubrid/commit/48cbd78e9e7d6971867a3c8bc3e9a23a7f2d2ba2)). Contributed by @yeongseon.

- **The agent-evaluation runner no longer follows links and measures the attempt itself (#985)** — the opt-in contributor tooling now creates its start record with `O_CREAT|O_EXCL|O_NOFOLLOW` and mode `0600` (a stale or planted entry is removed, never its target), reads it only as a regular file of at most 4 KiB, otherwise falls back to `attempt.json`, labelled as such, and keeps its state directory at exactly `0700`, refusing a symlink or another user's directory. `collect` copies the held checkout by directory descriptor without following links (a symlink is copied as a link), so a path swapped during the copy cannot pull outside content into `candidate/patch.diff`. The start time is also recorded outside `OUT_DIR` and preferred over the attempt-writable `attempt.json`, so a forged or non-numeric `prepared_epoch` can no longer hide `over_max_minutes`; without that record, a valid `attempt.json` value is used and labelled, and an invalid one makes the duration and `over_max_minutes` `"unknown"`. `run.json` gains `duration_source`. The driver is unchanged ([#986](https://github.com/cubrid-lab/pycubrid/issues/986), [81f327d](https://github.com/cubrid-lab/pycubrid/commit/81f327d683f868ac53af6103fe65e82f7eef29b9), [#987](https://github.com/cubrid-lab/pycubrid/issues/987), [14ebed1](https://github.com/cubrid-lab/pycubrid/commit/14ebed12a1137ea1949286bea724e2446c9e6bd2)). Contributed by @yeongseon.

- **Every repository-tooling test module and its inputs select the tooling lane (#983)** — a pull request that changed only one of 25 repository-tooling test modules, or a file only they read, ran none of the tests that check it, because the `tooling` paths filter in `ci.yml` did not list them. The filter now lists every such module and the files only they read (`agent-evals/**`, the four tutorial pages the `lab-volume-964` acceptance script reads, `THIRD_PARTY_LICENSES.md`, `release-please-config.json`, `api-baseline.json`). A guard in `tests/test_workflow_path_impact.py`, also run by the always-on lint job, fails when a module with `repo_tooling` tests is not selected. Which tests each lane runs is unchanged. Contributed by @yeongseon.

- **An offline grader for agent task evaluations (#925)** — `scripts/agent_eval.py` grades what a coding agent produced for a pycubrid task (a unified diff, the agent's report, and a runner's record of the commands executed on the base and patched trees) against a versioned task manifest in `agent-evals/`. It checks scope and size limits, that each named regression test is added by the patch and fails on the base but passes on the patch, required documentation, SQL built by string formatting in added Python lines, and that every validation claim is backed by evidence. It runs no model, test or network request, and its grade is deterministic JSON with no single score. `make agent-eval-check` self-tests it on seven hand-written candidates. Ordinary CI and the driver's tests are unchanged. Contributed by @yeongseon.

- **Three agent evaluation tasks from earlier pycubrid fixes (#925)** — `agent-evals/tasks/` adds `reply-bounds-383` (a malformed reply length must close the session without reading past the frame; #383), `lob-generation-932` (a LOB handle of a replaced physical session must fail before anything is sent; #932) and `lab-volume-964` (a tutorial documentation fix whose macOS check cannot run on Linux; #964). Each manifest links its issue, pull request and commit and ships the merged fix as `reference.diff`. The manifest format gains `[validation] manual` (checks only a person can do, which an attempt must list as not run), `[validation] acceptance` (scripts that must exit 1 on the base and 0 on the patch) and `[expected] reference_fix`; `grade --output FILE` writes the grade to a file and prints only the verdict. Ordinary CI and the driver are unchanged. Contributed by @yeongseon.

- **An opt-in runner for agent evaluation attempts (#925)** — `scripts/agent_eval_run.py prepare` makes a fresh repository holding only a task's base commit (the reference fix and later history are absent) and a prompt without the expected behavior or evidence links. `collect TASK_DIR OUT_DIR` takes the attempt's uncommitted changes as the candidate patch, runs the validation commands, regression tests and acceptance scripts on the base and candidate trees, grades the attempt and writes `run.json` (model and tool versions, tokens and cost, duration, passing commands, out-of-scope edits, regression escapes, human review actions and a SHA-256 of every artifact). Each check runs with every capability dropped in new user, mount, PID and network namespaces, with only system directories read-only and the tree under test writable, so there is no network and the caller's home directory and credentials are not visible; where that sandbox cannot be built, `collect` refuses to run unless `--no-sandbox` is passed and recorded. The runner never commits, pushes or publishes, and refuses symlinked or pre-existing paths in `OUT_DIR`. `summarize` reports repeated attempts as distributions of verdicts and failed checks per task, never one success rate. It runs no model and is not part of ordinary CI; `make agent-eval-check` now also runs its tests, and `AGENTS.md` links the evaluation guide. Contributed by @yeongseon.

- **The documentation check reads three more `case` forms in shell commands (#977)** — `scripts/_markdown.py` took `case` inside arithmetic (`$((case + 1))`) for a keyword, never closed an empty `case x in esac`, and let `esac` after a pattern `|` (`x|esac)`) close the `case`; each dropped the `make` goals of the whole documented command. It now parses these as the cookbook copy does (cubrid-lab/cubrid-cookbook-python#404), along with three more forms found in review: `;&` and `;;&` ending a clause like `;;`, a word continued by a quote or backslash (`esac"x"`, `esac\*`) being a pattern rather than `esac`, and a `#` comment inside a substitution. The 8 cookbook regression cases (3 failed here before) and 13 new ones are added; the cookbook copy takes the same change next so the copies stay identical ([#978](https://github.com/cubrid-lab/pycubrid/issues/978), [0edf209](https://github.com/cubrid-lab/pycubrid/commit/0edf209c30456b6ff81457b0225d8d245576d237)). Contributed by @yeongseon.

- **The documentation check allocates site anchors as the site does (#975)** — the parser shared with sqlalchemy-cubrid and cubrid-cookbook-python now reads this repository's `mkdocs.yml` and uses the slugifier it configures (`pymdownx.slugs`, as before; Python-Markdown's default for a site that sets none), and allocates ids with the `toc` extension's rule: a heading with an empty slug gets `_1` instead of an empty id, and a taken `_N` id is incremented (`_1` then `_2`) instead of becoming `_N_1`. Ordinary headings keep their anchors. This is the change merged in cubrid-lab/cubrid-cookbook-python#402, with tests for accented and Korean headings, `mkdocs.yml` detection and the collision rules ([#976](https://github.com/cubrid-lab/pycubrid/issues/976), [a0f8260](https://github.com/cubrid-lab/pycubrid/commit/a0f826089d8275a881efffa59f7d77e43ba7664b), refs [#975](https://github.com/cubrid-lab/pycubrid/issues/975)). Contributed by @yeongseon.

- **A change to the site notice template runs its tests (#971)** — `tests/test_docs_channel_notice.py` is now a `repo_tooling` test, and the `tooling` path filter in `ci.yml` selects `docs/overrides/**`, `mkdocs.yml` and the test itself, so a pull request that changes only the template runs the rendering tests instead of just the strict site build. Contributed by @yeongseon.

- **The site notice template tests run in CI (#971)** — `tests/test_docs_channel_notice.py` skipped its six rendering tests in every CI lane because no lane that runs it installed Jinja2. The `test` dependency group now includes `jinja2>=3.1` and the test imports it directly, so a lane without it fails; `tests/test_workflow_pins.py` and `THIRD_PARTY_LICENSES.md` record the new dependency. No workflow changes. Contributed by @yeongseon.

- **Prepare release checks the `last-release-sha` baseline override (#969)** — the recovery for an abandoned version sets a temporary `last-release-sha` in `release-please-config.json`, but nothing enforced its removal, and a stale override makes a later recovery silently list commits that already shipped. `scripts/reconcile_release_labels.py`, which runs before release-please, now classifies the override as sqlalchemy-cubrid does (byte-identical, from cubrid-lab/sqlalchemy-cubrid#950): it is active only while it is the merge of a release PR on `main` with a recorded abandonment, no `autorelease: pending`/`tagged` label and no tag for its version; once a later release containing it passes the existing publication proof, it is obsolete and preparation stops with a request to remove it in a normal PR. A malformed, unrelated or unreadable override also stops preparation, and an API error is reported as unresolved rather than as absence. Nothing is edited automatically; `RELEASING.md` ("Baseline override lifecycle") and `AGENTS.md` describe the rule, and `main` has no override today. Contributed by @yeongseon.

- **The documentation check is in sync with sqlalchemy-cubrid's again (#953)** — `scripts/_markdown.py` and its tests take the six shell fixes from cubrid-lab/sqlalchemy-cubrid#935: a `case` pattern's `)` does not close a substitution, and `case`/`esac` are keywords only at the start of a command; a `$(...)` inside double quotes is checked, but not one inside single quotes; arithmetic `$((...))` runs no command except the substitutions nested in it; an unquoted newline in a substitution body separates commands; and text that is not shell names no goal ([#954](https://github.com/cubrid-lab/pycubrid/issues/954), [4495ffb](https://github.com/cubrid-lab/pycubrid/commit/4495ffbdf5bd4ffbe6e294f5d73b4ccf5a940855), refs [#953](https://github.com/cubrid-lab/pycubrid/issues/953)). Contributed by @yeongseon.

- **`agent-review` no longer counts empty reply-only Copilot records as review rounds (#948)** — a Copilot record with no body and no thread-opening comment, such as a reply to an existing thread, is now ignored like an error or quota notice instead of counting as a summary-only round, which could put a PR past the two-round budget and reject valid evidence. A record that opens a thread still counts. Ported unchanged from cubrid-lab/cubrid-mcp-server#349 ([#949](https://github.com/cubrid-lab/pycubrid/pull/949), [10fb72c](https://github.com/cubrid-lab/pycubrid/commit/10fb72c65a40e522cc1c2d4e287a3e815431bb29)). Contributed by @yeongseon.

- **The documentation check converges with sqlalchemy-cubrid's parser (#941)** — `scripts/_markdown.py` takes sqlalchemy-cubrid's reviewed parser (cubrid-lab/sqlalchemy-cubrid#922, cubrid-lab/sqlalchemy-cubrid#924, cubrid-lab/sqlalchemy-cubrid#928, cubrid-lab/sqlalchemy-cubrid#930), plus two shell fixes from this change's review that sqlalchemy-cubrid receives next. It now handles adjacent shell operators such as `;>` and `&>>`, shell comments only at the start of a word, multi-line quotes (an unclosed quote is prose), `$(...)`, `<(...)` and `>(...)` as part of their word with the body checked as its own command, link source tails and the inline HTML or autolinks markdown-it parsed, and follows Python-Markdown's block quote and list rules for the `#<digit>` line check, which now also reports such lines in list items and block quotes ([#947](https://github.com/cubrid-lab/pycubrid/issues/947), [ebb3170](https://github.com/cubrid-lab/pycubrid/commit/ebb3170b582ac5f59ac80ef3d655c3aba3b7bba9)). Contributed by @yeongseon.

- **Advisory `agent-review` check for review evidence (#924)** — `.github/workflows/agent-review.yml` runs `scripts/agent_review_gate.py` from the base branch with a read-only token on every pull request and review event, and reports whether GitHub's own review records meet the review route of 2026-10-15: a substantive Copilot review within the two-round AI review budget, a maintainer's `APPROVED` review, or a `COMMENTED` review that opens an inline thread. Changes to the literal-binding, prepared-bind, protocol, packet, connection/TLS and `compat` modules, every workflow, `release-please-config.json`, the release and lane-audit scripts and the gate itself need a maintainer's `APPROVED` review. Pending requests, PR-body checkboxes, summary-only bot text, other people's reviews and stale reviews never count; unresolved critical or security threads and missing API data fail the check. release-please PRs that change only release-please files, and Dependabot PRs, are exempt. The gate is a copy of the cubrid-mcp-server gate (Refs cubrid-lab/cubrid-mcp-server#341). The check is advisory: a pull request can publish a check with the same name, so it must not be made a required check until it is posted from an identity a PR cannot impersonate (#945). `CONTRIBUTING.md`, `AGENTS.md`, the PR template and `docs/CONTRIBUTING.ko.md` document it. Contributed by @yeongseon.

- **The documentation check closes its parser gaps (#939)** — `scripts/_markdown.py` now matches sqlalchemy-cubrid's reviewed parser (cubrid-lab/sqlalchemy-cubrid#919), plus the four cases of cubrid-lab/sqlalchemy-cubrid#921: HTML `href`/`src` targets are read with the standard HTML parser, fence closure is exact, brackets in link destinations, titles and HTML comments are not references, `make` goals come from a quote-aware shell lexer, protocol-relative URLs are external, grouped Makefile rules declare every target, and findings after multi-line HTML tags and link titles keep their exact line. The quickstart video's site-relative `<source src>` is marked as a reviewed exception ([#940](https://github.com/cubrid-lab/pycubrid/issues/940), [703a1c2](https://github.com/cubrid-lab/pycubrid/commit/703a1c29ba4f76996a2574ade6a9fb6c89124203)). Contributed by @yeongseon.

- **Deterministic documentation defects block the merge (#927)** — a new `docs-check` job in `ci.yml` runs `scripts/check_docs_links.py` and `scripts/check_agents_docs.py` on every tracked Markdown file when Markdown, `scripts/` or the `Makefile` change, and the `CI Gate` requires it then. It fails with `file:line` and a suggested fix on a missing, absolute or out-of-repository relative link target; an anchor that is not a heading or `<a id>` of its target; an undefined reference link or unclosed code fence; and a `make` goal or `scripts/` path in a command that does not exist. Release history keeps its old commands. Markdown is parsed as CommonMark with `markdown-it-py` (pinned in the `docs` group) by both scripts, and external links are never fetched. The shared `doc-lint` workflow stays advisory; its findings are recorded in `docs/CI_POLICY.md`. `<!-- docs-check: ignore -->` marks a reviewed false positive. Contributed by @yeongseon.

## [1.13.0] - 2026-10-14

This release fixes a bug where a negative number bound right after a minus sign could turn the rest of a statement into a comment and update every row (see Security), reports CUBRID error codes by their real meaning, and adds `DatabaseError.error_source`. **Authentication failures are now `OperationalError` with SQLSTATE `28000`, and error `-4` is now an interrupted statement (`HY008`)**, so code that matches exact exception classes or SQLSTATEs, or retries every operational error as transient, should read the Upgrade notes. `fetchmany()` with a size of 0 or less now emits a `DeprecationWarning`.

### Upgrade notes

- **Authentication failures are `OperationalError` with SQLSTATE `28000` (#903)** — a wrong
  password (`-171`) or an unknown user (`-165`) used to raise plain `DatabaseError` with
  `HY000`. `except DatabaseError` still catches them, because `OperationalError` is a subclass.
  Code that tested `type(exc) is DatabaseError` or `sqlstate == "HY000"` for these codes, or that
  retries every `OperationalError` from `connect()` as a transient failure, should check `errno`
  (unchanged) or `sqlstate == "28000"`. Contributed by @yeongseon.

- **Native error code meanings now follow CUBRID `error_code.h` (#895)** — errno `-4` is now an
  interrupted statement with SQLSTATE `HY008` and description `'Interrupted'`, no longer `08S01`
  and `'Communication error'`. The connection stays open, but the server may have aborted the
  interrupted transaction (after `cubrid killtran` the next statement reports `-111`), so roll
  back before reusing it. Codes `-5` to `-12`, `-394`, `-21001` and `-21003` no longer carry a
  description and raise `DatabaseError` with `HY000`; before, `-6`, `-11` and `-12` were
  `OperationalError`, `-7` and `-8` `DataError` and `-394` `ProgrammingError`, each for a
  client-side meaning the server never sends under that number. `-14` (`ER_IO_WRITE`) is now
  `OperationalError`. See Fixed. Contributed by @yeongseon.

### Added

- **`DatabaseError.error_source` tells CAS errors from server errors (#903)** — a new
  read-only attribute, sync and async, that survives `pickle` and `copy`. It is `"cas"` when the
  broker reported the error with `CAS_ERROR_INDICATOR` (the CAS itself, for example
  `get_schema_info()` with an invalid schema type), `"server"` with `DBMS_ERROR_INDICATOR` (the
  database server, including every failed batch row), and `None` for errors the driver raises
  itself. Error numbers are unchanged, and CAS numbering (`-1000` to `-1200`) overlaps server
  codes, so `error_source` is the way to tell them apart. `-165` (`ER_AU_INVALID_USER`) and
  `-171` (`ER_AU_INVALID_PASSWORD`) are now described as "Invalid user" and "Invalid password"
  and raise `OperationalError` with SQLSTATE `28000` (see Upgrade notes)
  ([#915](https://github.com/cubrid-lab/pycubrid/issues/915),
  [d9435cc](https://github.com/cubrid-lab/pycubrid/commit/d9435cca61fa3bd55c03c47240fdf2fd3a1d9829),
  refs [#903](https://github.com/cubrid-lab/pycubrid/issues/903)) by @yeongseon.

### Changed

- **Internal: prepared bind types and encoders moved out of `protocol.py` (#856)** — refactor
  with no behavior change. The prepared-binding types and encoders behind
  `executemany(..., prepared=True)` and `compat.native` `bind_param()`/`bind_set()` (24 names)
  now live in the private `pycubrid/_prepared_binding.py`. `pycubrid.protocol` re-exports every
  one of them as the same object, so existing imports and `isinstance()` checks are unchanged
  (only `__module__` differs), and a 10,000-row mixed-type `EXECUTE_ARRAY` request is
  byte-identical before and after. CI, mutation testing, CODEOWNERS and the architecture docs
  cover the new module. Contributed by @yeongseon.

### Deprecated

- **`fetchmany(size)` with a size of 0 or less (#678)** — sync `Cursor` and async `AsyncCursor`
  `fetchmany()` now emit a `DeprecationWarning` naming the calling line when an explicit integer
  `size` is 0 or negative. The result is unchanged in 1.x: `[]`, without consuming rows or
  sending a FETCH. pycubrid 2.0 will raise `ProgrammingError("size must be a positive
  integer")`, the rule the `arraysize` setter already applies. Omitted/`None` sizes and positive
  integers do not warn, and the `pycubrid.compat` wrapper keeps returning `[]` for `size <= 0`
  without a warning, like the official CUBRIDdb driver. Replacement, which works on 1.x and 2.0:
  skip the call when the size can reach 0, for example
  `rows = cur.fetchmany(remaining) if remaining > 0 else []` (on an `AsyncCursor`:
  `rows = await cur.fetchmany(remaining) if remaining > 0 else []`; the async warning message
  suggests this awaited form). See [API reference](https://github.com/cubrid-lab/pycubrid/blob/v1.13.0/docs/API_REFERENCE.md#fetchmanysize)
  ([#876](https://github.com/cubrid-lab/pycubrid/issues/876),
  [fb30d0a](https://github.com/cubrid-lab/pycubrid/commit/fb30d0a6c3eeaa9d29e4409209b156f23ec7d081),
  [#879](https://github.com/cubrid-lab/pycubrid/issues/879),
  [0f507ca](https://github.com/cubrid-lab/pycubrid/commit/0f507cab93a080c3906703883ebfd9305de85eee),
  refs [#678](https://github.com/cubrid-lab/pycubrid/issues/678)) by @yeongseon.

### Fixed

- **An async batch cancelled while its rows are encoded sends nothing and keeps the session
  (#882)** — `AsyncCursor.executemany()` with `prepared=True`, and the default literal batch of
  an `INSERT`, `UPDATE`, `DELETE` or `MERGE`, encode every row without yielding, so a
  cancellation requested meanwhile was raised only after the first request was written and the
  session was retired. Both paths now yield once after encoding, so the cancellation is raised
  before any request and the session is kept. The event-loop hold itself is unchanged; it is
  now measured and documented (about 7-10 µs per simple three-column row on CUBRID 11.4; see
  `docs/PERFORMANCE.md`), with the cancellation semantics after `PREPARE` and `EXECUTE_ARRAY`.
  Contributed by @yeongseon.

- **`TimingStats` counts executes and fetches consistently and as documented (#899)** — the
  escape-mode probe that `connect()` runs on a new session was counted as an application
  execute, so "connect + one `execute()`" reported `execute_count == 2`; it is no longer
  counted. One successful `executemany()` call now counts as one execute in both drivers: the
  `EXECUTE_BATCH` and `prepared=True` paths recorded nothing, and the per-row loop counted every
  row. `fetch_count` is unchanged and now documented as the number of server `FETCH` requests,
  so the `SELECT 1` + `fetchall()` example reports `fetch=0`
  ([#908](https://github.com/cubrid-lab/pycubrid/issues/908),
  [46012b3](https://github.com/cubrid-lab/pycubrid/commit/46012b325e6e4cacade5fed5dc278ab6835697a0))
  by @yeongseon.

- **Sub-minute UTC offsets, verbs followed by a comment and `Lob.write()` input (#898)** —
  three input-handling gaps, fixed in both drivers where they apply
  ([#907](https://github.com/cubrid-lab/pycubrid/issues/907),
  [5ce7ae7](https://github.com/cubrid-lab/pycubrid/commit/5ce7ae7ebea0d4749ebf23529cd7dfdb5507e0d0)):
  - An aware `datetime` parameter whose UTC offset is not a whole number of minutes (for
    example `+05:30:45`) now raises `DataError` in the literal and prepared paths. Before, the
    offset was truncated to `+05:30` and the server stored an instant 45 seconds off.
  - A statement whose verb is followed directly by a comment, such as `INSERT/*x*/INTO t ...`,
    is now detected as `INSERT`. `executemany(..., prepared=True)` raised `NotSupportedError`
    for it, and the default `executemany()` ran one request per row instead of one
    `EXECUTE_BATCH`.
  - `Lob.write()` accepts only `bytes`, `bytearray` and `memoryview`; anything else, such as a
    `str`, raises `InterfaceError` instead of a raw `TypeError` or a request. Every empty value
    returns `0` without broker I/O, as the docstring said, and a multi-byte `memoryview` writes
    and reports its byte count instead of its item count.

  by @yeongseon.

- **Malformed FC20 and FC40 replies are reported as malformed (#897)** — an `EXECUTE_BATCH`
  reply whose row error has a negative message length was read as an empty message, and a
  `GET_LAST_INSERT_ID` reply whose two-byte type header did not fit its one-byte value read the
  second byte past the value and returned `''`. Both now raise
  `OperationalError('malformed response from broker')` and close the session, as the
  `EXECUTE_ARRAY` and row-cell parsers already did
  ([#906](https://github.com/cubrid-lab/pycubrid/issues/906),
  [25a0708](https://github.com/cubrid-lab/pycubrid/commit/25a070838004584974ce6456db132ce9a262e2e6))
  by @yeongseon.

- **Undecodable JSON values raise `DataError` and keep the session (#900)** — with
  `json_deserializer=json.loads`, complete JSON text that `json.loads` cannot decode now raises
  `DataError: JSON value cannot be decoded` with the original error as its cause: an integer
  longer than Python's integer string conversion limit (before, a `ValueError` reported as a
  malformed reply that closed the session) or nesting deeper than the recursion limit (before, a
  raw `RecursionError`). Invalid JSON text still raises `DataError: JSON value is not valid JSON`
  (#543). On CUBRID 11.4 the deep-nesting case was reproduced live (20,000 levels); the
  5,000-digit integer is rejected at insert, so it was reproduced offline only. The
  `CUBRIDDataType.NULL` alias of `UNKNOWN` is now documented. Contributed by @yeongseon.

- **`ping(reconnect=True)` and broker redirects fail the same way in both drivers (#896)** — a
  sync `Connection.ping(reconnect=True)` whose reconnect the server rejected at `OPEN_DATABASE`,
  for example a wrong password (`-171`), raised `DatabaseError` instead of returning `False`; it
  now returns `False`, as the async driver already did. A broker redirect to a port above
  `65535` now raises `OperationalError` in both drivers before any reconnect. Before, the async
  driver let a raw `OverflowError` escape for a numeric host, and the sync driver's resolver
  reduced the port to 16 bits and tried that other port
  ([#905](https://github.com/cubrid-lab/pycubrid/issues/905),
  [4562360](https://github.com/cubrid-lab/pycubrid/commit/4562360f3b80a1df5eb3b975f07825e87ce7f9e4),
  refs [#896](https://github.com/cubrid-lab/pycubrid/issues/896)) by @yeongseon.

- **Async requests refused during another task's session setup keep that session (#894)** — an
  `AsyncConnection` request that slipped past the setup wait while `ping(reconnect=True)` or
  `connect()` was running its escape-mode probe still raised "connection setup in progress;
  retry operation". But `get_schema_info()` then dropped the new session, and `commit()`,
  `rollback()` and cursor statements treated the refused `CHECK_CAS` as a dead CAS and replaced
  it, so the probe failed and `ping()` returned `False`. These calls now raise the same retryable
  `OperationalError` before any cleanup or recovery, sending nothing. A schema result from the
  replaced session keeps its retired outcome (`fetch_schema_info()` raises `InterfaceError`,
  `close_schema_info()` is a no-op); the setup task's own requests are unchanged
  ([#902](https://github.com/cubrid-lab/pycubrid/issues/902),
  [ffa0069](https://github.com/cubrid-lab/pycubrid/commit/ffa006989c234aedf7b4f07f7d4b529342e7cb6f))
  by @yeongseon.

- **CAS error `-4` is an interrupted statement, not a communication error (#895)** — the error
  tables in `pycubrid.error_codes` followed an old client-side CCI numbering for `-1` to `-15`
  with several other misnumbered codes, so an interrupted statement raised
  `OperationalError(description='Communication error', sqlstate='08S01')` and pools and ORMs
  discarded a healthy session. Every entry was checked against CUBRID `src/base/error_code.h`
  (release/10.2 and release/11.4) and now cites its `ER_*` constant
  ([#904](https://github.com/cubrid-lab/pycubrid/issues/904),
  [411bfd4](https://github.com/cubrid-lab/pycubrid/commit/411bfd4609998ce054c8b456a366d50954780976)):
  - `-4` (`ER_INTERRUPTED`): `'Interrupted'`, `OperationalError`, SQLSTATE `HY008`.
  - `-1` (`ER_FAILED`) is `'Generic failure'` and `-111`
    (`ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED`) is `'Transaction aborted by the server'`; their
    classes and SQLSTATEs are unchanged.
  - `-13`, `-14` and `-15` (`ER_IO_READ`, `ER_IO_WRITE`, `ER_IO_WRITE_OUT_OF_SPACE`) are I/O
    errors raised as `OperationalError` / `HY000`; before, they were described as shard, cursor
    position and statement pooling errors (and `-14` had SQLSTATE `24000`).
  - Removed `-5` to `-12` (server hash-table and volume errors the table described as client
    conditions), `-394` (`ER_TX_ENDPOINT_TOO_LARGE`, not "Column not found": an unknown column
    is `-494`) and `-21001`/`-21003` (JDBC client codes the broker never sends; a wrong password
    is `-171`). They are now `DatabaseError` / `HY000` without a description.
  - `pycubrid.constants.ErrorCode` uses the same server names: `ER_FAILED`, `ER_GENERIC_ERROR`,
    `ER_OUT_OF_VIRTUAL_MEMORY`, `ER_INTERRUPTED`, `ER_IO_READ`, `ER_IO_WRITE` and
    `ER_IO_WRITE_OUT_OF_SPACE` replace the old CCI-style names.

  The API reference and the troubleshooting and connection guides now show the real
  authentication errors. Contributed by @yeongseon.

### Security

- **A negative number bound after a minus sign no longer starts a comment (#893)** — the
  literal binding of sync and async `execute()` and of the default `executemany()` pasted a
  negative `int`, `float` or `Decimal` straight after the SQL text, so
  `UPDATE t SET x = x-? WHERE id = ?` with `(-5, 1)` was sent as `x--5 WHERE id = 1`. CUBRID
  reads `--` as a line comment, so the `WHERE` clause was dropped and every row was updated;
  anyone who controls a numeric parameter could truncate the statement this way. A space now
  separates a literal that starts with `-` from a `-` right before its placeholder (`x- -5`).
  Every other rendering, including negative values after any other character and collection
  elements, is byte-identical. The prepared paths (`executemany(..., prepared=True)` and
  `compat.native`) send typed values and were not affected. See
  [Parameter binding](https://github.com/cubrid-lab/pycubrid/blob/v1.13.0/docs/PARAMETER_BINDING.md#placeholder-tokenizer)
  ([#901](https://github.com/cubrid-lab/pycubrid/issues/901),
  [626e2f4](https://github.com/cubrid-lab/pycubrid/commit/626e2f4dcff36ee50f0fe39c7a31fe4779e738af))
  by @yeongseon.

### Documentation

- **The README and the documentation site show the CUBRID Lab repository icon (#919)** — the
  English README and its Korean, Chinese, Hindi, German and Russian translations open with the
  centered pycubrid icon, and the documentation site uses it as its header logo and favicon,
  all loaded from one immutable [cubrid-lab/brand](https://github.com/cubrid-lab/brand) commit
  ([#920](https://github.com/cubrid-lab/pycubrid/pull/920),
  [05f1db3](https://github.com/cubrid-lab/pycubrid/commit/05f1db339de618113ee6305f5988dd71c47cf7db),
  [#921](https://github.com/cubrid-lab/pycubrid/issues/921),
  [0719025](https://github.com/cubrid-lab/pycubrid/commit/0719025d48398a7b691c576c9501fa35ab06f13b))
  by @yeongseon.

- **`DEVELOPMENT.md` project structure and architecture diagrams match the repository (#878)**
  — the English and Korean guides now list the `aio` and `compat` packages, the private shared
  modules, `error_codes.py` and `timing.py`, describe `tests/` by its behavior-oriented modules
  instead of the removed `test_suite.py`, and no longer hard-code function-code, type or packet
  class counts. The agent guide's module map, test, docs and commit sections were refreshed too
  ([#880](https://github.com/cubrid-lab/pycubrid/issues/880),
  [f6a2734](https://github.com/cubrid-lab/pycubrid/commit/f6a2734655aca5506c4551f85387c50cc8fbec05),
  [#877](https://github.com/cubrid-lab/pycubrid/issues/877),
  [6e05ce0](https://github.com/cubrid-lab/pycubrid/commit/6e05ce0fce91e92dde2c9e249dcf5eba9e8cac1f))
  by @yeongseon.

- **The German, Hindi and Russian READMEs state the current Python requirement** — updated in
  [#870](https://github.com/cubrid-lab/pycubrid/issues/870) (German,
  [5a7638e](https://github.com/cubrid-lab/pycubrid/commit/5a7638e6634ef776632b57e4476e61713bb84a33),
  refs [#702](https://github.com/cubrid-lab/pycubrid/issues/702)),
  [#871](https://github.com/cubrid-lab/pycubrid/issues/871) (Hindi,
  [cc0b879](https://github.com/cubrid-lab/pycubrid/commit/cc0b8794099754d4d08779a7c843df13bed6bd97),
  refs [#703](https://github.com/cubrid-lab/pycubrid/issues/703)) and
  [#873](https://github.com/cubrid-lab/pycubrid/issues/873) (Russian,
  [4f81fa3](https://github.com/cubrid-lab/pycubrid/commit/4f81fa3561d1a167679b9fc151bdb1b4539854f6),
  refs [#704](https://github.com/cubrid-lab/pycubrid/issues/704)) by @yeongseon.

### Tests

- **The live microbenchmarks check that they act on real rows (#922)** — the benchmark table's
  `AUTO_INCREMENT` key is not reset by `DELETE` and nothing checked results, so
  `test_bench_single_select` had been timing a lookup of a missing row (reproduced on CUBRID
  11.4.6 and 10.2.18). The benchmarks now use explicit primary keys and fail on a missing or wrong
  row or affected-row count in every round. They connect through the shared test endpoint
  (`CUBRID_TEST_URL`/`CUBRID_TEST_HOST`) instead of `CUBRID_HOST`, `CUBRID_PORT` and `CUBRID_DB`,
  need pytest-benchmark 5.0 in the dev group, and were renamed (workload revision
  `micro-explicit-keys-v2`, with `test_bench_prepared_reuse` now
  `test_bench_execute_parameterized_loop_100`) so older results are not compared with them. The
  first reference run on CUBRID 11.4 and 10.2 is in `benchmarks/2026-10-14-micro-explicit-keys-v2`.
  Test harness only; no driver change. Contributed by @yeongseon.

- **The wide-row measurement reports batch time alone (#914)** —
  `scripts/measure_prepared_wide_rows.py` now starts the two async connections together, as the
  sync threads already were, and reports each connection's `executemany()` and commit times
  apart from the whole workload; the `docs/PERFORMANCE.md` tables were rerun. The async second
  batch still starts one batch's encoding time after the first because encoding does not yield
  (#882); memory conclusions are unchanged (two connections cost 1.5-2.0 times one). Script and
  docs only. Contributed by @yeongseon.

- **Wide-value and two-connection prepared batches measured (#881)** — a new script and opt-in
  `slow` integration test measure `executemany(..., prepared=True)` with wide text, bytes,
  50-element sequences and LOB handles, sync and async, on one and two connections. On CUBRID
  11.4 the client heap peak is about 3-4.6 times the request size for scalar values and about 9
  times for sequences; two concurrent connections cost the sum of two batches. 16-128 MiB single
  values are accepted, and a value too wide for its column fails only its row. No runtime change;
  results in `docs/PERFORMANCE.md`. Contributed by @yeongseon.

- **What `read_timeout` bounds when a reply arrives slowly, pinned and documented (#855)** — a
  new offline test sends a reply in ten pieces 0.1 s apart (0.9 s in all) with
  `read_timeout=0.3`. The sync driver, whose `read_timeout` bounds each socket send or receive
  call, completes it and keeps the session; the async driver, whose `read_timeout` bounds the
  whole request round trip, raises `OperationalError('read timeout: ...')` with the
  `asyncio.TimeoutError` as its cause, and completes the same reply when the bound is larger. A
  sync stall or async cancellation partway through a frame retires the session and its cursor and
  schema handles, keeps buffered rows readable and never resends the interrupted SQL;
  `ping(reconnect=True)` then gives a clean session, and cancellation still raises
  `asyncio.CancelledError`. `docs/CONNECTION.md` (and its Korean translation) now tabulates what
  `connect_timeout`, `read_timeout` and the TLS handshake bound on each driver and states that
  none is a wall-clock deadline for a statement or cancels it on the server;
  `docs/SUPPORT_MATRIX.md` and `docs/API_REFERENCE.md` say the same in brief. Tests and docs only.
  Contributed by @yeongseon.

- **Large `executemany(..., prepared=True)` batches measured and their failure states pinned
  (#854)** — new offline tests check, sync and async, that an `EXECUTE_ARRAY` frame over the
  32-bit `DATA_LENGTH` limit raises `DataError` before any byte is sent and keeps the session, and
  that a transport fault before `PREPARE`, after `PREPARE`, during a partial send or after a
  complete send raises `OperationalError`, retires the session and the cursor's handle, and never
  resends the batch. A send failure is the error's cause; a missing or cut reply has
  `asyncio.IncompleteReadError` as the cause on the async cursor and none on the sync cursor. An
  opt-in `slow` integration test measured 100,000 rows at 8.7-12.5 s with a 47.3 MiB heap peak,
  and 1,000,000 rows at about 123 s with 473.5 MiB, on CUBRID 10.2 and 11.4; every row was
  inserted. The results and the decision (no runtime change) are in `docs/PERFORMANCE.md`, and
  the command is in `docs/DEVELOPMENT.md`. Test, script and docs only. Contributed by @yeongseon.

## [1.12.0] - 2026-10-12

This release adds an **opt-in prepared batch** for synchronous and asynchronous `executemany(prepared=True)` using typed host variables and an EXECUTE_ARRAY request. Default literal batches remain unchanged; prepared mode has explicit statement, LOB/autocommit and error-handling limits detailed below.

### Added

- **Opt-in prepared batch for `executemany()` (#839)** — `Cursor.executemany()`
  and `AsyncCursor.executemany()` take a keyword-only `prepared=False`. With
  `prepared=True`, an INSERT, UPDATE, DELETE or MERGE is sent as one `PREPARE`
  and one `EXECUTE_ARRAY` request (function code 21) carrying every row's values
  as typed host variables, instead of N rendered SQL literals. For 10,000-row
  batches on CUBRID 10.2-11.4 it took 0.33-0.62x of the literal batch's time for
  INSERT, UPDATE and DELETE and 0.12-0.21x for MERGE
  ([PERFORMANCE.md](https://github.com/cubrid-lab/pycubrid/blob/v1.12.0/docs/PERFORMANCE.md#shipped-prepared-batch)). Every row is
  still executed and only a failing row is undone, autocommit still commits each
  successful row, the first failing row's error is raised with the same
  exception mapping (an interrupted batch raises the interruption), and
  `rowcount` is the sum of the per-row counts. Values use the #840 prepared
  types; one column may mix types from row to row; `Set`/`Multiset`/`Sequence`
  are sent as string-element collections (`Multiset` as SEQUENCE, because the
  broker rejects the MULTISET bind kind). Rows may come from any iterable. Every
  row is encoded before anything is sent, and errors name the position
  (`rows[i][j]`) but never the value; after `PREPARE`, a server statement type
  or marker count that does not match raises before any row runs. Other
  statements raise `NotSupportedError` instead of falling back. A
  `pycubrid.Lob` value is bound by its handle on the sync cursor with autocommit
  off; with autocommit on it raises `NotSupportedError` before any request,
  because each row's commit deletes the session's temporary LOB files and later
  rows would fail (-1016). The async cursor rejects `Lob` values. With
  autocommit on, the previous batch's handle is released on the next `PREPARE`,
  as on `PREPARE_AND_EXECUTE` (#488). The default `executemany()` is unchanged.
  See [API_REFERENCE.md](https://github.com/cubrid-lab/pycubrid/blob/v1.12.0/docs/API_REFERENCE.md) and
  [PARAMETER_BINDING.md](https://github.com/cubrid-lab/pycubrid/blob/v1.12.0/docs/PARAMETER_BINDING.md) ([#848](https://github.com/cubrid-lab/pycubrid/pull/848)) ([7f1747d](https://github.com/cubrid-lab/pycubrid/commit/7f1747d006bf5ac13d009ac191794d6c5cc066ca)) by @yeongseon.
- **Failed `executemany()` batch rows are reported (#841)** — when rows of a DML
  batch fail, the server still runs the other rows, but the driver raised only the
  first error and lost which rows failed. The raised exception keeps its class,
  message, `code`, `errno` and `sqlstate`, and now also has `batch_index` (the row it
  describes), `batch_errors` (`(index, error)` for every failed row, in order, each
  mapped like the raised error) and `batch_rowcounts` (one count per executed row,
  `-1` for a failed row). This applies to the literal batch, to
  `executemany(..., prepared=True)` (an interrupted prepared batch names the
  interrupted row) and to `executemany_batch()`, on the sync and async cursors.
  Every other `DatabaseError` has the three attributes set to `None`. `rowcount`
  stays `-1` after a partial failure; the applied rows can be counted from
  `batch_rowcounts`. `BatchExecutePacket` keeps the statement order in a new
  `row_results` and an `index` in each error, and rejects a reply whose executed
  count is negative or larger than the batch, that stops early without an
  interrupted (`ER_INTERRUPTED`) statement, or whose V3+ error row does not start
  with `DBMS_ERROR_INDICATOR` (-2), none of which CAS sends. See
  [API_REFERENCE.md](https://github.com/cubrid-lab/pycubrid/blob/v1.12.0/docs/API_REFERENCE.md#partial-failure-which-rows-failed) ([#852](https://github.com/cubrid-lab/pycubrid/pull/852)) ([b646cb6](https://github.com/cubrid-lab/pycubrid/commit/b646cb64873cf13c1eefc228559bb325dc549802)) by @yeongseon.
- **Wider typed prepared parameters (#840)** — `pycubrid.compat.native`
  `cursor.bind_param()` now sends `bool` (INT 1/0, the literal path's value),
  64-bit `int` (BIGINT), `int` past 64 bits up to 38 digits and `Decimal`
  (NUMERIC text), `float` (DOUBLE), `date`, `time` and naive `datetime` (DATE,
  TIME, DATETIME), aware `datetime` (DATETIMETZ with the literal path's zone
  text) and `bytes`/`bytearray` (VARBIT) as typed host variables. The checks
  match the literal path: `nan`/`inf` raise `ProgrammingError`, more than 38
  digits raise `DataError`, and forged subclasses are refused. An empty `bytes`
  value raises `NotSupportedError`, because the broker reads a zero-length bind
  as NULL. Existing INT32, string and NULL binds keep their exact bytes, and the
  ordinary cursors' literal binding is unchanged. Host-variable coercion differs
  from literal coercion in a few documented cases (a `float` is DOUBLE, not a
  NUMERIC literal; conversion failures are server error -494); see
  [Prepared host-variable mapping](https://github.com/cubrid-lab/pycubrid/blob/v1.12.0/docs/PARAMETER_BINDING.md#prepared-host-variable-mapping).
  Round trips against the literal path pass on CUBRID 10.2, 11.0, 11.2 and
  11.4. The encoder is shared so the planned opt-in `EXECUTE_ARRAY` batch path
  (#839) can reuse it. ([#846](https://github.com/cubrid-lab/pycubrid/pull/846)) ([29fd19c](https://github.com/cubrid-lab/pycubrid/commit/29fd19cafc3af665c2f25664bca6caceff03b922)) Contributed by @yeongseon.
- **`typing.Self` for context-manager and iterator returns (#689)** —
  `Connection.__enter__`, `Cursor.__enter__` / `__iter__`, `Lob.__enter__`,
  `AsyncConnection.__aenter__` and `AsyncCursor.__aenter__` / `__aiter__` are now
  annotated `-> Self` instead of the concrete class, so subclasses keep their own
  type in `with` / `async with` blocks and `for` / `async for` loops. A static
  contract module, `tests/typing_self_contract.py`, is part of `make typecheck` and
  fails mypy if any of these annotations regresses. Annotations only; no runtime
  change. ([#689](https://github.com/cubrid-lab/pycubrid/issues/689)) ([a441d70](https://github.com/cubrid-lab/pycubrid/commit/a441d7092ab254b47aa4ae052d15238e669861de)) Contributed by @Jah-yee.

### Changed

- **Placeholder tokenisation cached per `executemany()` call (#837)** — the
  literal batch path of `Cursor.executemany()` and `AsyncCursor.executemany()`
  splits the SQL on its placeholders once per call instead of once per parameter
  set, and an exact `tuple` or `list` parameter set skips the `typing.Sequence`
  check. The rendered SQL is byte-identical (pinned against SQL rendered before
  the change), and so are the errors and the row that raises them. For 10,000
  five-parameter rows, client-side rendering went from 145 ms to 38.5 ms
  offline and the client CPU inside a live `executemany()` from about 195 ms to
  52-98 ms; the live wall time is dominated by the server. See
  [PERFORMANCE.md](https://github.com/cubrid-lab/pycubrid/blob/v1.12.0/docs/PERFORMANCE.md#placeholder-tokenisation-cached-per-executemany-call) ([#853](https://github.com/cubrid-lab/pycubrid/pull/853)) ([98b8263](https://github.com/cubrid-lab/pycubrid/commit/98b8263ba8d17f022e232524c06590d80601b7da)) by @yeongseon.
- **CUBRIDdb wrapper integers past 32 bits (#840)** — the
  `pycubrid.compat.cubriddb` cursors pass `int` arguments to native
  `bind_param()`, so a value past signed 32-bit now binds BIGINT (or NUMERIC
  past 64 bits) instead of raising `DataError`. The wrapper still rejects
  `bool`, `float`, `bytes` and temporal scalars. Contributed by @yeongseon.

### Documentation

* **i18n:** update the Python requirement in the Chinese README ([#847](https://github.com/cubrid-lab/pycubrid/issues/847)) ([f0b09d2](https://github.com/cubrid-lab/pycubrid/commit/f0b09d240fadf13ec70787152b7a2a474880d349)) by @Vieta100-omo.

### CI

- **The closing guard skips a multi-line link reference definition** — a `Closes` keyword
  followed by a link reference definition whose destination or title continues on later
  lines (so nothing renders between the keyword and `owner/repo#N`) is now blocked; the
  change can only add blocking. Copilot found this in cubrid-lab/sqlalchemy-cubrid#822;
  `pr-title.yml` stays byte-identical across pycubrid, sqlalchemy-cubrid, cubrid-mcp-server
  and cubrid-cookbook-python. Contributed by @yeongseon.

- **Generated issue references are written as "refs", not "closes", in `CHANGELOG.md` and the
  release PR body** — release-please 17.6.0 labelled every trailing issue `closes`, so a
  commit that only said `Refs #N` was listed as closing that issue and GitHub could close it
  when the release PR merged. `scripts/compose_release_changelog.py` (shared with
  sqlalchemy-cubrid and cubrid-mcp-server; cubrid-lab/sqlalchemy-cubrid#820,
  cubrid-lab/sqlalchemy-cubrid#821, cubrid-lab/cubrid-mcp-server#280) and the **Prepare
  release** workflow now rewrite them to `refs`; no permission changes (#850) by @yeongseon.

- **The closing guard is the one shared with sqlalchemy-cubrid, cubrid-mcp-server
  and cubrid-cookbook-python** — `pr-title.yml` is the shared file (a test pins the guard
  block's SHA-256; the three sibling repositories take the same fix in their own pull
  requests). It now also catches a closing keyword used as link text, a quoted continuation
  line, an image or link reference definition between the keyword and the reference, a
  comment that ends at `--!>`, `<?` and `<![CDATA[`, raw-text elements such as `<select>`, a
  leading byte order mark, relative issue links, and a foreign issue listed after another
  one (`Closes #1, owner/repo#2`), while keeping this repository's title, commit-message,
  underscore-owner and 250-commit checks. Code spans, inline comments and fences or indented
  code inside list items or block quotes are now checked too, so some bodies the old guard
  accepted are blocked: the change trades missed closures for false positives. The 13
  changed regression expectations are in `test_uncertain_markdown_is_checked_on_purpose`.
  Contributed by @yeongseon.
- **Repository-local issue closure in PRs** — the required `PR title` check now
  rejects closing keywords that target a different GitHub repository in the PR body, the PR
  title and the PR's commit messages (the title and commit messages make up the squash
  commit); local `Closes #N` and cross-repository `Refs owner/repo#N` remain valid. Keywords
  split by emphasis, inline HTML, comments, entities or escapes are caught, as are
  `http://`, `www.`, autolink, `<a href>`, inline and reference-link issue URLs. A PR with 250 or more commits fails because GitHub
  lists only 250. Contributed by @yeongseon.

## [1.11.0] - 2026-10-11

This release adds Python 3.15 support (standard GIL builds only; `requires-python` stays `>=3.11`), lets the opt-in CUBRIDdb wrapper cursors accept collection arguments and `executemany()`, and fixes two correctness problems: `charset="ksc5601"` failing on Python 3.15, and the second occurrence of a repeated daylight-saving hour being stored one hour off. It also hardens password redaction in the contributor-only `scripts/collect_repro.py`. Decimal-subclass parameters need the C `_decimal` module (see the Python 3.15 entry). Contributors: the `dev` and `mutation` extras were replaced by dependency groups. The remaining entries are documentation, tests and CI.

### Added

- **Python 3.15 support (#831, cubrid-lab/sqlalchemy-cubrid#666)** — the package declares the Python 3.15 classifier, and the full compatibility matrix now runs Python 3.11–3.15 × CUBRID 10.2/11.0/11.2/11.4 (20 cells) on manual dispatch and every release. Standard GIL builds only; free-threaded builds are not supported. `requires-python` stays `>=3.11`. Decimal-subclass parameters need the C `_decimal` module; a Python built without `libmpdec` lacks it, and the `actions/setup-python` 3.15.0 build, the `python:3.15-rc` Docker image and a source build without `libmpdec-dev` were observed without it. See the support matrix ([#831](https://github.com/cubrid-lab/pycubrid/issues/831)) ([0563613](https://github.com/cubrid-lab/pycubrid/commit/056361386e3cb41af0558bd7cd974eb07d75a0a1)). Contributed by @yeongseon.

- **CUBRIDdb wrapper collection arguments and `executemany()` (#610)** — the opt-in `pycubrid.compat.cubriddb` cursors now accept the official wrapper's collection call shapes. `execute(query, args, set_type)` binds `list`, `tuple`, `set` and `frozenset` arguments through the native `set()`/`imports()`/`bind_set()` API. `set_type` is one code for every collection, a per-position list or tuple, or omitted for the official inference (`int`/`bool` INT, `float` FLOAT, `Decimal` NUMERIC, `date`/`datetime` DATE, `time` TIME, `str` STRING; empty collections STRING; mixed types `TypeError`). Elements are sent as their `str()` text with the SET kind, the same request as the official wrapper's for supported non-`None` elements. `executemany(query, args_list)` checks every group before one prepare and every group's value count before any execute, so an invalid group runs no group; it then executes each group and keeps the last `rowcount`. An empty list leaves `rowcount=-1` and `description=None`, pycubrid's own choice. Differences from the official wrapper: a `None` element binds a NULL element (the official wrapper sends the text `'None'`), BIT/VARBIT elements raise `NotSupportedError`, and other iterables raise `ProgrammingError`. Behavior changes for existing callers: argument errors now discard the previous result and set `rowcount=-1`, `description=None`; unsupported positional values fail before the prepare instead of at bind time; and a non-`None` `set_type` without a collection argument is now ignored instead of raising `NotSupportedError`. The scalar subset and ordinary cursors are unchanged. See [Wrapper collection arguments](https://github.com/cubrid-lab/pycubrid/blob/v1.11.0/docs/API_REFERENCE.md#wrapper-collection-arguments-610-wrapper-collection-arguments) ([#813](https://github.com/cubrid-lab/pycubrid/pull/813), [1accfaa](https://github.com/cubrid-lab/pycubrid/commit/1accfaa22945d3bd868f5940a68f085dae32a3ec)) by @yeongseon.

### Fixed

- **`charset="ksc5601"` works on Python 3.15** — Python 3.15 maps its own `ksc5601` codec alias to CP949, which pycubrid rejects because CP949 can emit ASCII bytes inside a multibyte character, so `charset="ksc5601"` raised `ValueError` on 3.15. pycubrid now maps `ksc5601` to EUC-KR itself, as on Python 3.11–3.14. Other aliases such as `ks_c_5601` and `korean` follow the running Python's codec table, so on 3.15 they resolve to CP949 and are rejected. Contributed by @yeongseon.

- **The second occurrence of a repeated DST hour binds as itself (#819)** — an aware `datetime` with a region `tzinfo` (`ZoneInfo`) in a repeated wall time was sent with the region alone, which CUBRID resolves to the first occurrence, so `datetime(2026, 11, 1, 1, 30, fold=1, tzinfo=ZoneInfo("America/New_York"))` (01:30 EST, 06:30 UTC) was stored as 01:30 EDT, one hour off, with no error. With `fold=1` in a repeated wall time, the literal now names the second occurrence by its abbreviation (`DATETIMETZ'2026-11-01 01:30:00.000 America/New_York EST'`), so it reads back with its region. A numeric abbreviation (Santiago `-04`), one not matching `[A-Za-z]{3,6}`, or one both occurrences share (Moscow `MSK` in 2014) is never put in the SQL; the `fold=1` literal then carries the offset alone (`-05:00`), which keeps the instant but not the region. `fold=0` values keep the region-only literal, which CUBRID already resolves to the first occurrence; every other value renders byte-identically, and a skipped spring-forward wall time is still sent with the region alone, which CUBRID rejects with an error. Verified on CUBRID 10.2 and 11.4 for `DATETIMETZ` and `TIMESTAMPTZ`, sync and async ([#820](https://github.com/cubrid-lab/pycubrid/pull/820), [2228d66](https://github.com/cubrid-lab/pycubrid/commit/2228d663b6e2c9462c648b9dd941720219c464ae)). Contributed by @yeongseon.

- **`scripts/collect_repro.py` classifies bracketed hosts the same way on every Python (#753)** — a `CUBRID_TEST_URL` whose bracketed host is not an IPv6 literal (for example `[bad]` or `[127.0.0.1]`) is now recorded as `<unparseable-url-redacted>` on all supported interpreters; early 3.11 patch releases such as 3.11.1 used to record a password-redacted URL instead. The password was redacted either way, and valid IPv6 literals such as `[::1]` still keep their host and port. Contributor tooling only; the driver is unchanged ([#776](https://github.com/cubrid-lab/pycubrid/pull/776), [164d0d5](https://github.com/cubrid-lab/pycubrid/commit/164d0d58819aea81281606708e3acfa07492c916)). Contributed by @yeongseon.

- **`make mutation` stops with a message on Python 3.11 (#812)** — mutmut 3.8 fails with a `TypeError` on 3.11 before testing any mutant. The target now checks for Python 3.12+ first, says why, and runs mutmut with the same interpreter; `docs/DEVELOPMENT.md` (+ ko) documents the requirement. CI's weekly mutation job already uses 3.12. Contributor tooling only; the driver is unchanged. Contributed by @yeongseon.

### Security

- **`scripts/collect_repro.py` redacts more forms of the password of a `CUBRID_TEST_URL` (#777)** — this contributor reproduction script, not the driver, wrote all or part of a password in plain text to `metadata.json` and `reproduce.md` for URLs without `://` (`u:pw@host/db`), passwords containing `/`, `?`, `#` or `@`, and queries containing `@`. Endpoint-resolver errors that echo part of the password (`invalid port: ... 'Syn7h'`) were also quoted. Such URLs, and any URL over 2048 characters, are now recorded as `<unparseable-url-redacted>`, resolver errors get a fixed message, and URL-derived fragments of 3 or more characters are redacted in free text such as JUnit failures (this may over-redact unrelated text). Known limits: `CUBRID_TEST_PASSWORD` is matched exactly, so a password altered in other ways (for example lowercased inside a longer word) may remain, and a JDBC-style URL without `@` (`jdbc:cubrid:host:33000:db:u:PW:`) is not recognised unless `CUBRID_TEST_PASSWORD` also holds the password. Valid `cubrid://user:pw@host:port/db` URLs are still recorded as `cubrid://user:***@host:port/db` ([#778](https://github.com/cubrid-lab/pycubrid/pull/778), [dc0683f](https://github.com/cubrid-lab/pycubrid/commit/dc0683fd27ad1a7f0500b640baa0c74276c1030c)). Contributed by @yeongseon.

### Documentation

- **Performance guide records the prepared batch evaluation (#836)** — `docs/PERFORMANCE.md` (+ ko) reports on CUBRID 10.2, 11.0, 11.2 and 11.4 (cubrid-lab/cubrid-benchmark#23) that a prototype `EXECUTE_ARRAY` request took 0.33-0.55x of `executemany()` for INSERT, UPDATE and DELETE and 0.15-0.61x for MERGE at 10,000 rows, but accepts only a narrow prepared type set; a prepare-once loop was slower on 10.2 and 11.0. `executemany()` keeps the literal batch; follow-ups are tracked in #839, #840 and #841. Docs only; driver behaviour is unchanged ([#843](https://github.com/cubrid-lab/pycubrid/issues/843)) ([6b28f5b](https://github.com/cubrid-lab/pycubrid/commit/6b28f5b943628bac0ddb2a7ac1508feebe40ef08)). Contributed by @yeongseon.

- **Performance guide explains the `executemany()` gap against CUBRIDdb (#822)** — `docs/PERFORMANCE.md` and its Korean copy gain the measured ratios (INSERT 1.13x, 1.39x and 1.54x at 100, 1,000 and 10,000 rows; MERGE up to 4.72x), the finding that about 95% of the statement phase is waiting for the broker (about 275 µs per literal batch statement against about 131 µs per prepared execute), and what users can do. The prepared-statement batch path and the cached placeholder tokenisation are tracked in #836 and #837. Docs only; driver behaviour is unchanged ([#838](https://github.com/cubrid-lab/pycubrid/issues/838)) ([f8d906d](https://github.com/cubrid-lab/pycubrid/commit/f8d906df0eb0ad0099e1e0342bf702937ae7ec2c)). Contributed by @yeongseon.

- **Performance guide covers pycubrid on CUBRID only (#795)** — `docs/PERFORMANCE.md` (+ ko) no longer compares pycubrid with PyMySQL on MySQL and is reorganised; anything not yet measured for the current release is marked "not measured" and points to baseline issue #797. The 2026-03 `cubrid-benchmark` results (pycubrid 0.5.0) are linked as historical: they predate the cursor memory-bounding fix (#203, PR #207, 1.6.0) and the 0.6.0 `fetchall()` fix. `AGENTS.md` states driver-focused performance goals and the Performance issue template drops the PyMySQL ratio. Docs only; the driver is unchanged ([#802](https://github.com/cubrid-lab/pycubrid/pull/802), [7cc4b20](https://github.com/cubrid-lab/pycubrid/commit/7cc4b20f5e0997d71da0568e35c3901358f45c1e)). Contributed by @yeongseon.

- **Private vulnerability reporting is the preferred security channel (#751)** — GitHub private vulnerability reporting is now enabled for the repository. `SECURITY.md`, `README.md` and the translated READMEs name it as the preferred private route, with the maintainer email as the fallback ([#816](https://github.com/cubrid-lab/pycubrid/issues/816)) ([fe49031](https://github.com/cubrid-lab/pycubrid/commit/fe4903168c903c5680c44be3010bccd1a137da29)), closes [#751](https://github.com/cubrid-lab/pycubrid/issues/751). Docs only. Contributed by @yeongseon.

- **Contributor and development guides state the Python 3.11+ requirement (#700)** — the prerequisites in `CONTRIBUTING.md` and `docs/DEVELOPMENT.md`, with their Korean copies, still said Python 3.10+; they now say 3.11+, and the development guide's note points to `requires-python`. The recorded past CI matrix is unchanged. Docs only ([#826](https://github.com/cubrid-lab/pycubrid/pull/826), [d727224](https://github.com/cubrid-lab/pycubrid/commit/d72722467c81cb690f002c921dc2e468110192fe)). Contributed by @yeongseon.

- **Connection guide and PRD state the Python 3.11+ requirement (#701)** — the prerequisite row in `docs/CONNECTION.md` and the runtime row of the PRD dependency matrix, with their Korean copies, still said Python 3.10+ after 1.10.0 raised `requires-python` to `>=3.11`; they now say 3.11+. Recorded past measurements and behavior that name Python 3.10 are unchanged. Docs only ([#825](https://github.com/cubrid-lab/pycubrid/pull/825), [4f6c3ff](https://github.com/cubrid-lab/pycubrid/commit/4f6c3ff69393e23689e19850f548d32ce19f15fa)). Contributed by @yeongseon.

- **README family states the full compatibility matrix as 4 × 4 on dispatch and release (#796)** — `README.md` and the Korean, German, Hindi, Russian and Chinese READMEs said the full live matrix was 5 × 4 (the last four also said nightly). Since Python 3.10 was retired, `integration-full` covers Python 3.11–3.14 × CUBRID 10.2, 11.0, 11.2 and 11.4 (16 cells) and runs only on `workflow_dispatch` and as the release gate; there is no schedule. `docs/DEVELOPMENT.md` (+ ko) now says `integration-tls` runs Python 3.11 and 3.14, and the PRD CI summary no longer lists Python 3.10. Docs only; the driver and workflows are unchanged ([#803](https://github.com/cubrid-lab/pycubrid/pull/803), [d30bedb](https://github.com/cubrid-lab/pycubrid/commit/d30bedb55c74aa97b96a790f607d7c41967f06e3)). Contributed by @yeongseon.

- **Development guide points to the current CI cost evidence (#750)** — `docs/DEVELOPMENT.md` and its Korean copy replace the historical #564 cost baseline (a 2 OS × 5 Python matrix with Python 3.10 and pip caching, which no longer exists) with a pointer to the measurements and coverage-ownership table in `docs/CI_POLICY.md` ([#808](https://github.com/cubrid-lab/pycubrid/issues/808)) ([7d43169](https://github.com/cubrid-lab/pycubrid/commit/7d43169ee2beba9ecad700829a119460254389c3)), closes [#750](https://github.com/cubrid-lab/pycubrid/issues/750). Docs only. Contributed by @yeongseon.

- **`AGENTS.md` drops stale planning context and volatile counts (#749)** — the old "Performance Loop System" snapshot (R2/R3 phases, the Week 8 decision gate, the #14–#22 issue table and fixed PyMySQL ratios) is replaced by pointers to `ROADMAP.md`, `docs/PERFORMANCE.md` and `cubrid-benchmark`, and protocol counts, the Python minimum and CI versions point to their canonical sources (`pycubrid/constants.py`, `pyproject.toml`, `docs/CI_POLICY.md`). Docs only; the driver is unchanged ([#779](https://github.com/cubrid-lab/pycubrid/pull/779), [98acc87](https://github.com/cubrid-lab/pycubrid/commit/98acc8707dea72576c2394a3cd87487fe0e23871)). Contributed by @yeongseon.

* **aio:** correct Python version notes in TLS docstrings ([#824](https://github.com/cubrid-lab/pycubrid/issues/824)) ([b20a1b6](https://github.com/cubrid-lab/pycubrid/commit/b20a1b63a4f4e23527b44504924510a980cecefd)) by @Vieta100-omo (first contribution 🎉).

* publish the current performance baseline ([#823](https://github.com/cubrid-lab/pycubrid/issues/823)) ([35770b1](https://github.com/cubrid-lab/pycubrid/commit/35770b10346f4bae644e2af61da9b2550a39760a)) by @yeongseon.

* **release:** list timed-out runs as rerunnable in the blocked-preparation recovery ([#800](https://github.com/cubrid-lab/pycubrid/issues/800)) ([2ff0d39](https://github.com/cubrid-lab/pycubrid/commit/2ff0d390c7241842a868d648971395c79f0ed7c3)) by @yeongseon.

* **release:** skip the green-publisher step for an abandoned release ([#806](https://github.com/cubrid-lab/pycubrid/issues/806)) ([f66e8de](https://github.com/cubrid-lab/pycubrid/commit/f66e8de1675e8f5dfa53d7e55d877f5d4e15d034)), closes [#801](https://github.com/cubrid-lab/pycubrid/issues/801) by @yeongseon.

* **review:** specify tool-based review routes and thread closure ([#821](https://github.com/cubrid-lab/pycubrid/issues/821)) ([5f5fef1](https://github.com/cubrid-lab/pycubrid/commit/5f5fef17dd503140990dfbc13011ac37244ddd38)) by @yeongseon.

* update Python requirement in LLM index ([#804](https://github.com/cubrid-lab/pycubrid/issues/804)) ([0bc0a21](https://github.com/cubrid-lab/pycubrid/commit/0bc0a214cc48f12fb204a610581df95fd1060d8c)) by @randomcodeross (first contribution 🎉).

### CI

- **The cookbook verifies the built wheel before it is published (cubrid-lab/cubrid-cookbook-python#240)** — `publish-pypi.yml` now runs the cookbook smoke test in candidate mode on the release wheel and requires a successful report matching its digest and version, and `publish` re-checks `dist/` and `release-meta/` against the build's checksums (`scripts/verify_dist.py`) before tagging. The gate is never skipped, `resume` included. A `dry-run` dispatch verifies the freshly built wheel; the post-publication `verify-cookbook` is unchanged. Both cookbook calls use one pin, moved to `98866c2`, and sqlalchemy-cubrid gets the same change. No runtime change. Contributed by @yeongseon.

- **The PyPI duplicate guard uses `hashlib.file_digest()` (#742)** — `scripts/pypi_duplicate_guard.py` hashes `dist/` files with the Python 3.11 stdlib helper instead of a hand-written read loop, with the same digests; the sqlalchemy-cubrid and cubrid-mcp-server copies get the same change. No runtime change. Contributed by @yeongseon.

- **Contributor tools moved from the published `dev` extra to dependency groups (#784)** — the `dev` and `mutation` extras are gone from the package metadata, so the wheel lists only the runtime dependency. `pyproject.toml` declares PEP 735 groups instead (`test`, `lint`, `security`, `release`, `docs`, `mutation`, and `dev`, which resolves to the same packages as the old extra). Contributors replace `pip install -e ".[dev]"` with `pip install -e . --group dev` (pip 25.1 or later) or `make install`. No driver, runtime dependency or supported-version change. Contributed by @yeongseon.

- **Python 3.15 in the full matrix and as the newest offline endpoint** — `integration-full.yml` runs the live suite on Python 3.11–3.15 × four CUBRID versions (20 cells) and the TLS lane on 3.11, 3.14 and 3.15; the oldest/newest offline cells of `ci.yml` and the release `offline-endpoints` move from 3.11/3.14 to 3.11/3.15. The ordinary PR smoke (3.12) and representative live cells (3.14/11.4 and 3.11/10.2) are unchanged. The manual `python-canary.yml` lane is renamed "Python 3.15 Packaging Check". Contributed by @yeongseon.

- **Tests that need the C decimal module skip without it** — CI's Python 3.15.0 has no `_decimal`, so the Decimal-subclass rendering tests are skipped there, and a new test checks that such a Python rejects a Decimal subclass with `ProgrammingError`. Contributed by @yeongseon.

- **Every workflow artifact has an explicit retention period (#750)** — the weekly SBOM upload had none and was kept for the repository default of 90 days, and `anchore/sbom-action` stored a second copy. `sbom.yml` now keeps the SBOM for 30 days and turns off the action's own upload, and a test requires `retention-days` on every `actions/upload-artifact` step. `docs/CI_POLICY.md` (+ ko) records the rule and the 2026-10-11 cache review (no cache change needed). CI only. Contributed by @yeongseon.

- **The release runs lint, type checking, the public API check and the repository tooling tests itself (#750)** — the release path used to rely on a `main` push run of `ci.yml` that nothing required to succeed and a later merge could cancel. `integration-full.yml` now runs copies of the `lint`, `typecheck`, `compat-check` and `repo-tooling-tests` jobs on the release SHA, and `full-matrix-result` fails if any is not successful, so publication is blocked. `publish-pypi.yml` is unchanged; about 1.5 extra runner minutes per release. No runtime change. Contributed by @yeongseon.

- **Weekly mutation testing is sharded under the 180-minute cap (#750)** — the `bug-hunt.yml` `mutation` job runs as four matrix shards (`protocol`, `connection`, `packet-aio-cursor-lob`, `cursor-common`) of about 1,550–1,850 of the 6,812 mutants each, with a 90-minute timeout instead of the former 300-minute exception. mutmut's sandbox now also copies `THIRD_PARTY_LICENSES.md`, which `tests/test_third_party_licenses.py` (#772) reads; without it every mutation run failed at stats collection. A test-order dependence in `tests/test_protocol_fuzz.py` (`ZoneInfo` compares by identity after its cache is cleared) is fixed by comparing the zone key. No runtime change. Contributed by @yeongseon.

- **Dependabot groups minor and patch updates (#750)** — `.github/dependabot.yml` groups minor and patch updates into one PR per ecosystem and directory (`dev-tools` for pip `/`, `docs-tools` for pip `/docs`, `github-actions`), so a weekly batch runs CI once per group. Major updates, security updates and the runtime dependency `tzdata` stay in their own PRs; majors are still held from auto-merge, and a grouped PR still needs the required checks. `docs/CI_POLICY.md` (+ ko) documents it. No runtime change. Contributed by @yeongseon.

- **"Prepare release" fails visibly when a merged release is blocked** — `scripts/reconcile_release_labels.py` now checks every merged `autorelease: pending` release PR it cannot mark tagged against the newest `publish-pypi.yml` run at its merge SHA. A running run, or a merge under 2 hours old with no run, only emits a notice. A failed, cancelled or timed-out newest run, a success without publication proof, or no run after 2 hours fails with the PR, merge SHA, run URL, conclusion and the recovery command. Before, after the 1.10.0 cookbook failure, the run stayed green. Nothing is relabelled, rerun or dispatched automatically; see RELEASING.md, "When preparation is blocked". Contributed by @yeongseon.

- **Cookbook release verification pinned to the cookbook SHA with the PyPI wait** — the `verify-cookbook` call in `.github/workflows/publish-pypi.yml` is pinned to `32e80c6ea9ae78324f764d7b873ffb64b000ddcc` (cubrid-cookbook-python#274), the same commit in pycubrid, sqlalchemy-cubrid and cubrid-mcp-server. The cookbook now waits up to 10 minutes for PyPI to serve the exact requested version before installing, and reports what PyPI served if it times out; this fixes the 1.10.0 post-publish stale-CDN failure that blocked release-please. No runtime change. Contributed by @yeongseon.

- **CHANGELOG lint applies the duplicate-heading check only after the cutoff** — `scripts/lint_changelog.py` now gates rule 5 (no repeated `###` heading within one version section) by `SECTION_POLICY_CUTOFF` like the section policy: it applies in `[Unreleased]` and releases after 1.10.0, and released history up to 1.10.0 is never rewritten. The script is now identical to the one in sqlalchemy-cubrid except for the cutoff value. Contributor tooling; the driver is unchanged. Contributed by @yeongseon.

- **CHANGELOG tooling treats fenced code as content** — `scripts/lint_changelog.py` and `scripts/compose_release_changelog.py` no longer read a `###` line inside a fenced code block as a heading. An unclosed fence, or a `## [` release header inside an open fence (which `scripts/extract_release_notes.py` would truncate at), now fails closed. Only column-0 triple-backtick fences are recognised; the same rules reach cubrid-mcp-server and cubrid-cookbook-python in follow-up PRs. Contributor tooling only; the driver is unchanged. Contributed by @yeongseon.

- **GitHub Release naming and standard release-note sections** — `AGENTS.md` gains a "GitHub Release Policy": a Release title is exactly its tag `vX.Y.Z`, tags are never moved to fix a title, and release notes come from `CHANGELOG.md`. On resume or recovery, `publish-pypi.yml` now fails closed through the new `scripts/check_release_title.py` if an existing Release has another title; it never renames one. `scripts/extract_release_notes.py` appends one `**Full Changelog**` compare link, and `scripts/lint_changelog.py` requires the standard `###` sections in order for `[Unreleased]` and releases after 1.10.0 (older headings are kept). release-please and `scripts/compose_release_changelog.py` map generated notes into those headings, and `scripts/check_release_please.cjs` reads the current manifest version instead of the stale 1.8.0 value. No runtime change. Contributed by @yeongseon.

- **Cookbook release verification pinned to the shared cookbook SHA** — the `verify-cookbook` call in `.github/workflows/publish-pypi.yml` is pinned to `bd6749093813d3a447f993fec72ac733adbeae63`, the same commit in all three package repositories (pycubrid, sqlalchemy-cubrid, cubrid-mcp-server). The cookbook adds a Python 3.11 cell (`Smoke Tests (CUBRID 11.4, Python 3.11)`), so the release report now needs all three cells (11.2/3.12, 11.4/3.12, 11.4/3.11); `RELEASING.md` lists the third job. No runtime change. Contributed by @yeongseon.

- **Documentation site build on pull requests (#786)** — `ci.yml` gains a `docs-build` job that runs the `docs.yml` build (`docs/requirements.txt`, `scripts/generate_llms_full.py`, `mkdocs build --strict`) when `docs/**`, `mkdocs.yml`, `scripts/generate_llms_full.py`, `docs.yml` or `ci.yml` change; it never uploads or deploys anything. Previously a broken strict build or docs-tool pin bump only failed on `main`. Contributed by @yeongseon.

- **Pinned docs tools and scan concurrency (#782, #783)** — `docs.yml` installs `mkdocs`, `mkdocs-material` and `pymdown-extensions` from the pinned `docs/requirements.txt`, which Dependabot now updates. `codeql.yml` and `security.yml` gain caller-level `concurrency` that cancels superseded pull-request runs only; main and scheduled runs are never cancelled or dropped. `security.yml` and `maintenance.yml` install the `bandit[toml]==1.9.4` pinned in `pyproject.toml`. Contributed by @yeongseon.

- **Scheduled and release validation without duplicate work (#750)** — `docs/CI_POLICY.md` records measured job counts, runner minutes and failure yield per event. The weekly `ci.yml` run skips lanes a successful push run of the same SHA already ran (its `detect-changes` job gains read-only `actions: read`), `bug-hunt.yml` moves to Thursday 04:00 UTC, and the release gate now runs the Python 3.11/3.14 offline suite with the 95% coverage floor (`offline-endpoints`) at the release SHA. Pull requests and main pushes are unchanged. Contributed by @yeongseon.

- **Offline tests run on the oldest and newest supported Python (#745)** — `ci.yml` now runs the full offline suite (95% coverage floor) on Python 3.11 and 3.14 instead of 3.12 alone for main pushes, the weekly schedule, manual dispatch and risk-selected PRs; ordinary PRs keep the Python 3.12 smoke cell. Coverage reports and the Codecov flag carry the Python version, and `CI Gate` fails if any endpoint cell fails, is cancelled or never runs. Contributed by @yeongseon.

- **The workflow-reader guard catches split paths and indirect readers (#770)** — `tests/test_workflow_path_impact.py` now recognises split workflow paths (such as `ROOT / ".github" / "workflows"`) and constants, helpers and fixtures bound to one, and requires every test that reads workflows, including `unittest.TestCase` subclasses and nested `Test*` classes, to carry the `repo_tooling` mark. Two-step path constants, glob patterns, imported names and tests inside top-level `if`/`try` blocks remain out of scope. Test-only; no module needed a new mark. Contributed by @yeongseon.

### Tests

- **Release-please push guards are pinned (#801)** — `tests/test_release_workflows.py` now asserts the ordered guards of `release-please.yml` (stale-main `exit`, `autorelease: review` re-check, `set -euo pipefail`, branch `case` guard, freeze-step label count, `persist-credentials: false`), so weakening any of them fails a test. `RELEASING.md` gains the recovery row for abandoning a merged release (remove `autorelease: pending`, never add `autorelease: tagged`). Test and docs only. Contributed by @yeongseon.

- **Makefile signal-cleanup test no longer depends on how pytest was launched (#775)** — `test_signal_cleanup_preserves_volumes[SIGINT-*]` failed whenever pytest ran as a background job of a non-interactive shell, because such jobs inherit SIGINT as ignored and the recipe's `trap ... INT TERM` never ran. The test now starts `make` with default SIGINT and SIGTERM dispositions, uses a stub that fails fast, and waits up to 60 s instead of 5 s. The assertions and the `Makefile` are unchanged. Contributed by @yeongseon.

## [1.10.0] - 2026-10-08

This release requires **Python 3.11 or later**. Python 3.10 applications remain on the 1.9.x line until their interpreter is upgraded; the release also removes a Python 3.10-only TLS workaround and updates async deadline, documentation and CI behavior.

### Upgrade notes

- **Python 3.11 or newer is required (#684).** Python 3.10 reached upstream end
  of life on 2026-10-01 and its retirement was announced in 1.9.0. On Python 3.10
  `pip` keeps installing 1.9.x, the last line that supports it; only the latest
  release line receives fixes, so 1.9.x gets no further releases. Upgrade Python,
  recreate your virtual environment and validate your application before upgrading. ([#708](https://github.com/cubrid-lab/pycubrid/pull/708)) ([5b3d9ec](https://github.com/cubrid-lab/pycubrid/commit/5b3d9ecfe8632c5194d1201e9540e4fdf8b8d98e)) by @yeongseon.

### Changed

- **The Python 3.10-only async TLS preflight probe is removed (#685)** —
  `AsyncConnection` no longer carries the blocking certificate-verification probe
  that ran before `loop.start_tls()` on Python 3.10 (#156). It already returned
  immediately on Python 3.11 and newer, so nothing changes there: the async TLS
  upgrade, its handshake bound and its error surface are the same. The private
  helpers `_maybe_probe_tls_verification`, `_probe_tls_verification_sync` and
  `_recv_exact_sync` are gone with their Python 3.10-only tests. Contributed by @yeongseon.
- **Async deadlines handle the built-in `TimeoutError` and keep
  `asyncio.wait_for()` (#687, #744)** — the async TCP connect, connect handshake
  and request deadlines catch the built-in `TimeoutError` (`asyncio.TimeoutError`
  is the same class since Python 3.11). They stay `asyncio.wait_for()` calls
  rather than `asyncio.timeout()` blocks while Python 3.11.0–3.11.2 are
  supported: there, a `timeout()` block entered by a task that was already
  cancelled (for example cleanup after a caught `CancelledError`) re-raises
  `CancelledError` instead of `TimeoutError` when it expires
  ([python/cpython#102780](https://github.com/python/cpython/issues/102780),
  fixed in 3.11.3), so a driver deadline would have surfaced as a cancellation
  instead of `OperationalError`. Behavior is the same as 1.9.x: a driver deadline
  raises `OperationalError` with the same messages, a transport-raised
  `TimeoutError` is still a socket failure that retires the session, a
  `TimeoutError` from a callback after a complete reply still propagates
  unchanged, and a new caller cancellation still raises `asyncio.CancelledError`
  after retiring the session. `None` means no deadline. Zero keeps its
  `wait_for()` meaning: the fresh, not-yet-started operation is cancelled before
  it runs, so `connect_timeout=0` fails before the TCP connect is attempted and
  `read_timeout=0` fails before any handshake bytes are sent, both with
  `OperationalError`. ([#754](https://github.com/cubrid-lab/pycubrid/pull/754)) ([1a44790](https://github.com/cubrid-lab/pycubrid/commit/1a44790cd5e3a6c37357fa42783ef86533106fab)) Contributed by @yeongseon.
- **Ruff and mypy target Python 3.11 (#688)** — `target-version = "py311"` and
  `python_version = "3.11"` in `pyproject.toml`, matching the minimum supported
  version. The rule selection is unchanged and no source needed a fix. Contributed by @yeongseon.

### Removed

- **Python 3.10 support (#684)** — `requires-python` is `>=3.11` and the 3.10
  classifier is gone. CI no longer tests 3.10: the oldest integration pair is
  Python 3.11 × CUBRID 10.2, the full matrix covers Python 3.11–3.14 and the TLS
  lanes run on 3.11 and 3.14. No driver behavior changes on Python 3.11 or newer. Contributed by @yeongseon.

### Documentation

- **License inventory check tolerates exact-pin bumps (#771 follow-up)** —
  `tests/test_third_party_licenses.py` now checks exact `==` dev pins for
  presence only, so a Dependabot bump such as `ruff==0.16.11` no longer fails
  the repository-tooling tests until `THIRD_PARTY_LICENSES.md` is regenerated;
  declared ranges (e.g. `mutmut>=3.8,<4`) are still enforced, and the reviewed
  docutils row must keep its "Needs review" category. Contributed by @yeongseon.
- **Reproducible third-party license inventory (#735)** — `THIRD_PARTY_LICENSES.md`
  is regenerated from fresh `.[dev]` and `.[mutation]` environments at a recorded
  commit, Python and OS by the new standard-library
  `scripts/generate_third_party_licenses.py`. MPL-2.0 packages (`certifi`,
  `hypothesis`, `pathspec`) are now classified separately from permissive
  licenses instead of under a blanket "no copyleft" statement, GPL-family
  metadata and any partly unrecognised license expression are flagged for review
  (docutils is resolved from its own `COPYING`),
  and the document separates what pycubrid uses from what it distributes. The
  Windows-only `tzdata` runtime dependency stays explicit.
  `tests/test_third_party_licenses.py` fails when a declared dependency is missing,
  a recorded version falls outside its declared range, or a row's category
  disagrees with the generator. No runtime change. ([#771](https://github.com/cubrid-lab/pycubrid/pull/771)) ([aacb5b5](https://github.com/cubrid-lab/pycubrid/commit/aacb5b588be7a15f4126a76a20f77334828fd5a4)) Contributed by @yeongseon.
- **The README and quickstart state Python 3.11 or later (#699)** — the
  requirement lines, the supported range (3.11–3.14) and the FAQ answer in
  `README.md` and `docs/README.ko.md`, and the prerequisites in
  `docs/quickstart.md` and `docs/ko/quickstart.md`. The Python 3.10 retirement
  notice in both READMEs now describes the requirement as in effect from 1.10.0. ([#739](https://github.com/cubrid-lab/pycubrid/pull/739)) ([27e35be](https://github.com/cubrid-lab/pycubrid/commit/27e35be9eee6518c1c673e51195fa25954dd5da4)) Contributed by @yeongseon.
- **Python 3.10 async TLS probe guidance is removed (#686)** — the preflight
  probe is gone (#685), so the README, `SECURITY.md`, the connection, protocol,
  development, example, API, PRD and support-matrix pages and their Korean
  counterparts no longer describe it or the Python 3.10 `start_tls()` hang it
  worked around. The troubleshooting section "Async TLS Handshake Hangs on
  Python 3.10" is deleted, with the links to it in the translated READMEs; the
  sync driver's Python 3.10 `wrap_socket()` reset note goes with it. ([#737](https://github.com/cubrid-lab/pycubrid/pull/737)) ([4df5db0](https://github.com/cubrid-lab/pycubrid/commit/4df5db0331c2072440d22b0b08ed20323ecaec93)) Contributed by @yeongseon.
- **In-page links work on the documentation site (#728)** — the site used the
  default heading slugifier, which drops non-ASCII characters, so Korean
  headings got ids such as `_2` and every Korean table of contents entry was
  dead; English links written for GitHub's slugs (`#tls--ssl`) missed as well
  (198 broken anchors). `mkdocs.yml` now uses the Unicode-aware slugifier from
  `pymdown-extensions`, the two anchors that were still wrong are fixed, and
  `validation.links.anchors: warn` makes the strict build fail on a new broken
  anchor. ([#729](https://github.com/cubrid-lab/pycubrid/pull/729)) ([c2a4f1d](https://github.com/cubrid-lab/pycubrid/commit/c2a4f1db875c65a3c497daf6427711d643401dec)) Contributed by @yeongseon.
- **Korean pages reviewed against the English wording (#726)** — the structure
  check cannot see a missing or outdated sentence. A sentence-level review of
  seven Korean guides corrected 72 passages: `API_REFERENCE` (21, including the
  missing `Lob.read()` paragraph on repeated `LOB_READ` round-trips),
  `UPSTREAM_COMPATIBILITY` (20), `DEVELOPMENT` (13, including the async TLS
  matrix tests), `PROTOCOL` (8), `PARAMETER_BINDING` (6, replacing an outdated
  description of escape-mode negotiation), `TROUBLESHOOTING` (4) and
  `CONNECTION` (2). The translations of `PRD` and `PREPARED_BINDING_DESIGN`
  added in this release were reviewed the same way (1 and 15 corrections,
  mostly literal renderings that read ambiguously). The banner on every Korean
  page now says CI checks the structure. English is unchanged. ([#727](https://github.com/cubrid-lab/pycubrid/pull/727)) ([ed6e7d9](https://github.com/cubrid-lab/pycubrid/commit/ed6e7d934e2931ebd286761e5b9a69e327ce9db9)) Contributed by @yeongseon.
- **PRD no longer quotes figures that every release makes false (#724)** —
  `docs/PRD.md` said version 1.8.0, "770 offline tests / 811 total", "97.29%
  coverage", "10 modules", "6 guide files" and "release workflow on tag". The
  version, test and coverage figures now link to PyPI, the test tree and
  Codecov, the per-file test counts are dropped, and the release row describes
  the release-PR flow. The documentation home page states SQLAlchemy 2.0–2.1 for
  sqlalchemy-cubrid, as that project does. English and Korean. ([#725](https://github.com/cubrid-lab/pycubrid/pull/725)) ([8a3304b](https://github.com/cubrid-lab/pycubrid/commit/8a3304bc7acebeb4368e24d3275940e6ef3839ed)) Contributed by @yeongseon.
- **PRD and the typed CAS binding design are available in Korean (#715)** —
  `docs/ko/PRD.md` and `docs/ko/PREPARED_BINDING_DESIGN.md` are added, both PRD
  pages join the site navigation, and the Korean home page and CI policy carry
  the same translation header as the other Korean pages. With these, every
  document under `docs/` has a Korean counterpart and the structure check has no
  exception left. ([#722](https://github.com/cubrid-lab/pycubrid/pull/722)) ([328720d](https://github.com/cubrid-lab/pycubrid/commit/328720d4044d53db8288f95f74f8f9e5894b6a36)) Contributed by @yeongseon.
- **Korean documentation home page and one Korean navigation section (#714)** —
  `docs/ko/index.md` is the Korean landing page, linked from the English home
  page and back. The Korean guides were spread over three navigation groups;
  they are now one "한국어" section with the same grouping as the English
  navigation, and every Korean page is reachable from it. ([#720](https://github.com/cubrid-lab/pycubrid/pull/720)) ([73e4ea7](https://github.com/cubrid-lab/pycubrid/commit/73e4ea756a1efda3d09e43e22b6ff13aa585174a)) Contributed by @yeongseon.
- **Korean support matrix and CI policy match the English documents (#713)** —
  `docs/ko/SUPPORT_MATRIX.md` gains the CI matrix table, the "Server Behavior
  Differences Between CUBRID Versions" table and the unknown-option row, and
  loses a Korean-only closing section; `docs/ko/CI_POLICY.md` is a full
  translation instead of a summary. Both CI policy pages are in the site
  navigation. The full-integration count in the support matrix is corrected to
  16 (four Python versions × four CUBRID versions) in both languages. ([#719](https://github.com/cubrid-lab/pycubrid/pull/719)) ([66e2c5e](https://github.com/cubrid-lab/pycubrid/commit/66e2c5e346c3f270bff3cfc0c0b2797648bccc2a)) Contributed by @yeongseon.
- **Korean connection, development and troubleshooting guides are in sync again
  (#712)** — four sections existed only in English: unknown connection options
  (`docs/ko/CONNECTION.md`), "Connection Option Has No Effect"
  (`docs/ko/TROUBLESHOOTING.md`), and the backslash-escape-mode pin and mutation
  testing (`docs/ko/DEVELOPMENT.md`). They are translated; the English text is
  unchanged. ([#718](https://github.com/cubrid-lab/pycubrid/pull/718)) ([16a0665](https://github.com/cubrid-lab/pycubrid/commit/16a06659597ea9a93b899291a480d0a2e1050d8b)) Contributed by @yeongseon.
- **`get_error_description()` is documented, and the Korean API reference is
  complete again (#711)** — `docs/API_REFERENCE.md` covered every public name
  except `pycubrid.get_error_description`; it now has its own section in English
  and Korean. The Korean reference also gains the sections it was missing:
  unknown connection options, `UnknownConnectionOptionWarning`, the native
  prepared example and the wrapper cursor's `next()` alias (also added to the
  English text). No behavior change. ([#717](https://github.com/cubrid-lab/pycubrid/pull/717)) ([221339e](https://github.com/cubrid-lab/pycubrid/commit/221339eec34912ec0a0adec665673a25541fccdf)) Contributed by @yeongseon.

**Additional commit references (generated at release time):**

* **agents:** align contributor ownership and good-first-issue guardrails ([#752](https://github.com/cubrid-lab/pycubrid/issues/752)) ([fb3da8c](https://github.com/cubrid-lab/pycubrid/commit/fb3da8c94b86cf1ad576989f84c6ba465b9e0894)), closes [#743](https://github.com/cubrid-lab/pycubrid/issues/743) by @yeongseon.
* **ci:** correct what enable-cache auto does on the release path ([#764](https://github.com/cubrid-lab/pycubrid/issues/764)) ([e0e475b](https://github.com/cubrid-lab/pycubrid/commit/e0e475b62741f22a3d6deeed56c7e8ba1cbf3007)), closes [#759](https://github.com/cubrid-lab/pycubrid/issues/759) by @yeongseon.

### Tests

- **CI fails when a Korean document drifts from its English source (#716)** —
  `scripts/check_docs_translation.py` compares every `docs/<name>.md` with
  `docs/ko/<name>.md` and fails on a missing Korean file or a different number
  of headings (levels 2-4), fenced code blocks or table rows; the lint job runs
  it on every event. Until now only README drift was checked, and six guides
  had drifted unnoticed. Documents that are deliberately English-only are
  listed in the script with a reason. The check compares structure, not wording. Contributed by @yeongseon.
- **The official-driver differential lane runs on Python 3.11 (#683)** — the
  pinned CUBRIDdb oracle (cubrid-python `e75ec36`, CCI `7d1eb8f`) was built and
  compared on Python 3.10 only. The `official-differential` job in `ci.yml` and
  `integration-full.yml`, the claims fixture and the generated compatibility
  summaries now use Python 3.11, so the lane no longer depends on the
  interpreter that 1.10.0 retires. Claims, source pins and the required servers
  (10.2 and 11.4) are unchanged; earlier evidence recorded on Python 3.10.12
  stays in `docs/UPSTREAM_COMPATIBILITY.md` as history. Contributed by @yeongseon.

## [1.9.0] - 2026-10-04

This release expands the opt-in `pycubrid.compat` interfaces for applications migrating from CUBRID's official Python drivers, adds a `charset` connection option and typed `Set`/`Multiset`/`Sequence` parameters, and makes DB-API errors, transactions and malformed-reply handling stricter and more predictable. **Read the upgrade notes:** existing 1.8.0 compatibility names were promised to remain stable during 1.x at publication time, while newer compatibility surfaces were described as provisional. That original commitment was later explicitly reversed in v1.17.0; this historical note does not silently rewrite either policy.

### Upgrade notes

Behavior changes you may notice (details in the entries below):

- **`pycubrid.compat` additions are provisional.** Compatibility names first released in 1.9.0 or later may change or be removed in a later MINOR release, announced in the release notes; names already in 1.8.0, and all of `pycubrid` and `pycubrid.aio`, keep the normal 1.x guarantee. Comparing `api-baseline.json` at v1.8.0 and v1.9.0, 1.9.0 adds these names and removes none: `pycubrid.compat.cursors` (`Cursor`, `DictCursor`); `pycubrid.compat.native.lob`, `pycubrid.compat.native.set` and the `SEEK_*` constants; native connection `lob()`, `set()`, the cached `autocommit`, `isolation_level`, `lock_timeout` and `max_string_len` members, `server_version()`, `client_version()`, `ping()`, `set_autocommit()` and `set_isolation_level()`; native cursor `fetch_lob()`, `bind_lob()`, `bind_set()`, `data_seek()`, `row_seek()`, `row_tell()` and `result_info()`; and `commit()`, `rollback()`, `cursor()`, `ping()`, `server_version()`, `get_autocommit()`, `set_autocommit()`, `set_fetch_value_converter()` and an `autocommit` property on the CUBRIDdb-style wrapper connection. The ordinary-driver additions (`Set`, `Multiset`, `Sequence` and the `charset` option) are not provisional. See [the release policy](https://github.com/cubrid-lab/pycubrid/blob/v1.9.0/RELEASE_POLICY.md#2-semantic-versioning-rules) by @yeongseon.
- **Foreign-key restrictions raise `IntegrityError`.** A `DELETE`/`UPDATE` of a parent row that a foreign key still references (`-924`) and a `TRUNCATE` of a referenced parent table (`-1284`) now raise `IntegrityError` (SQLSTATE `23000`, still a `DatabaseError` subclass) instead of a generic `DatabaseError`. (#493) by @yeongseon.
- **Values Python cannot represent raise `DataError` and keep the session.** On ordinary sync and async cursors, a complete reply holding such a value no longer raises `OperationalError('malformed response from broker')` and closes the connection: zero `DATE`/`DATETIME`/`TIMESTAMP` values (#512), undecodable character values, and invalid JSON decoded by the built-in `json.loads` (#543). Errors from a caller-supplied `json_deserializer` are not reclassified, and the prepared API in `pycubrid.compat.native` stays fail-closed (`OperationalError`, session closed). Contributed by @yeongseon.
- **`CALL`, `callproc()`, `EVALUATE` and `NULL`-typed columns return decoded values** (`42`, a `datetime`) instead of raw `bytes`. (#542) by @yeongseon.
- **A failed `execute()` no longer leaves the previous result on the cursor.** After the previous query handle was closed, a failure leaves `description` as `None`, `rowcount` as `-1` and nothing fetchable. If closing the previous handle itself fails, `execute()` raises and the buffered result is kept. (#373) by @ryanduguid.
- **`decimal.Decimal` parameters are sent in plain fixed-point notation,** so values such as `Decimal("1E-7")` stay `NUMERIC` instead of coming back as `float`. Contributed by @yeongseon.
- **Invalid timeouts are rejected before a socket is opened:** negative, NaN and infinite `connect_timeout`/`read_timeout` values. (#367) by @yeongseon, co-authored by @blackmore-technology-group.
- **`Lob.read()`/`Lob.write()` raise `InterfaceError`** for an `offset` or `length` that is not a plain `int`, including `bool`. (#449) by @HarshRajSinghania.
- **`DBAPIType` no longer compares equal to `bool` values:** `STRING == True` is `False`. (#369) by @Mr-Neutr0n.
- **`fetchmany(size)` requires a plain Python `int`** when a size is supplied on ordinary sync and async cursors: floats, booleans and integer subclasses now raise `ProgrammingError` before consuming rows or requesting a page. Omitted/`None` sizes still use `arraysize`, and zero/negative integers still return `[]`. (#371) Positive-only validation is reserved for a future major release ([#678](https://github.com/cubrid-lab/pycubrid/issues/678)) by @yeongseon.
- **A stalled sync TLS handshake gives up after 10 seconds.** `connect(..., ssl=...)` without `read_timeout` now raises `OperationalError` instead of waiting forever. (#535) by @yeongseon.
- **Python 3.10 support is deprecated;** see Deprecated below. Contributed by @yeongseon.

### Added

- **`charset` connection option (#86)** — `pycubrid.connect()`, `pycubrid.aio.connect()`, `pycubrid.compat.native.connect()` and `cubriddb.Connection(charset=...)` (previously `"utf8"` only) accept `charset` (default `"utf-8"`): a Python codec or the CUBRID names `utf8`, `euckr`, `iso88591`. It is validated before any socket work (`TypeError` for a non-string; `ValueError` for an unknown codec, CUBRID `binary` or a codec that is not ASCII-transparent, such as UTF-16/32, Shift_JIS, Big5, GBK or CP949; `DataError` for unencodable credentials) and kept across reconnects. SQL text, batch and schema-info arguments, prepared strings and `OPEN_DATABASE` credentials are encoded with it before anything is sent; an unencodable character raises `DataError` naming the codec and position, sends nothing and keeps the session. Character values, `ENUM` and collection elements, and column/table names and defaults are decoded strictly (`DataError` naming the codec); error messages use replacement. Fetched `JSON` stays UTF-8, and `NUMERIC`, time zone names, version strings and LOB contents are unaffected (`CLOB` bytes are in the column charset; LOB file locators decode with `errors="replace"`). `get_schema_info()` checks its arguments first, so an unencodable pattern no longer closes the connection. The broker does no conversion, so the codec must match the database charset (a `CHARSET utf8` column in an EUC-KR database raises `DataError` under `charset="euckr"`; convert with `CAST(... CHARSET euckr)`). With the default UTF-8 codec, behavior is unchanged except for two edge cases: an invalid-UTF-8 column name now raises `DataError` and keeps an ordinary cursor's session (previously `OperationalError` and a closed connection), and a database/user/password longer than the 32-byte `OPEN_DATABASE` field is cut on a character boundary instead of mid-character. With `euc_kr`, Hangul outside KS X 1001 (such as 똠) is rejected as unencodable. `charset=None` means the default and a locale such as `"ko_KR.euckr"` is accepted. A new `integration-charset` CI job runs live round trips on CUBRID 11.4 with `CUBRID_LOCALE=ko_KR.euckr` ([#510](https://github.com/cubrid-lab/pycubrid/pull/510), [3874252](https://github.com/cubrid-lab/pycubrid/commit/387425245e70c63e6b5d99265d5e8cbca61affb0)). Contributed by @yeongseon.
- **Typed collection parameters for ordinary cursors (#567)** — new `pycubrid.types.Set`, `Multiset` and `Sequence` (also exported from `pycubrid`) wrap an immutable tuple and bind through `execute()` and `executemany()` on sync and async cursors as `SET{...}`, `MULTISET{...}` and `SEQUENCE{...}`, so SQLAlchemy and other DB-API callers can bind CUBRID collections (cubrid-lab/sqlalchemy-cubrid#484). Elements use the hardened scalar renderer (#518, #528), nested collections raise `ProgrammingError`, and the classes cannot be subclassed. A `dict` argument raises `TypeError`, and `Sequence` also rejects a `set`/`frozenset` because its order would be nondeterministic. Plain Python `set`/`list`/`tuple` parameters stay rejected (the message now names the typed classes), and fetching is unchanged (`decode_collections=True` still returns `frozenset`/`list`). The instances are immutable, copyable and picklable: re-calling `__init__` does nothing (#568 review), and `format_parameter()` raises `ProgrammingError` for an instance that bypassed `__new__` (for example `object.__new__(Set)`). MULTISET duplicates and SEQUENCE order round-trip live on CUBRID 10.2 and 11.4 ([#568](https://github.com/cubrid-lab/pycubrid/pull/568), [17cc5dc](https://github.com/cubrid-lab/pycubrid/commit/17cc5dca98ee33ad052ce5ba8d04bfe6b9826812)) ([#580](https://github.com/cubrid-lab/pycubrid/pull/580), [882b704](https://github.com/cubrid-lab/pycubrid/commit/882b704165ac7d1b614c2831995a8071c3b654ac)). Contributed by @yeongseon.
- **Native LOB handle fetch and bind (#441)** — `pycubrid.compat.native` gains sync-only `connection.lob()` (or `native.lob(conn)`), `cursor.fetch_lob(col, lob, /)`, `cursor.bind_lob(index, lob, /)` and a local `lob.close()`. `fetch_lob()` consumes the next row like `fetch_row()` and takes the BLOB/CLOB type from the requested column; at the end of the result it returns `None`, a non-LOB column raises `ProgrammingError` without consuming the row, and a NULL cell leaves the lob empty. `bind_lob()` sends the official bind bytes; a fetched handle of a committed row may be bound again, on another connection or after a reconnect (the server stores a copy), while an empty or closed lob raises `InterfaceError` before any request. Arguments are checked in the official order (`TypeError` for a non-int index or column or a non-lob argument first), and `fetch_lob()` fills only an open lob of its own connection. A complete reply whose cell is not a handle of the column's LOB type raises `DataError` without consuming the row and keeps the session; damaged handle framing raises `OperationalError` and retires the session. Each binding belongs to the physical session that made it and is never sent to a replacement session or another connection (internal wire contract, not user-visible). Live 10.2/11.4 round trips (0 bytes to 1 MB) and fourteen official-driver differential claims (eleven match, three classified deviations) pass ([#616](https://github.com/cubrid-lab/pycubrid/pull/616), [068325e](https://github.com/cubrid-lab/pycubrid/commit/068325e75abb068a6b9d4f9bc841038cb7743a1e)). Contributed by @yeongseon.
- **Native LOB byte-position stream (#442)** — `pycubrid.compat.native.lob` gains sync-only `write(data, type="B")`, `read(length=0)`, `seek(offset, whence=SEEK_CUR)` and the `SEEK_*` constants. BLOB and CLOB text is UTF-8, seek positions count bytes and `SEEK_END` subtracts its offset. Writes create the LOB lazily and append only at the tracked end, and reads span the broker's chunk cap; a successful short read advances the position even if a later `LOB_READ` reply fails, so a live session never repeats accepted bytes. Deliberate safe differences from the official extension: non-end writes and invalid positions are rejected locally, empty/EOF returns `""`, and oversized reads are clamped. Temporary handles created by `write()` stay single-use after an autocommit bind, and the physical-session fences apply to read/write as to bind. Ordinary `Lob.read(length, offset=0)`/`write(bytes, offset=0)` and async are unchanged ([bebef4d](https://github.com/cubrid-lab/pycubrid/commit/bebef4d3b9abdc65feef62d770d014e8ececff4c)). Contributed by @yeongseon.
- **Native LOB file transfer (#443)** — sync native holders gain `imports(file, type="B")` and `export(file)` for raw BLOB/CLOB bytes in bounded chunks. Import stages a replacement until the whole input closes, and export publishes a sibling temporary file only after a successful write, flush and close. Exact-string paths, local error codes, empty populated output and same-session ownership are explicit safety differences from the official C extension; empty output is candidate-only evidence, not official parity, and no general filesystem or whole-driver parity is claimed ([#653](https://github.com/cubrid-lab/pycubrid/pull/653), [5d40a0f](https://github.com/cubrid-lab/pycubrid/commit/5d40a0f8bfc1f51ff43ab4b1c8bd3b119783a999)). Contributed by @yeongseon.
- **Native collection binding (#440)** — `pycubrid.compat.native` gains sync-only `connection.set()` (or `native.set(conn)`), `set.imports(data, type, /, *, kind=SET)` and `cursor.bind_set(index, s, /)`. As in the official driver every element is sent as a STRING element (any element type code except BIT/VARBIT, which raise `NotSupportedError`). `kind=MULTISET` keeps duplicates and `kind=SEQUENCE` keeps duplicates and order; MULTISET is sent as SEQUENCE because CUBRID 10.2/11.4 brokers reject the MULTISET bind kind (`-454`). `data` must be a tuple (`InterfaceError` otherwise); `None` is a NULL element, the text `'NULL'`, empty strings and Python `int` elements (INT only, signed 64-bit) are accepted, an element containing NUL is rejected (classified deviations; fourteen differential claims, seven match, seven deviations), and a set never imported binds SQL NULL. A server conversion error keeps the prepared handle usable. Invalid input fails before I/O and leaves the set and slot unchanged. The wrapper `execute(..., set_type)`/`executemany` collection shapes are not provided (#610) ([#607](https://github.com/cubrid-lab/pycubrid/pull/607), [c316c39](https://github.com/cubrid-lab/pycubrid/commit/c316c39a3c7ef993c541b93114380dc10ed778dc)). An internal wire contract for immutable SET/SEQUENCE/MULTISET values of INT or STRING elements shipped first, with no public API of its own (#482) ([#569](https://github.com/cubrid-lab/pycubrid/pull/569), [bf53d2c](https://github.com/cubrid-lab/pycubrid/commit/bf53d2ca45290533ad8d6b47cbb54c4a9ba533c5)). By @yeongseon.
- **Native cursor positioning (#444)** — sync native cursors gain `data_seek`, `row_seek` and `row_tell` with the official dual counters and boundary clamp/error behavior; a cache miss fetches an absolute position without replaying the query or reconnecting. Cross-page seeking needs explicit manual `autocommit=False` before preparation, because the default autocommit can release a server result after its last batch ([#444](https://github.com/cubrid-lab/pycubrid/issues/444), [ff8eee7](https://github.com/cubrid-lab/pycubrid/commit/ff8eee7fd4c8c403b48000747293bf55af3bb3c3)). Contributed by @yeongseon.
- **Native extended column metadata (#445)** — the opt-in sync native cursor gains `result_info([n])`, returning cached 15-field tuples (CCI types, integer constraint flags, names and default strings). It leaves row position and `description` unchanged, keeps metadata after EOF/commit/rollback for the same owner, and hides it after a failed execution. Local `InterfaceError` carries a `.code` as a classified difference. Owned 10.2/11.4 and pinned official comparisons cover this subset, not non-UTF-8 data or complete driver parity ([#642](https://github.com/cubrid-lab/pycubrid/pull/642), [ee62e28](https://github.com/cubrid-lab/pycubrid/commit/ee62e28a5f316e779da20cfe575a1cd0ed2dda30)). Contributed by @yeongseon.
- **Native cached settings and effective mode setters (#467)** — the sync `pycubrid.compat.native.connection` exposes writable cached `autocommit`, `isolation_level`, `lock_timeout` and `max_string_len` members (assignment sends nothing), plus positional-only `set_autocommit(bool)` and `set_isolation_level(4/5/6)`, which change the effective mode only on success. As in CCI, an autocommit change commits first only when the mode actually changes during an active transaction; an isolation change does not commit. Initial lock/max/isolation values come from the broker (only a complete max-string server error falls back to 0), and the official extension's fresh level-4 `UNKNOWN` text quirk is kept until the setter corrects it. Manual-mode LOB fetches keep their conservative non-transferable provenance after a later commit. The `compat.cubriddb` wrapper adds a bool-validated `autocommit` setter/property and a raw-cache getter. Ordinary defaults and the ordinary/async setting APIs are unchanged ([7a22678](https://github.com/cubrid-lab/pycubrid/commit/7a226783103136fa518f5a479622799b2bea8b4d)). Contributed by @yeongseon.
- **Native connection utilities (#666)** — opt-in zero-argument `server_version()`, frozen own-driver `client_version()` and integer `ping()` on the native connection, plus the wrapper's server-version and ping delegates. Requests stay on the exact live physical session with no retry or reconnect, and client identities are not a matching-ID or version-format promise. This additive subset does not certify full native error/recovery parity ([#667](https://github.com/cubrid-lab/pycubrid/pull/667), [9fbbc05](https://github.com/cubrid-lab/pycubrid/commit/9fbbc05b494e52197907bea138b36a3380ff6421)). Contributed by @yeongseon.
- **Qualified wrapper row cursors (#466)** — `cubriddb.Connection.cursor()` selects tuple or exact-name dictionary rows, and `pycubrid.compat.cursors.Cursor`/`DictCursor` can be built directly. SELECT uses the official seven-field description, preserving Unicode, SQL NULL versus empty text, duplicate-name overwrite and the official falsey bulk-fetch versus `None` iteration stop. The small `execute(query, args=None, set_type=None)` bridge reuses native INT32/string/NULL binding; a non-`None` `set_type`, mappings, collection/LOB arguments and `executemany` are not added. Non-SELECT `description` stays `None`. Garbage-collected wrapper cursors now make a best-effort same-session handle cleanup without commit or reconnect; explicit `close()` remains deterministic ([2215d0a](https://github.com/cubrid-lab/pycubrid/commit/2215d0a739252a25e93a399a9e3fc84a96501f48)). Contributed by @yeongseon.
- **Wrapper transaction delegates (#662)** — the opt-in sync CUBRIDdb-style connection gains zero-argument `commit()` and `rollback()`, forwarding once to the native owner and returning `None` with unchanged error propagation. Rollback can invalidate fetching while wrapper `rowcount`/`description` snapshots remain; manual mode is explicit ([#664](https://github.com/cubrid-lab/pycubrid/pull/664), [44aafb9](https://github.com/cubrid-lab/pycubrid/commit/44aafb9ed7cf413eb558d0a3709c88fcbb19e38c)). Contributed by @yeongseon.

### Changed

- **Internal refactors with no behavior change** — a private CCI extended-type value is now kept from column metadata as a prerequisite for `result_info` (#631); ten simple CAS reply parsers share one private header/error helper (#560); the sync connection imports `Cursor` directly and an unused async connect-dispatch helper is removed (#561); and each reply is now explicitly marked as session-verified instead of comparing `CAS_INFO` object identity, with one shared backslash-escape probe helper (#525). Verification stays per reply (every `OUT_TRAN` reply still gets its own `CHECK_CAS`), and 46 sync/async replay scenarios send identical request sequences before and after. Public APIs and behavior are preserved. Contributed by @yeongseon.

### Deprecated

- **Python 3.10 support** — advance notice for the 1.9.x release. Python 3.10 reached upstream end of life on 2026-10-01 ([PEP 619](https://peps.python.org/pep-0619/#310-lifespan)). The 1.8.x line and 1.9.x retain Python 3.10 support; the following minor release (planned 1.10.0) will require Python >=3.11. Upgrade Python, recreate your virtual environment and validate your application before upgrading. Package metadata, CI coverage and runtime behavior are unchanged in 1.9.0; see the [support matrix](https://github.com/cubrid-lab/pycubrid/blob/v1.9.0/docs/SUPPORT_MATRIX.md) and [roadmap](https://github.com/cubrid-lab/pycubrid/blob/v1.9.0/ROADMAP.md) ([#658](https://github.com/cubrid-lab/pycubrid/issues/658), [ba58b80](https://github.com/cubrid-lab/pycubrid/commit/ba58b80f5cc7af26f664f06a4b5c5e78bade827b)) by @yeongseon.

### Fixed

- **`fetchmany()` validates its size before fetching (#371)** — ordinary sync and async cursors raise `ProgrammingError` for non-integer sizes, including floats and booleans, before buffer consumption or a FETCH request. Positive sizes, omitted/`None` defaults, the zero/negative no-ops and the separate compatibility wrapper keep their behavior. Classified MINOR because booleans and nonpositive floats previously completed without error ([#679](https://github.com/cubrid-lab/pycubrid/pull/679), [8dd1198](https://github.com/cubrid-lab/pycubrid/commit/8dd11989167594f36774da824224064073883240)). Contributed by @yeongseon.
- **Connection timeouts are validated before socket creation (#367)** — sync and async connections reject negative, NaN and infinite `connect_timeout`/`read_timeout` values during common initialization, so an invalid value no longer leaks a newly opened socket; `None`, zero and finite positive values keep their meaning ([#646](https://github.com/cubrid-lab/pycubrid/pull/646), [7577e4d](https://github.com/cubrid-lab/pycubrid/commit/7577e4d5c33911ee7c7ca7707fbd37e2ad9e04d4)). Contributed by @yeongseon, co-authored by @blackmore-technology-group.
- **`DBAPIType` no longer reads `bool` values as integer type codes (#369)** — `STRING == True` is now `False` and `STRING != False` is `True`; integer subclasses such as `enum.IntEnum` still compare equal by value, so `STRING == CUBRIDDataType.STRING` is unchanged ([#479](https://github.com/cubrid-lab/pycubrid/issues/479), [4efe599](https://github.com/cubrid-lab/pycubrid/commit/4efe599d29965e606e3ce77682c162be9830fcd7), [#369](https://github.com/cubrid-lab/pycubrid/issues/369)). By @Mr-Neutr0n (first contribution 🎉).
- **`Lob.read()`/`Lob.write()` reject non-int and boolean offset/length (#449)** — `offset` and `length` must be a concrete `int` (`type(value) is not int` is rejected, including `bool`, `float`, `str` and `IntEnum` members) and raise `InterfaceError("<name> must be an int, got <type>")` before `_ensure_connected()` or any packet is built. Previously `lob.write(b"", offset=True)` silently returned `0` and a `float` offset failed late in wire serialization with `DataError`. Valid non-negative `int` values, the empty-write shortcut's return value (#394) and the `DataError` for an in-range `int` too large to serialize (for example `offset=2**63`) are unchanged. The closed-LOB-before-invalid-argument ordering test is adapted from heyadhithya's PR #458 ([#516](https://github.com/cubrid-lab/pycubrid/pull/516), [dad392e](https://github.com/cubrid-lab/pycubrid/commit/dad392e8396ae82c7db962671c1c714b3dfe2dd5)). Contributed by @yeongseon and @HarshRajSinghania, co-authored by @heyadhithya.
- **`Lob.write()` keeps the handle's size field current (#441)** — the packed handle in `Lob.lob_handle` embeds the LOB size, which the server trusts when the handle is sent back, but `write()` left it at its creation size (usually 0), so a bound written handle stored the wrong length. It now raises the field to `offset + bytes written` after each write, never lowering it, as CCI does; a reply claiming more bytes than were sent raises before the handle changes. Return values and errors are unchanged, and written BLOB/CLOB handles up to 1 MB store their full length on live 10.2/11.4 ([#615](https://github.com/cubrid-lab/pycubrid/issues/615), [35d4e2c](https://github.com/cubrid-lab/pycubrid/commit/35d4e2cbedbea85bbdac0d76d047572e5cffb6ff)). Contributed by @yeongseon.
- **`callproc()` rejects malformed procedure names (#372)** — empty or invalid dot-separated segments raise before any SQL runs, in sync and async cursors; valid single and qualified identifiers still work ([#515](https://github.com/cubrid-lab/pycubrid/issues/515), [601a879](https://github.com/cubrid-lab/pycubrid/commit/601a879f52b951ccdc366d95fde47b3d2d84b75a), [#372](https://github.com/cubrid-lab/pycubrid/issues/372)). By @yotam1001 (first contribution 🎉).
- **A failed `execute()` clears the previous result (#373)** — after closing the old query handle, a later binding or request failure leaves `description=None`, `rowcount=-1`, `lastrowid=None` and no fetchable rows or held fetch-page error, in sync and async cursors. If closing the old handle fails, `execute()` keeps the buffered result and its page error, though connection invalidation or reconnect handling may still retire the handle ([#531](https://github.com/cubrid-lab/pycubrid/issues/531), [32195b1](https://github.com/cubrid-lab/pycubrid/commit/32195b1f856b8d499a6e45ebfb89ee4a7ebc57ea)). By @ryanduguid (first contribution 🎉).
- **Foreign-key restrictions raise `IntegrityError` (#493)** — a `DELETE`/`UPDATE` of a referenced parent row (native `-924`, `ER_FK_RESTRICT`) and a `TRUNCATE` of a referenced parent table (`-1284`, `ER_TRUNCATE_PK_REFERRED` on CUBRID 11.4; 10.2 reports `-924`) raise `IntegrityError` with SQLSTATE `23000` for single statements and batch failures alike. Dropping a referenced primary key (`-923`) is a schema error and stays `DatabaseError` ([95a34e6](https://github.com/cubrid-lab/pycubrid/commit/95a34e6fcb6dcca27ed10a11e24aadb18cb34c2d)). By @yeongseon.
- **Reserved-word hints describe the diagnostic position (#509)** — an `unexpected 'VARCHAR'` syntax message no longer claims that `VARCHAR` is the offending identifier; the hint says an identifier at or before the reported token may be reserved, with the existing quoting advice. Server text and error metadata are unchanged ([5fde41f](https://github.com/cubrid-lab/pycubrid/commit/5fde41f37dd030f7c6098f3a7a8d70a9ecbc5f4a)). Contributed by @yeongseon.
- **`CALL` and `EVALUATE` results and `NULL`-typed columns decode their values (#542)** — under CAS protocol 8 (CUBRID 10.2+) such cells start with a two-byte type header, but the row parser read one byte and returned the cell as raw `bytes`. `CALL` of a stored function or method, `callproc()` and `EVALUATE` now return the decoded value (for example `42` instead of `b'\x08\x00\x00\x00*'`, or a `datetime`), sync and async, and collection values keep their kind. The single-byte header of older brokers is still accepted, and a header longer than its cell is a malformed reply. Verified live on CUBRID 10.2 and 11.4 ([#549](https://github.com/cubrid-lab/pycubrid/pull/549), [e329a11](https://github.com/cubrid-lab/pycubrid/commit/e329a117c153d891e85b53a1cfd24580ca38766e)). By @yeongseon.
- **Nonempty NULL-only collections decode (#483)** — with `decode_collections=True`, a nonempty `SET`/`MULTISET`/`SEQUENCE` whose elements are all SQL NULL (`{NULL}`, `{NULL, NULL}`) decodes to `[None, ...]` (a `SET` becomes `frozenset({None})`) instead of raw `bytes`. A NULL-type header, including an empty collection, whose count does not match the payload or whose element lengths are not NULL markers raises `OperationalError('malformed response from broker')`. Default raw-bytes mode is unchanged ([c588055](https://github.com/cubrid-lab/pycubrid/commit/c588055f9bbe31ae6872430bd75aede26060cbec), [e379116](https://github.com/cubrid-lab/pycubrid/commit/e379116e618791b8a65a060c108139954ff3fea9), [#483](https://github.com/cubrid-lab/pycubrid/issues/483), [775f38c](https://github.com/cubrid-lab/pycubrid/commit/775f38c1d2b2422617ae8546ce11b3ccd445bc7f)). By @yeongseon.
- **Invalid UTF-8 in a complete reply keeps the session (#492)** — server and batch error messages are decoded with `errors="replace"`, so the real CUBRID error surfaces with its `errno`/`sqlstate` (this happens when CUBRID cuts an echoed value inside a multi-byte character). A `CHAR`/`VARCHAR`/`NCHAR`/`ENUM`/`JSON` value, including a collection element, that is not valid UTF-8 raises `DataError`, the session stays usable, and `execute()` keeps its handle so it is released normally (CUBRID 10.2 can store such values when it truncates an oversized string by bytes). Truncated packets and invalid UTF-8 in protocol framing fields still raise `OperationalError` (column-name text in a complete reply raises `DataError`, #581), as does the prepared API in `compat.native` ([734fb29](https://github.com/cubrid-lab/pycubrid/commit/734fb294276927fa50c6887e99871a82a2c8eebc)). By @yeongseon.
- **Zero `DATE`/`DATETIME`/`TIMESTAMP` values no longer close the connection (#512)** — CUBRID accepts values such as `DATE'0000-00-00'` and the zero `TIMESTAMP`, `TIMESTAMPTZ`, `TIMESTAMPLTZ`, `DATETIMETZ` and `DATETIMELTZ` values, but Python has no year 0. The value now raises `DataError` naming the CUBRID type and fields (`CUBRID DATE value (0, 0, 0) cannot be represented in Python: year 0 is out of range`) and the session stays usable, on `execute()` and on a later fetch page, sync and async; any other temporal field Python cannot hold (such as a `TIME` hour of 25) in a complete reply is reported the same way. A row `DataError` (#492, #413, #512) is raised only after the rest of the reply is checked against its length, so a truncated reply still raises `OperationalError` and closes the connection. The prepared API in `compat.native` stays fail-closed. There is no option to return zero dates as `None` or text; the docs show SQL workarounds (`NULLIF(d, DATE'0000-00-00')`, `CASE`, `TO_CHAR`). Behavior is identical on CUBRID 10.2 to 11.4 (found by the version differential, #351) ([#532](https://github.com/cubrid-lab/pycubrid/pull/532), [5357f66](https://github.com/cubrid-lab/pycubrid/commit/5357f660a39ef048e4d90f06d38e91a47b742bc8)). By @yeongseon.
- **Invalid JSON text in a complete reply raises `DataError` (#543)** — a `JSON` value that is not valid JSON, decoded with the built-in `json.loads`, raised `json.JSONDecodeError`, which the connection layer reported as `OperationalError('malformed response from broker')` and closed the connection. It now raises `DataError` (the `JSONDecodeError` chained as `__cause__`), and the connection and cursor stay usable on `execute()` (`description` is `None`, the handle is still released) and on later fetch pages (rows already collected are kept, #507). A truncated reply around the cell is still `OperationalError`. A caller-supplied `json_deserializer` is not wrapped, and `compat.native` stays fail-closed ([#571](https://github.com/cubrid-lab/pycubrid/pull/571), [c1a5de5](https://github.com/cubrid-lab/pycubrid/commit/c1a5de55db5a5ecca164bb6826e55726dd56f8aa)). By @yeongseon.
- **Unresolvable time zones raise `DataError`; the daylight-saving fold is honored (#413)** — a `TIMESTAMPTZ`/`TIMESTAMPLTZ`/`DATETIMETZ`/`DATETIMELTZ` value whose region the client's IANA time zone database cannot resolve now raises `DataError` naming the zone, with a hint to install `tzdata`, instead of logging a warning per value and returning a naive `datetime`. This mostly affects clients without a time zone database (Windows without `tzdata`, minimal container images), where every region value, including the LTZ types' `UTC`, silently lost its zone. An offset that is malformed or not strictly within ±24 hours also raises `DataError` instead of a closed connection, and the session stays usable; offsets, resolvable regions and an empty zone suffix decode as before. pycubrid now depends on `tzdata` on Windows only (`tzdata; sys_platform == 'win32'`). A region value in the repeated hour when daylight saving time ends honors the abbreviation CUBRID sends (`America/New_York EST` at 2026-11-01 01:30 decodes with `fold=1`, UTC-05:00); a missing, unknown or shared abbreviation keeps `fold=0`. The prepared API stays fail-closed, as in #492 ([180dd56](https://github.com/cubrid-lab/pycubrid/commit/180dd56b1b48c9fb6649285b109d8d7dbfaa752b), [209b201](https://github.com/cubrid-lab/pycubrid/commit/209b20154591783e80dbb5c758ef4629dfc3367d), [#413](https://github.com/cubrid-lab/pycubrid/issues/413), [98d8150](https://github.com/cubrid-lab/pycubrid/commit/98d8150403a525dc27f7d6003d8f5944984cdadc)). By @yeongseon.
- **Hostile time zone callbacks raise `ProgrammingError` (#530)** — datetime parameter timezone callbacks, key lookup and offset-field errors now raise `ProgrammingError("invalid tzinfo on datetime parameter")` with the original exception as cause, without formatting hostile exception text; invalid key values keep their specific error and an offset of `None` still renders naive. Without the C `_datetime` module, temporal and `timedelta` subclasses are rejected before forged fields can be read. Invalid time zone failures stay within DB-API errors ([4fcd9c8](https://github.com/cubrid-lab/pycubrid/commit/4fcd9c82be27e56fcdf1ab8d992c56d1d8b0c994)). Contributed by @yeongseon.
- **Rows fetched before a failing page are no longer lost (#507)** — when a later FETCH page raised a data-level `DataError` (invalid text #492, an unresolved zone #413, a zero date #512), `fetchall()` and `fetchmany()` dropped the rows already collected, and every retry requested the same page again (failing again, or, in autocommit mode after the broker closed the result, raising `DatabaseError` with CAS error `-1012`). The call that reaches the page still raises `DataError`, but the collected rows stay buffered and the next fetch returns them without contacting the server; after that every fetch raises the same `DataError` until `execute()` or `close()`, so no row of or past the failing page is returned. The connection stays usable and the cursor keeps its handle, sync and async; live-tested on CUBRID 11.4 with a zero `DATE` several pages in ([#536](https://github.com/cubrid-lab/pycubrid/pull/536), [ef815f8](https://github.com/cubrid-lab/pycubrid/commit/ef815f853b5167daae016416c05701b4a420332e)). By @yeongseon.
- **`Decimal` parameters use plain fixed-point notation (#517)** — `str(value)` switched to E notation (`Decimal("1E-7")` was sent as `1E-7`), which CUBRID parses as `DOUBLE`, so such values came back as `float` and lost digits in `NUMERIC` columns. They now stay `NUMERIC` with sign, trailing zeros and scale (`Decimal("1E+5")` is sent as `100000`). A `Decimal` whose plain literal needs more than 38 digits (CUBRID's `NUMERIC` maximum precision; leading fractional zeros count), such as `Decimal("1E-39")` or a 39-significant-digit value, raises `DataError` before anything is sent. `NaN`/`Infinity` still raise `ProgrammingError`, integral values written without an exponent (`Decimal("42")`) render as before, and sync and async cursors share the change ([#526](https://github.com/cubrid-lab/pycubrid/issues/526), [a7a3563](https://github.com/cubrid-lab/pycubrid/commit/a7a3563e4ee2fecc06c9208ecbe42392f575749f)). By @yeongseon.
- **Years below 1000 are zero-padded in `DATE`/`DATETIME`/`DATETIMETZ` literals (#519)** — the year was rendered with `strftime("%Y")`, which does not pad on Linux, and CUBRID reads `DATE'99-01-02'` as 1999, so `date(99, 1, 2)` was silently stored as year 1999. Years are now always four digits (`DATE'0099-01-02'`); years 1, 99, 999 and 1000 round-trip on CUBRID 10.2 and 11.4. Contributed by @yeongseon.
- **Reads past the end of a broker reply are rejected (#383, #523, #555)** — a length that ran past the reply was cut short by a Python slice and returned as if complete, a negative length moved the reader backwards, and fixed-width cells (`SHORT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `MONETARY`, `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`, `OBJECT` and the fixed part of the TZ types) ignored their size word. Every length-prefixed read now checks `0 <= length <= remaining`, a decoded collection's elements must fill its declared size, every row cell must use exactly its declared size, and a negative FETCH tuple count, column-metadata length or column count (previously decoded as empty) is rejected. Such a reply raises `OperationalError('malformed response from broker')` and closes the connection, sync and async; `DataError` stays for complete replies (#492, #512), SQL `NULL` cells (non-positive size) and a `LOB_READ` short read (#362) are unchanged, and bytes after the last declared value are still ignored. A normal server never sends such replies ([#533](https://github.com/cubrid-lab/pycubrid/pull/533), [4605671](https://github.com/cubrid-lab/pycubrid/commit/4605671725dce24c895996770ed29d51bb1adb25)) ([#574](https://github.com/cubrid-lab/pycubrid/pull/574), [a341cec](https://github.com/cubrid-lab/pycubrid/commit/a341cec58f36d33b8ae75734b3854619c7287911)). By @yeongseon.
- **A text, metadata or collection error cannot hide a damaged reply tail (#581, #591, #595)** — undecodable column-metadata text, an unrepresentable row value or a failing collection element previously raised `DataError` at once, so later damage in the same reply went unnoticed and the session was kept. The rest of the reply is now validated by its declared lengths first: later structural damage raises `OperationalError` and closes the connection (collection members still run their decoders and the first complete `DataError` is kept), while a complete reply still raises `DataError` and keeps the session. FC41 now rejects negative bind, `total_tuple_count` and inline tuple counts and an impossible column count as FC2 already did, FC3 rejects a negative inline tuple count, and partial inline-fetch headers are rejected. Sync and async behave the same ([#588](https://github.com/cubrid-lab/pycubrid/pull/588), [d19a981](https://github.com/cubrid-lab/pycubrid/commit/d19a9819067262d64f3c0e2f09a009edd7a5fc71)) ([#596](https://github.com/cubrid-lab/pycubrid/pull/596), [62f5866](https://github.com/cubrid-lab/pycubrid/commit/62f5866898818faa9146249712bc40a0fef80f4f)) ([#597](https://github.com/cubrid-lab/pycubrid/pull/597), [1a93ecf](https://github.com/cubrid-lab/pycubrid/commit/1a93ecf6b69012dd8a3a85be9d16a65898d5ec60)). By @yeongseon.
- **Transport failures retire cursor handles; async timeout errors name their cause (#556)** — an uncertain transport failure closed the connection but left cursors holding the dead session's handle ids, and the async invalidation could be skipped when `wait_closed()` failed or was cancelled. Every sync and async socket-error, malformed-reply, timeout, `ping()`, `CHECK_CAS` recovery and session-restore path now retires the connection and all cursor and schema handles first; a `BaseException` such as `KeyboardInterrupt` during a sync reply retires the session too, as async cancellation already did. Buffered rows stay readable and the next required FETCH fails explicitly; nothing is replayed. The async `OperationalError('read timeout')` for any `TimeoutError` is now `read timeout: no complete round trip within read_timeout=<n>s` only when that deadline expired, and `socket communication timed out` for a transport timeout. An `OSError` raised by a `json_deserializer` after the whole reply was read now propagates unchanged and keeps the session, while a `ValueError`-family error (orjson, simplejson) is still treated as a malformed reply ([3d72216](https://github.com/cubrid-lab/pycubrid/commit/3d72216ff807f9a1fe6d7292cf754f44359b2626), [#556](https://github.com/cubrid-lab/pycubrid/issues/556)). Contributed by @yeongseon.
- **Async setup failure no longer leaks into waiting tasks (#554)** — tasks waiting on `AsyncConnection.connect()` setup (including the reconnect of `ping(reconnect=True)`) re-raised the setup owner's exception instance, so cancelling the owner cancelled every waiter. Each waiter now raises a fresh exception: a pycubrid error keeps its class (or the nearest `pycubrid.exceptions` class), `code`, `errno` and `sqlstate` with the original chained as `__cause__`; any other error becomes `OperationalError` naming it by `repr()`, and a cancelled or interrupted setup becomes `OperationalError("connection setup was cancelled or interrupted in another task; retry operation")`. The owner's own exception and a waiter's own cancellation are unchanged ([#578](https://github.com/cubrid-lab/pycubrid/pull/578), [7f5fe22](https://github.com/cubrid-lab/pycubrid/commit/7f5fe229000ebec13bc14427f0dfe277acfb5b09)). By @yeongseon.
- **Autocommit is applied on one CAS session (#551, #520, #521)** — in both drivers `conn.autocommit = v` / `await conn.set_autocommit(v)` could send `SET_DB_PARAMETER` to one CAS session and `COMMIT` to a recycled replacement, so `autocommit` reported the new value while the session had another. The new value is now recorded before the `COMMIT`, so its single reconnect restores it first, a call replaces the session at most once, and a failed `COMMIT` (including a native error) closes the connection, keeps the previous value and raises `OperationalError` with the cause chained, while a rejected `SET_DB_PARAMETER` still raises its native error and keeps the session ([#552](https://github.com/cubrid-lab/pycubrid/pull/552), [7ac5a3e](https://github.com/cubrid-lab/pycubrid/commit/7ac5a3e528efb97516f638d607e5f3547fc3d69b)). Related fixes found by the sync/async replay parity suite (#521): sync `Connection.connect()` after `close()` re-applies an explicitly set `autocommit` on the new session, including `ping(reconnect=True)` and the reconnect after a failed `CHECK_CAS` (#520); sync `connect(autocommit=True)` sends both requests on the session it just opened, as `pycubrid.aio` does, and failures now close the connection and raise `OperationalError` instead of leaking a native `DatabaseError` with an open socket; both drivers send `CHECK_CAS` before applying autocommit to an `OUT_TRAN` session after the escape probe's `ROLLBACK`; and sync `ping(reconnect=False)` closes a session whose `CHECK_CAS` failed (still returning `False`, later calls raise `InterfaceError` until `connect()` or `ping(reconnect=True)`), like async. Contributed by @yeongseon.
- **Freed and deferred cursor handles are never reused for another cursor (#584, #601)** — with statement pooling off, direct CUBRID CAS reuses handle ids after an automatic transaction end, so sync and async drivers now invalidate cursor/schema ownership on the actual `OUT_TRAN` reply; buffered rows and completed results stay available, and unfinished results fail explicitly instead of closing or fetching another cursor's reused id ([#598](https://github.com/cubrid-lab/pycubrid/pull/598), [1251165](https://github.com/cubrid-lab/pycubrid/commit/125116574a7eeb05c942bcc24563be28eef9ee2b)). A caller interrupt during the deferred-`CLOSE_REQ` flush of `commit()`/`rollback()` now keeps unsent same-session ids in FIFO order (completed or uncertain sends are not replayed, stale-session ids are discarded) ([1686920](https://github.com/cubrid-lab/pycubrid/commit/16869201ca04a8b9e0b4ee5e73cb996bb38c0625)). Contributed by @yeongseon.
- **Repeated native prepared execution errors keep the real server code (#611)** — after a complete broker error, the opt-in sync prepared cursor returns that failure unchanged, and only the next explicit `execute()` closes and re-prepares a non-LOB statement on the same session. A repeated bad `SET(INTEGER)` value now surfaces conversion `-494` instead of a stale-plan `-1024`, without replaying a possibly effective statement inside its failing call. Temporary LOB bindings require an explicit reprepare and rebind ([e39e00e](https://github.com/cubrid-lab/pycubrid/commit/e39e00e8e61b2c04f3e8250a7b82cde1a3f6a82a)). Contributed by @yeongseon.
- **TLS handshake bounds and the Python 3.10 preflight probe (#535, #592, #593)** — sync `connect(..., ssl=...)` without `read_timeout` waited forever when the broker or a proxy stalled during the TLS handshake. It now gives up after 10 seconds with `OperationalError` (the async driver's `ssl_handshake_timeout` default); to allow longer, set a larger `read_timeout` such as `30.0`, which also sets later request read timeouts. With `read_timeout` set nothing changes; `connect_timeout` still bounds only the TCP connect, and without `read_timeout` the socket returns to blocking after the handshake, so later requests stay unbounded as before. On Python 3.10, the async driver's certificate preflight probe now handshakes over memory BIOs on a socket it owns, so a broker reset before the ClientHello no longer causes a `ResourceWarning`; all probe I/O shares one deadline (best-effort `close_notify` cannot extend it, #593), queued fatal alerts are sent best effort within it and the original TLS error is re-raised, and failures sending the final handshake flight propagate. The same CPython 3.10 `ssl` behavior can still affect the sync driver's `wrap_socket()` upgrade (see `docs/CONNECTION.md`) ([#586](https://github.com/cubrid-lab/pycubrid/pull/586), [3af12ae](https://github.com/cubrid-lab/pycubrid/commit/3af12ae7d97ea6e1c5e30e616babc0f54fc7ddd0)) ([fa7b22a](https://github.com/cubrid-lab/pycubrid/commit/fa7b22ad4707510830d8cccb1518c3a875d1c330), [#594](https://github.com/cubrid-lab/pycubrid/issues/594), [7dddab3](https://github.com/cubrid-lab/pycubrid/commit/7dddab31c22f8bb8b837c30d6e942d483abe5f10), [#593](https://github.com/cubrid-lab/pycubrid/issues/593)). Contributed by @yeongseon.
- **`pycubrid.aio` TLS connect and close no longer hang or warn (#513, #514)** — if the broker stalled or reset the connection before the TLS handshake completed, connect cleanup awaited `StreamWriter.wait_closed()` on a stream asyncio never marks closed, so the call hung forever on Python 3.11+. The failed upgrade now notifies the stream protocol, and connect raises `OperationalError` within `read_timeout` (or the 10-second `ssl_handshake_timeout` when it is unset) and closes the socket; the sync driver was not affected ([#534](https://github.com/cubrid-lab/pycubrid/pull/534), [bd878fe](https://github.com/cubrid-lab/pycubrid/commit/bd878fe1af7b4e0eb5e76e47c9da3407d5394944)). After the in-place `loop.start_tls()` upgrade the stream protocol also believed it was plaintext, so every TLS peer close logged `WARNING returning true from eof_received() has no effect when using ssl`; it now knows it runs over TLS. Log output only. By @yeongseon.

### Security

- **`str`, `bytes`, date and time parameters are rendered without calling overridable methods (#528)** — `format_parameter()` escaped `str` with `value.replace(...)`, rendered bytes with `value.hex()` and dates and times with `value.strftime(...)`, all of which a subclass can override, and spliced `tzinfo.key` into `DATETIMETZ` literals unescaped. A `str` subclass whose `replace()` returned `x'; DROP TABLE users; --`, an overridden `hex()`/`strftime()` or a `tzinfo.key` containing `'` injected SQL. A `str` subclass is now copied to a plain `str` before the NUL/Ctrl-Z checks and escaping, bytes are rendered with `bytes.hex(value)`/`bytearray.hex(value)`, and date/time literals are built from integer fields read through the base-class descriptors. A non-empty `tzinfo.key` must be a plain `str` matching `[A-Za-z0-9_+/-]+` (every IANA name does), otherwise `ProgrammingError`. Parameters are dispatched on `type(value)`, so an object that only claims a supported type through `__class__` (including transparent proxies) raises `ProgrammingError("unsupported parameter type")`; `escape_string()` raises `ProgrammingError` for a non-`str`; and without the C `decimal` module, `Decimal` subclasses raise `ProgrammingError`. Output for plain values is byte-identical apart from the year padding (#519), and sync and async cursors share the change ([#529](https://github.com/cubrid-lab/pycubrid/pull/529), [dde9a07](https://github.com/cubrid-lab/pycubrid/commit/dde9a071f44ae275a5db486dea8ecf1521889761)). Contributed by @yeongseon.
- **`int`, `float` and `Decimal` subclasses are bound by value (#518)** — `format_parameter()` rendered them with `str(value)`/`format(value, "f")`, which a subclass can override: a `__str__` returning `1; DROP TABLE t` was sent verbatim, and on Python 3.10 `enum.IntEnum`/`enum.IntFlag` members were sent as `Color.RED` / `Perm.R|W`. Values are now rendered through the base-class methods, so `Color.RED` is sent as `1` and `Perm.R | Perm.W` as `6`. Plain values are unchanged, `bool` still renders as `1`/`0`, and sync and async cursors share the change ([#527](https://github.com/cubrid-lab/pycubrid/pull/527), [0577cd8](https://github.com/cubrid-lab/pycubrid/commit/0577cd82e1de3858b3694b0105f3f13c622082f9)). Contributed by @yeongseon.

### Performance

- **Unclosed autocommit cursors no longer exhaust server handles (#488)** — in autocommit mode only `commit()`/`rollback()` sent `CLOSE_REQ` for unclosed cursors, so on CUBRID 11.4.6 (statement pooling on) 5000 unclosed SELECTs pushed handle ids past 1100 and forced 4-5 CAS memory restarts per run (each silently resets SQL-set session state). Connections now track cursors weakly. When the broker reports statement pooling, an autocommit `close()` or re-`execute()`, and in any mode a cursor collected without `close()`, queue the handle id, and the next `PREPARE_AND_EXECUTE` carries it so CAS frees it before preparing (the deferred close JDBC uses). Measured live, sync and async: 5000 unclosed SELECTs keep the handle id at 1-2 with no restart, a SELECT then `close()` takes 2 requests instead of 4, and a reused cursor's SELECT then INSERT takes 5 requests instead of 8. One statement carries at most 256 ids (an explicit release with 256 queued sends `CLOSE_REQ` at once), `commit()`/`rollback()` still close every queued id, and the queue belongs to one physical session and is dropped when it is retired or replaced; a request with queued ids is never replayed. Without statement pooling CAS frees handles at every commit, so `CLOSE_REQ` is still sent at once, and a shard proxy keeps immediate `CLOSE_REQ` ([fe222fb](https://github.com/cubrid-lab/pycubrid/commit/fe222fb51ebb6fccf69f4d88941a1ecff9d82479), [#488](https://github.com/cubrid-lab/pycubrid/issues/488)). By @yeongseon.
- **Faster FETCH row parsing (#559)** — the SET conversion now runs only for SET columns and each cell's size word is read inline. Median parse time drops 13–23% for scalar, text, mixed and raw-collection replies and 5–6% for decoded collections (CPython 3.10, two runs), with unchanged allocation. A differential run over 10,490 seed, truncated and byte-mutated replies gave identical rows and exceptions ([#602](https://github.com/cubrid-lab/pycubrid/pull/602), [cfecfba](https://github.com/cubrid-lab/pycubrid/commit/cfecfba3cc1251b38465669427352579691e3b17)). Contributed by @yeongseon.

### Documentation

- **Behavior contracts and type notes** — the CAS session-loss contract is documented: `OperationalError` for an incomplete CAS reply, session retirement, and explicit recovery without statement replay; CUBRID's collection/NUMERIC `IF` SIGSEGV remains unfixed and its upstream report is tracked separately in #675 (#614) ([#676](https://github.com/cubrid-lab/pycubrid/pull/676), [14f7398](https://github.com/cubrid-lab/pycubrid/commit/14f739866040c1bb9efc47d7426b50661007f3bd)). The renewed CAS error-code evaluation (#505) is documented as numeric-code-first dispatch with an inner-code `-1`-only text fallback and raw `code`/`errno` preserved; the flag-zero compatibility contract is kept because legacy CAS and engine numbers overlap ([#656](https://github.com/cubrid-lab/pycubrid/pull/656), [eed27ab](https://github.com/cubrid-lab/pycubrid/commit/eed27abf8f4d350b84baaa6d7562812d90fc0aff)). `docs/TYPES.md` (English and Korean) now says a bound `datetime.time` silently loses its microseconds and `tzinfo` (CUBRID `TIME` has second precision, and the driver keeps truncating rather than raising `DataError`) ([#670](https://github.com/cubrid-lab/pycubrid/issues/670), [8332517](https://github.com/cubrid-lab/pycubrid/commit/833251756ff92c1300d59fde59e2b9015776054d)), and notes that LTZ values follow the session time zone kept across commit, scoped to a retained CAS session ([2f47b69](https://github.com/cubrid-lab/pycubrid/commit/2f47b699ddae5274b4a3e2557cb1b263fca7b274), [d4b370a](https://github.com/cubrid-lab/pycubrid/commit/d4b370aed70563ab898a701b970d307d18d114a9), [8814081](https://github.com/cubrid-lab/pycubrid/commit/881408120342f51868805fc61563ab9d9a4a27f1)). Contributed by @yeongseon.
- **`llms.txt` and verification guidance (#414, #415)** — the root `llms.txt` no longer claims prepared statements or a `Cursor.prepare()` method that ordinary cursors do not have, and `docs/llms.txt` is the only maintained index (driver-side literal binding, the opt-in sync-only `pycubrid.compat.native` prepared subset, the full PEP 249 exception list and `cubrid-cookbook-python` links); `scripts/generate_llms_full.py` copies it byte-for-byte to the root file, and CI `lint` fails when `docs/llms-full.txt` or `llms.txt` is stale or deleted ([6039dbe](https://github.com/cubrid-lab/pycubrid/commit/6039dbec69f88091578083011d8a93924317b8b6), [8957eaf](https://github.com/cubrid-lab/pycubrid/commit/8957eaf6e6c92e127f0768af72df219200cb544d), [#414](https://github.com/cubrid-lab/pycubrid/issues/414), [48aeefc](https://github.com/cubrid-lab/pycubrid/commit/48aeefc4ea049d7a764ca8cdd01e5cdb1cf7dc06)). `docs/SUPPORT_MATRIX.md` and `docs/TROUBLESHOOTING.md` (+ Korean) no longer call `cursor.execute(sql, params)` server-side `PREPARE_AND_EXECUTE` binding, the support matrix notes that `nextset()` raises `NotSupportedError`, and the Korean, German, Hindi, Russian and Chinese READMEs describe driver-side binding. Volatile version and test/coverage counts were replaced with references to source, measured coverage and CI policy (the 95% coverage floor is unchanged) ([#654](https://github.com/cubrid-lab/pycubrid/pull/654), [127c6a1](https://github.com/cubrid-lab/pycubrid/commit/127c6a1f5bcf67eeefbcd6623a577887745b304e)). Contributed by @yeongseon.
- **Examples, walkthroughs and Korean guides** — runnable README Quick Start examples (parameterized CRUD, commit/rollback and `ProgrammingError`, mirrored in the five maintained translations and verified on CUBRID 11.4) (#327) ([#677](https://github.com/cubrid-lab/pycubrid/pull/677), [518aa8b](https://github.com/cubrid-lab/pycubrid/commit/518aa8be4e2209b05e1a0c0faf68f9f3b7853df0)); a recorded sync/async query run and a Quick Start CRUD/context-manager video replace the illustrative README GIF, with editable VHS tapes and source-wheel provenance (#320) ([#665](https://github.com/cubrid-lab/pycubrid/pull/665), [a44e250](https://github.com/cubrid-lab/pycubrid/commit/a44e2502568a9492017a4655402eefd10bf07b0a)); the Korean README links the Korean performance guide `docs/ko/PERFORMANCE.md` with working anchors (#313) ([5b394b7](https://github.com/cubrid-lab/pycubrid/commit/5b394b70fd178fc529d2482e45faa91ac20f0384)); and a Korean contribution guide (#329) ([#663](https://github.com/cubrid-lab/pycubrid/pull/663), [ac89eb3](https://github.com/cubrid-lab/pycubrid/commit/ac89eb36fefd8c2b6a61b0b3cb11ed6dc9615440)). No driver behavior, API or support change. Contributed by @yeongseon.
- **Security, support and release process documentation** — `SECURITY.md` is aligned with latest-minor support and the Python 3.10 TLS preflight guidance, keeping disclosure instructions (#422, #649) ([#649](https://github.com/cubrid-lab/pycubrid/issues/649), [ac63fd3](https://github.com/cubrid-lab/pycubrid/commit/ac63fd38fc946b6ad3cfab91333784cafc76a45f)) by @yeongseon, co-authored by @chenzeyan54-commits; the reviewed roadmap backlog is reconciled with release, verification and compatibility work (#566) ([#680](https://github.com/cubrid-lab/pycubrid/pull/680), [4a9c598](https://github.com/cubrid-lab/pycubrid/commit/4a9c598555f2d3daa42e0055d64c9dbcf43d1e24)); the 1.9.0 upgrade notes mark new compat names provisional (#671, closes #396) ([#671](https://github.com/cubrid-lab/pycubrid/issues/671), [7fba209](https://github.com/cubrid-lab/pycubrid/commit/7fba209adb241e7873bbc2671299eb516d73a21e), [#396](https://github.com/cubrid-lab/pycubrid/issues/396)); and the contribution guide on issue specifications and owners, a pre-commit warning that hooks need an active `.[dev]` environment, release-please review and recovery, the `create-release` recovery command with idempotent SBOM re-upload, a cookbook smoke-test fallback pinned to the exact release, and the changelog/release-policy/protocol (English and Korean) updates are refreshed ([ea32de4](https://github.com/cubrid-lab/pycubrid/commit/ea32de472b1319182af73826af52ecdd3917be99), [acb4caa](https://github.com/cubrid-lab/pycubrid/commit/acb4caac56c2cbd479471f5a06124040be4ac29d), [fc922fe](https://github.com/cubrid-lab/pycubrid/commit/fc922fe1ebee46696124c12d2b5dad6a68ca64ef), [9784b0a](https://github.com/cubrid-lab/pycubrid/commit/9784b0a9677172a5fb1346d6f3011729c1640800), [dd621c4](https://github.com/cubrid-lab/pycubrid/commit/dd621c427b883b7b987563049ce8d398f35ee2ed), [330682c](https://github.com/cubrid-lab/pycubrid/commit/330682c6d1c18900a0a81dd6a3133dc90bfe7d4a), [a341cec](https://github.com/cubrid-lab/pycubrid/commit/a341cec58f36d33b8ae75734b3854619c7287911)). Contributed by @yeongseon.

### CI

- **Releases are published from a reviewed release PR (#539, #544, #494)** — `prepare-release.yml` opens the `chore: release vX.Y.Z` PR (dated CHANGELOG section, version bump, `make release-check`). On every push to `main`, a guarded workflow decides from git facts only (`scripts/release_detect.py`) and then runs, pinned to the merge SHA: release check, the full `integration-full.yml` matrix, one build with SHA-256 hashes, the annotated tag, a draft GitHub Release with SBOM, the PyPI upload and the cookbook verification of that exact version, which must report the requested version installed (the smoke test runs as a reusable workflow pinned to a cookbook commit, so no cross-repository token is needed). `make release` is replaced by the read-only `make release-check VERSION=x.y.z`; `create-release.yml` and the old manual publisher are removed, leaving a narrow recovery dispatch (`resume`, `verify-only`, `dry-run`). The workflow is named `publish-pypi.yml` to match the registered PyPI Trusted Publisher identity (environment `pypi`), correcting the `invalid-publisher` filename mismatch. The PyPI upload no longer passes `skip-existing: true`: `scripts/pypi_duplicate_guard.py` compares the SHA-256 of every file with the one PyPI already serves, drops an identical file (a partial upload recovered with `gh run rerun --failed`) and fails on a different hash or an unreachable PyPI. The release PR preparer is replaced by pinned release-please, which keeps curated Upgrade notes beside generated notes, and reviewed candidates are frozen before editing; a skipped cookbook verification is reported as failed in the release summary (#548) ([#548](https://github.com/cubrid-lab/pycubrid/issues/548), [45f3374](https://github.com/cubrid-lab/pycubrid/commit/45f3374b4cfa9d191316c935b3801e827aa94b1b)), and the changelog linter rejects duplicate release subsections (#648) ([#648](https://github.com/cubrid-lab/pycubrid/issues/648), [3c57bb9](https://github.com/cubrid-lab/pycubrid/commit/3c57bb9ffab347f5c57e27d080d4a9b205c64083)) (co-authored by @chenzeyan54-commits). Contributed by @yeongseon.
- **Reverted failed 1.9.0 publication attempt (#672)** — the earlier release PR merged before stabilization finished, and PyPI rejected publishing because the then-used workflow was not a registered Trusted Publisher. The revert restored the version and release manifest to 1.8.0 and returned the candidate notes to Unreleased; it did **not** publish 1.9.0 ([#672](https://github.com/cubrid-lab/pycubrid/issues/672), [cf2fe9d](https://github.com/cubrid-lab/pycubrid/commit/cf2fe9d474a02a56234e13c5992fbcd8e5c362a1)) by @yeongseon.
- **Cheaper routine CI** — routine PR CI runs one representative offline lane plus targeted live checks, with full compatibility matrices moved to explicit dispatch, releases and weekly representative checks; the repository-tooling lane is selected when official-fixture or scenario-ledger tests change (#651). A manual-only Ubuntu offline and wheel/sdist installation lane prepares for Python 3.15, which is not yet officially supported; final-runtime and live CUBRID evidence are required before promotion, and routine PR frequency and supported versions are unchanged. Repository policy/tooling tests (15 modules marked `repo_tooling`, #558) run in a separate required `repo-tooling-tests` job (ubuntu and macos) instead of the offline matrix, which dropped from 79.9s to 50.2s locally with the same 2,976 passing tests and every check kept. `cache: pip` on the 10 `setup-python` steps cuts repeated downloads (warm same-head run 295s versus 306s baseline, within runner variance), and `scripts/wait_for_cubrid.py` changes now select the integration and official-differential lanes (#564). Ruff and Mypy pre-commit hooks are `repo: local` hooks that run from the active `.[dev]` environment, so the `pyproject.toml` dev pin is the single version source and Dependabot bumps no longer fail the quality-tool gate (#476). Contributed by @yeongseon.
- **Official-driver differential gate (#446)** — official behavior that pycubrid claims is listed in `tests/fixtures/official_differential_claims.json` (each a `match`, or a `deviation` with reason, issue and both drivers' observations; the first 18 claims are the #344 stored-type fetches, a static scalar row and the #439 prepared INT/string subset, with classified deviations for MONETARY, description size/null_ok and native `bind_param(None)`). `tests/test_official_differential.py` runs one live case per claim through pycubrid and the official driver built by `scripts/build_official_oracle.py` from pinned cubrid-python `e75ec36` and CCI `7d1eb8f`. A new required `official-differential` job (Python 3.10, CUBRID 10.2 and 11.4; skipped only for docs-only changes) is part of the CI Gate and the nightly/release matrix, and a missing driver, zero cases, any skip, a mismatch or an unclassified divergence fails it. Contributed by @yeongseon.
- **Nightly and diagnostic lanes (#356, #655, #612)** — `bug-hunt.yml` runs three isolated, advisory CUBRID 11.4 downstream lanes against the exact pycubrid commit (SQLAlchemy ORM and pool tests, MCP live tool tests including a no-skip concurrency check, and Cookbook agent/worker tests) with recorded commits and evidence uploaded on failure (#356). The property job passes its xunit1 reports and a readiness sidecar to a bounded, credential-redacted failure-context collector (#655). The nightly mutation lane migrated to mutmut 3 (`mutmut>=3.8,<4`, TOML-array config) after 14 consecutive crashed runs from 2026-09-18 through 2026-10-01; the first measurement on Python 3.12 killed 5509 of 6878 mutants (80.1%) in about 58 minutes, and the job's failure policy is unchanged (#612) by @paikend. By @yeongseon.
- **Integration cleanup and startup (#501, #647)** — automatic integration/TLS cleanup preserves existing Docker volumes, including after readiness and test failures ([#647](https://github.com/cubrid-lab/pycubrid/issues/647), [f3b2e69](https://github.com/cubrid-lab/pycubrid/commit/f3b2e6983e62ae3d5d72149188d3357de96e5445)) by @yeongseon, co-authored by @HarshRajSinghania; fixed Docker startup sleeps are replaced by readiness waits ([8154952](https://github.com/cubrid-lab/pycubrid/commit/8154952a475a772aca7dae96505d52b02eee551f)) by @nightcityblade (first contribution 🎉).

### Tests

- **Offline sync/async parity and request budgets (#521, #557, #585, #563)** — `tests/test_replay_parity.py` replays scripted broker replies through a real sync `Connection` and `AsyncConnection` against in-process multi-session brokers and compares outcomes, exact requests, usability and session counts across connect/close, reconnect, autocommit restore, handle invalidation, the `OUT_TRAN` `CHECK_CAS` probe, failed pings, malformed replies and the `DataError` contracts (#512, #536); it found the four unintended differences fixed above. Seven exact round-trip budget scenarios (#557) fail on a dropped safety request such as `CHECK_CAS` as well as on an added round trip, and are the baseline that later round-trip work (#419, #488, #525) must not silently regress. Real TCP replay checks cover the deferred-close flush for sync and async commit and rollback (#585), and one pure-function golden matrix now covers scalar formatting under both escape modes (#563). No production behavior changes. Contributed by @yeongseon.
- **Protocol fuzzing and integration fail-closed behavior (#523, #522, #432)** — fuzz seeds now include realistic `PREPARE_AND_EXECUTE`, `PREPARE`, `EXECUTE` and FETCH replies with metadata for string, numeric, temporal, TZ, `BIT`, OID, collection, LOB and `JSON` columns, and the oracle admits only structural `OperationalError`, server errors and `DataError` (#523). A configured but unreachable CUBRID now errors instead of silently skipping hundreds of integration tests: one gate in `tests/conftest.py` skips only when neither `CUBRID_TEST_URL` nor `CUBRID_TEST_HOST` is set, every module resolves the endpoint through `tests/_cubrid_endpoint.py` (a URL naming another host or port is no longer silently ignored), and `make integration` waits with `wait_for_cubrid.py`, fails an all-skipped JUnit run, accepts `CUBRID_TEST_PORT=<port>` and always removes the container, as does `make integration-tls` (#522, #432) ([#541](https://github.com/cubrid-lab/pycubrid/pull/541), [68851c9](https://github.com/cubrid-lab/pycubrid/commit/68851c9aa9b125496250f7fff2ae6c4a3821f2b5)). Contributed by @yeongseon.
- **Version differential no longer fails on a server crash (#614)** — `SELECT IF(1=0, SET{1}, 0.000)` ends `csql` with SIGSEGV on CUBRID 10.2.18, 11.0.16, 11.2.9 and 11.4.6 (original CI observed CAS session loss on 10.2 only); the expression grammar drew a collection branch beside a NUMERIC branch of scale 2 or more in 75% of 1000-example runs, so the release-gate matrix failed on a server defect. `IF` and `CASE WHEN` now leave that pairing out (only `IF` was measured; not validated against live servers in this change; upstream report tracked in #675) by @yeongseon. A fatal statement now fails one test and `compare()` names every affected version instead of cascading and reporting only the first endpoint to die, with offline tests for the isolation behavior by @paikend.
- **Fixture and docs-check fixes (#429, #524, #605)** — the backslash-escape-mode pin is opt-out by an explicit `pytest.mark.no_escape_pin` marker instead of a filename guess, which found further live-connection modules that were silently pinned instead of negotiating (#524); offline tests pin the mode to the server default; missing-timezone fixtures also hide cached `tzdata.*` modules (#605); `tests/test_docs_reason.py` runs the docs-sync script in-process (about 8.7s down to under a second) and `scripts/check_docs_reason.py` no longer accepts a reason whose only content is empty emphasis around an invisible comment, one of four cases reported against the final head of #425 and still reproducible, with the same follow-up needed in sqlalchemy-cubrid and cubrid-cookbook-python (#429). Contributed by @yeongseon.

## [1.8.0] - 2026-09-29

This release improves transaction/session behavior, result safety and the opt-in official-driver compatibility surface. **Review the upgrade notes before deploying:** exception types, nullable metadata, collection decoding, unfinished SELECT behavior and session time-zone retention may change application-visible results; reconnect does not replay SQL or guarantee session state preservation.

### Upgrade notes

Behavior changes you may notice (details in the entries below):
- Native NOT NULL (`-631`) and invalid foreign-key (`-922`) violations now raise
  `IntegrityError` (SQLSTATE `23000`, still a `DatabaseError` subclass) instead
  of a generic `DatabaseError`. (#390)
- Fetching from an unfinished SELECT result after `commit()`/`rollback()`
  invalidated its handle now raises `InterfaceError` instead of silently
  returning a partial result as if exhausted. Already received rows remain
  readable; no replay or holdable-result guarantee is added. (#395)
- `cursor.description` `null_ok` was inverted and is now correct: `True` for
  nullable columns, `False` for NOT NULL/primary-key columns. (#431)
- SET/MULTISET/SEQUENCE columns report their collection type codes and decode
  as collections with `decode_collections=True` (raw bytes when disabled). (#430)
- Normal `commit()`, `rollback()` and autocommit requests keep the same CAS
  session, so isolation level and session variables survive transaction
  boundaries. (#468, #472)
- A session time zone set with `SET TIME ZONE` now also survives
  `commit()`/`rollback()`. On 1.7.x a transaction boundary could transparently
  reconnect and silently fall back to the server default zone, so
  `DATETIMELTZ`/`TIMESTAMPLTZ` values read after a commit came back in that
  zone (often `+00:00`). On 1.8.0 they come back in the session zone you set:
  the same instant with a different UTC offset. Compare instants rather than
  offsets or wall-clock fields if your code or expected output relied on the
  old offset. If the CAS itself closes the socket, the zone is lost like other
  SQL session state (see the next note). (#468, #472)
- If the CAS closed the socket after a transaction boundary (CAS restart,
  CHANGE CLIENT, `cubrid broker reset`), the driver probes with `CHECK_CAS`
  and reconnects once before the next request. Driver-owned settings (escape
  mode unless pinned, explicit autocommit) are restored; session state set with
  SQL (isolation, session variables) is not, so re-apply it. SQL bound for a
  replaced session is never sent to the new one; a retryable `OperationalError`
  is raised instead. Requests after a boundary cost one extra round trip. (#485)
- `commit()`/`rollback()` now close server handles held by unclosed cursors. In
  autocommit mode there is no such boundary: close cursors yourself, or their
  handles stay open until commit/rollback/close. (#485)
- `get_last_insert_id()` returns `None` instead of `""` when no identity is
  available; replace `value == ""` with `value is None` and check for `None`
  before `int(value)`. (#381)
- Unknown connection keyword arguments now emit
  `pycubrid.UnknownConnectionOptionWarning` (still ignored otherwise). Use
  `warnings.simplefilter("error", pycubrid.UnknownConnectionOptionWarning)` to
  reject them; `"ignore"` silences them. (#377)

New opt-in compatibility factories, a prepared scalar cursor and schema-info
methods are additive; ordinary 1.x connect/cursor behavior is unchanged (see
Added).

### Added

- **Official-driver compatibility factories (#465)** — explicit
  `pycubrid.compat.native` and `pycubrid.compat.cubriddb` namespaces validate
  CUBRID/UTF-8 DSNs, preserve the source's public/empty credential defaults and
  start one owned pure-Python sync connection with autocommit enabled. Wrapper
  aliases and close are available; cursor execution (`compat.cubriddb` has none),
  prepared binding, sharing, configurable charset and HA are not. Not full
  official-driver/DB-API parity. Ordinary 1.x defaults and async behavior are
  unchanged. MINOR/additive public surface, protected by the API baseline.
  Contributed by @yeongseon.

- **`compat.native` prepared scalar cursor (#439)** — a sync cursor where one
  physical-session-owned FC2 handle supports repeated typed FC3 execution of
  INT32, UTF-8 CHAR and SQL NULL, tuple-only row fetch, current-generation FC6
  close, and connection commit/rollback result hooks. Pooling-off/unknown
  sessions fail before FC2. HOLDABLE SELECT results continue across commit and
  invalidate across rollback; ordinary sync/async FC41 execution is unchanged and
  there is no async preparation. Live scalar, DML and 130-row multi-FETCH gates
  pass on CUBRID 10.2, 11.0, 11.2 and 11.4. Pinned official-native comparisons
  on 10.2/11.4 match the selected non-NULL scalar/DML results; native
  `bind_param(None)` crashes and is a documented safety deviation, not a NULL
  parity pass. This is an additive MINOR subset, not full native/DB-API parity,
  public async preparation, effective settings, or a release. Supporting work
  that shipped alongside it and changes nothing for ordinary use: FC2/FC3
  scalar packet serialization and refreshed-column-metadata parsing, with error
  records failing closed (#475), and fencing prepared FC3/FC6 requests to their
  owning physical CAS generation, so a stale handle is rejected before send even
  if a replacement server reuses its number and an uncertain post-send failure
  retires the session without replay (#478); the declared `threadsafety=1`
  contract is unchanged. Contributed by @yeongseon.

- **Owned schema rows (#456, #457)** — sync/async eager
  `Connection.fetch_schema_info(packet)` and idempotent `close_schema_info(packet)`
  (corrected FC9 requests/condensed metadata). Existing getter positional
  arguments and raw packet fields remain; keyword-only `arg2=None` adds the
  second filter. Immutable original-session ownership prevents forged/retired
  handle RPCs. Handles are closed at explicit transaction boundaries, at
  auto-committing cursor statements/batches and at version lookup with
  connection autocommit enabled; connection teardown and I/O failures retire
  them. Schema FETCH/CLOSE never reconnect, replay, implicitly commit or return
  partial rows as success. Live coverage on CUBRID 10.2 and 11.4 spans
  CLASS/VCLASS/ATTRIBUTE/CONSTRAINT/PRIMARY_KEY/IMPORTED_KEYS/EXPORTED_KEYS with
  real multi-FETCH/close evidence in both sync and async modes, not all schema
  codes or native parity. Schema examples now consume/close results and supply
  ATTRIBUTE's second filter; row-order, qualifier and index-family boundaries are
  documented. MINOR/additive surface; API baseline regenerated. Contributed by
  @yeongseon.

- **Unknown connection options are now surfaced instead of silently ignored (#377)** — `Connection.__init__`/`AsyncConnection.__init__` read a fixed set of options out of `**kwargs` and discarded everything else, so a typo such as `read_timout=30` or `connectTimeout=5` was accepted, had no effect, and gave no signal. Any keyword outside the supported set now emits `pycubrid.UnknownConnectionOptionWarning` (a `UserWarning` subclass, **not** part of the PEP 249 exception hierarchy) naming the option, suggesting the closest supported spelling when there is one, and listing the supported set. Known options behave exactly as before, and the warning is emitted before any socket work, so it is reported even when the connection then fails. It covers `pycubrid.connect()`, `pycubrid.aio.connect()` and direct `Connection(...)`/`AsyncConnection(...)` construction, and points at the caller's own line. Contributed by @Parth-Vasave (first contribution 🎉).

  A warning rather than a hard `TypeError` is deliberate: wrapper layers (connection pools, ORM dialects such as `sqlalchemy-cubrid`) legitimately forward extra keywords, so rejecting them would be a breaking change under `RELEASE_POLICY.md` §3 and cannot land on the 1.x line. Additive surface change (`api-baseline.json` regenerated).

### Fixed

- **Sessions survive transaction boundaries, and a CAS that closes the socket is recovered (#468, #485)** — `CAS_INFO[0]=0` is now treated as OUT_TRAN, not a released CAS session, so normal commit, rollback and autocommit requests keep the physical connection and session state. The CAS can still close the socket right after an OUT_TRAN reply (CAS memory restart at `APPL_SERVER_MAX_SIZE`, `cubrid broker reset`, CHANGE CLIENT with more clients than CAS processes), which used to fail the next request with `OperationalError: connection lost during receive`. Sync and async connections now send one `CHECK_CAS` before a request that follows an OUT_TRAN reply, like JDBC `checkReconnect`; only a failed probe replaces the session, once per request and before that request is first sent. No SQL is replayed, and requests tied to the lost session are not sent to the new one: a CLOSE_REQ is skipped; FETCH, last-insert-id, LOB read/write and native prepared requests fail; a `lastrowid` lost after an autocommit INSERT is `None` and logged at WARNING; cursors probe before rendering parameters, so SQL rendered for a replaced session is rejected with the retryable `OperationalError`; async FETCH/CLOSE_REQ whose handle another task's boundary released are no longer sent. Layers that set session state with SQL must keep re-applying it on a new session (sqlalchemy-cubrid#527). A failed replacement raises `OperationalError` and leaves the connection disconnected for `ping(reconnect=True)`; `ping(reconnect=False)` still reports failure without reconnecting, and `ping(reconnect=True)` also recovers from a disconnected socket or a failed `CHECK_CAS`. Commit and rollback now close the server handles of unclosed cursors, so handles no longer accumulate until the CAS exceeds its memory limit. This supersedes the earlier statement that only an explicit `ping(reconnect=True)` may recover. Contributed by @yeongseon.

- Re-probe automatically detected `no_backslash_escapes` on each new physical
  session, including explicit ping recovery (#471). Explicit `True`/`False`
  remains pinned; healthy same-session ping does not probe. Probe failure
  retires the replacement and returns `False` from ping, while direct connect
  raises. Async parameterized SQL bound against an older session generation is
  rejected before send, not silently rebound or replayed. PATCH correction;
  no dynamic `SET` or heterogeneous-failover guarantee. This supersedes the
  historical #264 note that recovery never re-probes. Contributed by @yeongseon.

- Async schema FETCH now discards the session without sending CLOSE when
  `KeyboardInterrupt` or `SystemExit` interrupts a pending reply; the original
  interruption is preserved. Ordinary FETCH errors retain their cleanup behavior. Contributed by @yeongseon.

- Unfinished SELECT results invalidated by commit/rollback no longer silently look exhausted: sync and async fetch methods raise `InterfaceError` when another broker FETCH is required without a valid handle. Fully buffered/exhausted results retain normal EOF, and the reconnect-specific `OperationalError` stays distinct. (#395) by @yeongseon.

- Native NOT NULL (`-631`) and invalid foreign-key (`-922`) errors raise `IntegrityError` by error code, independent of message language. Single-statement and batch paths preserve the native value in `code` and `errno`; the shared batch error helper no longer drops errno. (#390) by @yeongseon.

- Native syntax (`-493`), semantic (`-494`) and communication (`-671`) errors now carry their verified meanings: generic `ProgrammingError` / `42000` for parser errors, `OperationalError` / `08S01` for communication. Batch dispatch reuses known-code SQLSTATE lookup rather than discarding it in favor of a class default; unknown-code defaults are unchanged. Missing-table inference from `-493` alone is unsupported. (#391) by @yeongseon.

- `cursor.description` reports `null_ok=True` for nullable columns and `False` for NOT NULL/primary-key columns; the CAS byte is an `is_non_null` flag that was previously interpreted backwards. Size fields and collection codes are preserved. (#431, #398, #408) by @yeongseon.

- Collection column metadata retains CAS collection-kind flags instead of treating the element type as the column type. SET/MULTISET/SEQUENCE, including empty collections with a NULL element-type header, return their documented containers with `decode_collections=True`, or raw bytes when disabled, in both sync and async queries, for initial and subsequent fetches. (#403, #410) by @yeongseon.

- **`Connection.get_last_insert_id()` / `AsyncConnection.get_last_insert_id()` no longer return an ambiguous empty string after `commit()` (#381)** — the broker identity captured after INSERT is cached so it survives commit/rollback and SELECT. Successful values remain strings; unavailable identities return `None`. A new INSERT attempt, nonempty batch, or physical connection change clears the cache; failed, empty, or malformed identity retrieval leaves it unavailable. The broker can report an earlier identity after a non-auto-increment INSERT, so an ID does not prove the current statement generated it or that a row exists after rollback. `cursor.lastrowid` remains `int | None`. This is a documented bug correction, not an annotation-only change. Contributed by @srijavuppala (first contribution 🎉).

- Empty `bytes` LOB writes return `0` without a broker request after existing object, offset, connection and wire argument checks. BLOB/CLOB data and handles stay unchanged; nonempty ACK checks and existing bool/other-data paths are preserved. No new strict type policy or async LOB feature is introduced. (#394) by @yeongseon.

- **Empty `executemany()` clears previous results (#376)** — sync and async cursors
  release the previous query handle and reset result state to `description=None`,
  `rowcount=0`, and `lastrowid=None`. No SQL is executed; immediate query-close
  failures propagate without discarding the handle. With deferred close (#488),
  an eligible previous handle is queued without a request and an empty call
  does not flush the queue; FC20 batches likewise do not carry queued IDs (#585) by @HarshRajSinghania (first contribution 🎉).

- Failed batch execution no longer exposes stale cursor result state (#375): sync and async executemany_batch clear prior result metadata, row counts, and last-insert IDs before the batch request, including per-statement, transport, and response-parse failure paths. If closing the previous query handle fails, no batch is sent and the handle remains tracked. Contributed by @tayfuryldz.

- **Batch execution closes an existing query handle (#374)** — `executemany_batch()` now releases an active server-side query handle before sending a batch request, matching `execute()` and preventing the prior result-set handle from leaking. Sync and async cursors keep the same behavior. Contributed by @tayfuryldz (first contribution 🎉).

- **`Cursor.arraysize` now rejects non-integer values in sync and async cursors (#370).** Floats, booleans, and other non-integers raise `ProgrammingError` without changing the previous value; positive integers remain valid. Contributed by @MugdhoAI (first contribution 🎉).

- Format very large integer parameters as decimal strings without converting them to floats, avoiding `OverflowError`. Float NaN/infinity rejection and boolean formatting are unchanged. (#368) by @varshith84 (first contribution 🎉).

### Documentation

- **Official-driver compatibility design (#438, #418)** — a conservative additive design with separate wrapper/native namespaces, unchanged ordinary 1.x/SQLAlchemy contracts, classified safe deviations and focused migration/acceptance boundaries. It preceded the construction-only #465 slice and does not authorize a 2.0/default or release migration. A bounded typed-CAS prepared binding design for the sync scalar slice (#439) adds exact FC2/FC3/FC6 framing, session-owned handle/result states, explicit pooling-on evidence limits and failing-first test IDs. This is design evidence, not a shipped prepared API, ordinary 1.x behavior change, or full native-parity claim. Contributed by @yeongseon.

- **Official-driver inventories (#436, #437, #455)** — a source-referenced public API inventory and compatibility guide (#436), and a pinned declaration inventory with reviewed assertion subcases in a scenario ledger that keeps unknown/duplicate candidates and execution evidence separate (#437). Private FC9 request/condensed-column groundwork (#455) preceded the getter activation and owned row consumption in #456; its wire fixtures alone were not live-getter or native-parity certification. Catalog consistency checks do not certify functional parity. Contributed by @yeongseon.

- Added a README "First contribution" guide (with Korean translation) pointing newcomers to the right sibling repo for their first PR, documented the `good first issue` → `status: in progress` label lifecycle in AGENTS.md, and clarified contributor and maintainer review/label/translation responsibilities. Contributed by @yeongseon.

- Acknowledge CUBRID/cubrid-python's reference test scenarios in the README, NOTICE and third-party provenance notes, with source links and explicit licensing-verification limits. Contributed by @yeongseon.

### CI

- Development quality checks synchronize Ruff/Mypy hook revisions with the exact dev pins, reject installed-tool/configuration drift, and lint/format maintained scripts and demos through shared local/CI Make targets; the Mypy hook now checks the package. (#416) by @yeongseon.

- Full integration validation selects current pytest markers instead of filename globs, so the normal, TLS and nightly slow lanes cover the declared integration inventory, including concurrency stress; unknown skips and missing workflow paths fail the lane audit, and TLS provisioning runs broker commands as the service owner. (#397) by @yeongseon.

- Integration CI uses the shared CUBRID readiness probe with host/port connection fields and fails before running tests when all retries are exhausted. (#411) by @yeongseon.

- Docs exceptions in pull requests are validated with executable event-JSON checks, and the two verified shared workflow callers are pinned. CI code/security/release gates and security support policy are unchanged. Contributed by @yeongseon.

## [1.7.1] - 2026-09-18

This maintenance release fixes silent truncation when reading large LOB values and expands the documentation with Korean pages and a reorganized site. Callers using `Lob.read(n)` now receive the requested bytes up to end-of-LOB rather than a silently short first broker packet.

### Fixed

- **`Lob.read(n)` no longer silently truncates large reads (#362)** — the CUBRID broker caps each `LOB_READ` response at a fixed size (~81908 bytes), so a single request returned a short buffer for any LOB larger than that, with no error (silent partial-read data loss). `Lob.read` now loops, advancing the offset by the bytes the broker actually returned, until the full requested length is collected or the broker signals end-of-LOB. As a side benefit, `read(0)` now short-circuits with no server round-trip (previously it triggered a server-side transaction abort) by @yeongseon.

### Documentation

- **Demo GIF embedded in README** — auto-generated terminal demo showing pip install → connect → query → zero dependencies. Rendered from `demos/pycubrid-demo.json` via `demos/render_gif.py`. Contributed by @yeongseon.
- **Korean translations of 13 documentation pages (#317, #318)** — added Korean pages under `docs/ko/`: quickstart, connection guide, examples and FAQ; parameter binding, type system, CAS protocol and API reference; support matrix, architecture, performance guide, development guide and troubleshooting. The pages are reachable from the documentation site navigation. Keeping them in sync with the English pages is advisory, while the `README.ko.md` sync check remains a hard gate. Documentation only; no code or runtime behavior change. Contributed by @yeongseon.
- **Korean/multi-language docs governance** — every `docs/README.<lang>.md` translation now carries a sync marker, and docs-sync gained a `translation-sync` job that fails a PR when `README.md` changes without any translation changing (escape hatch: the `translations-deferred` label) by @yeongseon.
- **Docs site information architecture unified across the ecosystem** — nav reorganized to the shared six-tab skeleton (Home / Getting Started / Usage / Reference / Operations / Project), the five README translations (ko/de/hi/ru/zh) are now reachable via Project → Translations (previously URL-only), palette unified to blue with search-suggest, and the homepage gains an Ecosystem section linking the three sibling sites. Contributed by @yeongseon.
- **CUBRID server license relationship documented; copyright notice unified (#309)** — `docs/ARCHITECTURE.md` gains a section stating the verified upstream licensing (server engine Apache-2.0, APIs/connectors BSD per CUBRID's `COPYING`; the often-cited GPL v2+ no longer applies) and that pycubrid is an independent wire-protocol client with no server code included or linked. `THIRD_PARTY_LICENSES.md` carries the same one-paragraph statement. LICENSE/NOTICE copyright lines now read `Yeongseon Choe, Gyeongjun Paik` (2025-2026), reflecting the two primary authors. Contributed by @yeongseon.
- **Added `THIRD_PARTY_LICENSES.md`** — pip-licenses-generated inventory of the development toolchain's licenses. pycubrid itself has zero runtime dependencies, so nothing in the table ships in the wheel. Documentation only. Contributed by @yeongseon.

## [1.7.0] - 2026-09-02

This release expands DB-API connection compatibility, LOB lifecycle safeguards, time-zone parsing and public parameter-binding behavior. **Applications calling the escape helpers directly should review the new `no_backslash_escapes=True` default (see Upgrade notes);** ordinary driver-mediated binding passes its negotiated value explicitly.

### Upgrade notes

- **The escape helpers now default to literal-backslash mode (#293).** `escape_string()`, `format_parameter()`, `bind_parameters()` and `CursorParamsMixin._escape_string()` in `pycubrid._cursor_common` took `no_backslash_escapes=False` by default before 1.7.0; they now default to `True`, matching CUBRID's server default (`no_backslash_escapes=yes`, where a backslash is an ordinary literal character). `split_on_placeholders()` in the same module gains the same keyword in 1.7.0, also defaulting to `True`. Ordinary use through `Cursor` and `AsyncCursor` is not affected, because they pass the connection's negotiated value explicitly. Only code that calls these helpers directly without the keyword changes: a backslash that was doubled before is now left as is. To keep the previous behavior, pass `no_backslash_escapes=False`. A regression test pins the shared default. See [docs/PARAMETER_BINDING.md](https://github.com/cubrid-lab/pycubrid/blob/v1.7.0/docs/PARAMETER_BINDING.md) by @yeongseon.

### Added

- **DB-API 2.0 exception classes are now exposed as attributes on `Connection`/`AsyncConnection` (#282)** — PEP 249's optional extension recommends that the standard exception classes (`Warning`, `Error`, `InterfaceError`, `DatabaseError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`) be accessible as attributes of the `Connection` object, so multi-connection code can catch errors specific to a driver without importing its module. All ten classes are now set as class attributes on the shared `ConnectionCommonMixin`, so both sync `Connection` and `AsyncConnection` (and their instances) expose them with identity preserved — e.g. `conn.IntegrityError is pycubrid.IntegrityError`. Additive, backward-compatible surface change (`api-baseline.json` regenerated) by @yeongseon.
- **`Lob` now supports client-side lifecycle management — `close()`, context-manager (`with`), and a closed-state guard (#268)** — the CUBRID CAS wire protocol has **no** LOB release/free/close opcode (function codes are contiguous: `LOB_NEW=35`, `LOB_WRITE=36`, `LOB_READ=37`, `END_SESSION=38`), and the reference JDBC driver's `Blob.free()` is likewise purely client-side. LOB handles are therefore connection/session-scoped and reclaimed only when the owning connection/session closes. `Lob.close()` mirrors this: it is idempotent, performs **no** network I/O, and simply invalidates the Python object so subsequent `read()`/`write()` raise `InterfaceError("LOB is closed")`. `Lob` is now usable as a context manager (`__exit__` closes without suppressing exceptions). The `lob_handle`/`lob_type` properties remain readable after close for introspection. The class docstring documents the connection-scoped semantics. Contributed by @yeongseon.
- **`Lob` now rejects async connections instead of silently producing un-awaited coroutines (#266)** — `Lob` drives its connection *synchronously* (`_send_and_receive` with no `await`), but `AsyncConnection._send_and_receive` is a coroutine. A `Lob` bound to an `AsyncConnection` (via direct construction — `Lob` is exported) therefore called an async method without awaiting it, silently getting a coroutine object back and emitting "coroutine was never awaited" warnings. `Lob.__init__` and `Lob.create()` now detect an async connection (`inspect.iscoroutinefunction`) and raise `NotSupportedError` *before* any packet is sent; async LOB support is not implemented. `AsyncConnection.create_lob()` (previously absent, raising `AttributeError`) now exists and raises the same `NotSupportedError` for a clear, discoverable failure. Contributed by @yeongseon.

### Changed

- **Ruff lint rule selection now declared explicitly (#247)** — `pyproject.toml` configured ruff but never set `[tool.ruff.lint] select`, so `ruff check` inherited ruff's implicit defaults. Ruff expanded that default set in 0.16 (59 → 413 rules against this repo's config), which is why #245 (`0.15.22 → 0.16.1`) failed lint with 237 errors in untouched code. Pinning the ruff *version* in #236 stopped unpinned installs from drifting, but could not survive the bump itself — the rule set is now pinned too, via `select = ["E4", "E7", "E9", "F"]`, which is exactly what ruff selected by default through 0.15.x (same 59 rules under both versions) by @paikend.
- **Corrected the `Documentation` project URL and added a `Changelog` URL in packaging metadata (#269)** — `[project.urls].Documentation` pointed at the GitHub source tree (`.../tree/main/docs`) rather than the published docs site; it now points to `https://cubrid-lab.github.io/pycubrid/` (matching the README badge and `mkdocs.yml`). A `Changelog` URL (`.../blob/main/CHANGELOG.md`) was also added, mirroring sqlalchemy-cubrid. Packaging metadata only; no code or runtime behavior change. Contributed by @yeongseon.
- **Renamed the example table `cookbook_users` to `users` consistently across all documentation surfaces (#270)** — the placeholder name appeared in 81 places across `README.md`, all five translated READMEs (`docs/README.{ko,zh,hi,de,ru}.md`), `docs/CONNECTION.md`, `docs/EXAMPLES.md`, `docs/TYPES.md`, and the generated `docs/llms-full.txt`. Documentation only; no code or runtime behavior change. Contributed by @yeongseon.
- **Binding a Python collection as a single parameter now raises an actionable error message (#287)** — passing a `list`, `tuple`, `set`, `frozenset`, or `dict` as one bound value previously raised the generic `ProgrammingError("unsupported parameter type")`. It now raises `ProgrammingError` with a message that names the collection case and points at the fix: pycubrid does **not** auto-expand `IN (?, ?, ...)`, so placeholders must be expanded explicitly in the SQL. The exception class is unchanged (still `ProgrammingError`) and the message text remains non-contractual; other unsupported types keep the generic message. See [docs/PARAMETER_BINDING.md](https://github.com/cubrid-lab/pycubrid/blob/v1.7.0/docs/PARAMETER_BINDING.md#type-mapping-guarantees) by @yeongseon.

### Fixed

- **Backslash-escape mode is now auto-negotiated from the server, fixing silent string corruption (#255, #263, #264, #292, #300)** — CUBRID's `no_backslash_escapes` system parameter defaults to `yes` (a backslash is an ordinary literal character), but pycubrid defaulted its client-side flag to `False` and unconditionally doubled backslashes. Against a stock server this silently corrupted data: `C:\temp\file` (12 chars) was stored as `C:\\temp\\file` (14 chars), and `regex \d+` became `regex \\d+`. LIKE metacharacters (`%`, `_`) were never escaped and remain untouched. When `no_backslash_escapes` is not passed explicitly, `Connection`/`AsyncConnection` now probe the live server once at connect time with `SELECT CHAR_LENGTH('\\')` (#255): a result of `2` selects literal mode (`True`, no doubling) and `1` selects escape-processing mode (`False`). Passing `no_backslash_escapes=True|False` explicitly skips the probe, and then no probe transaction is opened. If the probe raises or returns any other length, connect now raises `OperationalError` instead of silently falling back to `False` with a logged warning (#263); that fallback could pick the wrong escaping mode and corrupt string literals or enable SQL injection. **Behavior change:** connections that previously succeeded with a mis-detected mode now raise at connect time, so pass `no_backslash_escapes` explicitly when the probe cannot run. Three follow-ups make the negotiation safe in practice. `AsyncConnection.connect()` now runs the sync order (connect, negotiate escapes, apply autocommit) behind a one-time setup gate, so another task's query can no longer run before the escape mode is pinned (#264); the setup task's own probe bypasses the gate, a setup failure propagates to waiting tasks, and transparent reconnect never re-probes. The probe transaction is now rolled back on every exit path (success, unexpected result or probe error), so a new connection, including one created with `autocommit=True`, is no longer handed over with a lingering driver-started transaction that could make CUBRID reject a follow-up operation such as SQLAlchemy setting `isolation_level` on a new pooled connection (#292). If that rollback itself fails, the connection is closed and `OperationalError` is raised so it can never be reused in an unknown transaction state, unless a probe error is already propagating, in which case that original error is preserved and the connection is still closed (#300). No public API change beyond the connect-time error. See [docs/PARAMETER_BINDING.md](https://github.com/cubrid-lab/pycubrid/blob/v1.7.0/docs/PARAMETER_BINDING.md#escape-mode-negotiation) by @yeongseon.
- **`TIMESTAMPTZ`/`TIMESTAMPLTZ` reads no longer crash with "malformed response from broker" (#289)** — `PacketReader._parse_timestamptz` decoded the timezone-aware timestamp types using the 7-short / 14-byte `DATETIMETZ` layout (which carries a millisecond field). But `TIMESTAMPTZ`/`TIMESTAMPLTZ` are **second-precision**: their wire layout is 6 shorts (12 bytes, no millisecond field) followed by the timezone string. The parser therefore over-read the first 2 bytes of the timezone string as a `ms` value and computed `ms * 1000`, which overflowed `datetime`'s "microsecond must be in 0..999999" range whenever those bytes were non-trivial (e.g. a leading `"As"` from `Asia/Seoul` = 16755 → 16755000 µs). The resulting `ValueError` surfaced as `OperationalError("malformed response from broker")`, making any `SELECT` of a `TIMESTAMPTZ`/`TIMESTAMPLTZ` column unreadable. `_parse_timestamptz` now reads the correct 12-byte second-precision layout (microsecond fixed to 0), and `DATETIMETZ`/`DATETIMELTZ` get their own `_parse_datetimetz` reader that keeps the 14-byte millisecond layout; a shared `_attach_timezone_suffix` helper parses the trailing timezone string for both. See [docs/TYPES.md](https://github.com/cubrid-lab/pycubrid/blob/v1.7.0/docs/TYPES.md) by @yeongseon.
- **`CUBRIDIsolationLevel.DEFAULT` corrected from `0x01` to `0x04` (READ COMMITTED) (#285)** — the enum advertised `DEFAULT = 0x01`, which aliases the lowest legacy pre-MVCC level (`COMMIT_CLASS_UNCOMMIT_INSTANCE`), not CUBRID's actual server default. CUBRID's default transaction isolation is **READ COMMITTED**, i.e. `REP_CLASS_COMMIT_INSTANCE = 0x04` (verified on live CUBRID 11.2: `GET TRANSACTION ISOLATION LEVEL` returns `4` on a fresh connection). `DEFAULT` now aliases `0x04`. The accompanying test that asserted `DEFAULT == 0x01` was fossilizing the wrong value and is corrected. A class-docstring note also records that CUBRID's MVCC engine (10.0+) only accepts levels `0x04`/`0x05`/`0x06`; the lower codes remain for protocol/back-compat reference only. This is a constant used for reference; the driver's connection logic does not consume it, so no runtime path changes. Contributed by @yeongseon.
- **Placeholder tokenizer is now CUBRID-dialect-aware (#265)** — `split_on_placeholders()` (which `bind_parameters()` uses to locate `?` placeholders) only understood ANSI single-quoted strings (`''` doubling), double-quoted identifiers, `--` line comments, and `/* */` block comments. A `?` inside CUBRID-specific lexical contexts was mis-detected as a real placeholder, throwing `wrong number of parameters` or interpolating into the wrong position. The tokenizer now also honours: backslash escapes inside single-quoted strings when the connection's `no_backslash_escapes` is `False` (so `\'` does not terminate the literal and `\\` is a literal backslash — `''` doubling stays valid in both modes; the standalone default `no_backslash_escapes=True` preserves the previous behaviour and matches CUBRID's server default), `//` C++-style line comments, and backtick (`` `id` ``) and bracket (`[id]`) quoted identifiers (first closing delimiter terminates). `bind_parameters()` now threads the connection's `no_backslash_escapes` through to the tokenizer for consistency with `escape_string()`. **Known limitation:** double quotes are treated as identifier delimiters, correct for CUBRID's default `ansi_quotes=yes`; SQL relying on `ansi_quotes=no` (where `"` delimits strings) is not specially handled, as pycubrid does not track that parameter. Contributed by @yeongseon.
- **Closed a `bool | None` type hole around the negotiated escape mode (#267)** — the cursor parameter-binding mixin declared `_connection: Any`, which masked from mypy that `Connection._no_backslash_escapes` is `bool | None` (unset until negotiated) while `bind_parameters()`/`format_parameter()` require a concrete `bool`. `CursorParamsMixin` is now `Generic` over the concrete connection type (bound to a minimal `_EscapeModeSource` protocol), `Cursor`/`AsyncCursor` specialise it (`CursorParamsMixin[Connection]` / `[AsyncConnection]`), and a new `_resolve_escape_mode()` narrows the value — raising `InterfaceError` if the mode is read before negotiation instead of silently mis-escaping. `AsyncCursor.__init__` is also tightened from `connection: Any` to `AsyncConnection`. No public behaviour change; mypy now catches misuse. Contributed by @yeongseon.
- **`Lob.read()` now rejects overlong server responses and invalid arguments (#268)** — `read()` returned `packet.lob_data` without checking it against the requested `length`, so a misbehaving server or corrupted transport could hand back more bytes than asked for. `read()` now raises `OperationalError` when the returned length exceeds the requested length, and both `read()`/`write()` validate that `offset` (and, for `read`, `length`) are non-negative, raising `InterfaceError` before any packet is sent. Contributed by @yeongseon.
- **`format_parameter` now rejects the Ctrl-Z (`0x1A`) byte in string parameters instead of emitting a raw control byte (#271)** — `escape_string()` previously prefixed `0x1A` with a backslash while leaving the raw control byte in the literal, even though CUBRID's SQL grammar defines **no** safe literal escape for it (there is no MySQL-style `\Z`, and the escape had no effect under CUBRID's default `no_backslash_escapes=yes`). Embedding `0x1A` is now rejected with `ProgrammingError("string parameter contains Ctrl-Z (0x1A) byte")` in **both** escape modes, consistent with the existing null-byte rejection. **Behavior change:** strings containing `0x1A` that previously produced a (malformed) literal now raise at bind time. The issue's two other claims were investigated and **rejected as invalid** against the CUBRID manual: `TIME` has second resolution (its literal grammar allows only `'HH:MI:SS'`), so dropping microseconds is correct; and CUBRID accepts scientific/exponential numeric literals (an `E`-form number is parsed as `DOUBLE`), so `str()`'s exponent output (e.g. `1e+20`) is a valid literal. Both are now pinned by tests. Contributed by @yeongseon.
- **`_extract_first_keyword()` now strips CUBRID C++-style `//` line comments (#256)** — the leading-comment stripper used by `executemany()` to detect the statement's DML verb only skipped `/* */` block comments and `--` ANSI line comments, so a statement prefixed with a `//` line comment (a valid CUBRID comment style) had its verb mis-detected and could be excluded from batch execution. `_RE_LEADING_COMMENTS` now also skips `//` line comments to EOL/EOF, keeping it consistent with the comment styles already honoured by `split_on_placeholders()` (#265) by @yeongseon.

### Documentation

- **Added an Acknowledgments section and a `NOTICE` file crediting the CUBRID Node.js driver ([node-cubrid](https://github.com/CUBRID/node-cubrid), © 2008–2012 Search Solution Corporation, BSD-3-Clause)** — during pycubrid's initial development, node-cubrid was consulted as a reference implementation to understand CUBRID's CAS wire protocol (packet structure and function codes). This is recorded as an acknowledgment in `README.md`, `docs/README.ko.md`, and the new `NOTICE` file. Documentation only; no code or runtime behavior change. Contributed by @yeongseon.

## [1.6.2] - 2026-08-06

This patch release hardens outgoing serialization and incoming numeric parsing, fixes direct async autocommit initialization and rejects non-finite Decimal parameters consistently.

### Fixed

- **Write-path serialization overflow now raises `DataError`, not raw `struct.error` (#223)** — `_send_and_receive` (sync) and `_do_send_and_receive` (async) called `packet.write(self._cas_info)` inside a block that only caught `OSError`. An oversized outbound value (e.g. a huge LOB offset/length, or a large `executemany` batch) trips `struct.pack`'s int32 range check and raised a bare `struct.error`, escaping the PEP 249 contract entirely — the write-side counterpart of the parse-side hardening already done in #201/#205. Both `packet.write()` call sites now catch `struct.error` and raise `DataError("parameter value too large to serialize into CAS request")`; since nothing was sent to the socket yet, the connection is left open and usable rather than torn down. Contributed by @yeongseon, co-authored by @paikend.
- **`AsyncConnection(autocommit=True)` no longer silently dropped (#224)** — constructing `AsyncConnection` directly (bypassing the `pycubrid.aio.connect()` factory) with `autocommit=True` swallowed the flag into `**kwargs` with no error and no effect, unlike sync `Connection`, which has always accepted `autocommit` in its own constructor. `AsyncConnection.__init__` now accepts a keyword-only `autocommit: bool = False` parameter, applied via `await self.set_autocommit(True)` the first time `connect()` completes. The `pycubrid.aio.connect()` factory no longer needs its own separate `set_autocommit()` call — `autocommit` just flows straight through to the constructor now. Contributed by @yeongseon, co-authored by @paikend.
- **`Decimal('NaN')`/`Decimal('Infinity')` now rejected like their `float` equivalents (#225)** — `format_parameter()`'s `Decimal` branch returned `str(value)` unconditionally, before ever reaching the NaN/Inf guard that already protects the `int`/`float` branch below it. A `Decimal('NaN')` or `Decimal('Infinity')` parameter was silently formatted as the bare token `NaN`/`Infinity` and sent straight to the server instead of raising the documented `ProgrammingError("nan and inf are not supported by CUBRID")`. The `Decimal` branch now checks `value.is_nan() or value.is_infinite()` first. Contributed by @yeongseon, co-authored by @paikend.
- **NUMERIC field parsing no longer leaks `decimal.InvalidOperation` (#231)** — `PacketReader._parse_numeric` constructed a `Decimal` straight from the wire string with no guard. An empty or corrupt NUMERIC field raised `decimal.InvalidOperation` (an `ArithmeticError`, not a `ValueError`), slipping past the malformed-response handlers in both sync and async `_send_and_receive`. This left the socket open and desynced for reuse. `_parse_numeric` now catches `InvalidOperation` and re-raises as `ValueError`, so parse errors flow through the existing handler: socket closed, `_connected = False`, `OperationalError` raised with cause chained. Contributed by @yeongseon.
- **CI lint now uses pinned ruff version (#236)** — the lint job used `pip install ruff` (unpinned), which installed the latest ruff. When ruff 0.16.x introduced new rules, every PR started failing lint. Now installs from `.[dev]` extras to match the pinned `ruff==0.15.22` in `pyproject.toml`. Contributed by @yeongseon.

## [1.6.1] - 2026-07-18

This patch release surfaces per-statement errors in batch execution and rejects invalid or oversized broker response lengths before allocating a response buffer.

### Fixed

- **`executemany()` batch error handling (#186)** — `executemany_batch()` in both sync `Cursor` and `AsyncCursor` consumed `packet.results` but never checked `packet.errors`, silently swallowing per-statement batch failures. Partial failures (e.g. one INSERT in a batch of 10 hits a unique constraint violation) were invisible to the caller — data integrity risk. Now raises the first batch error using the same CAS error code dispatch as `protocol._raise_error()` (PR #208), mapping to the correct PEP 249 exception class (IntegrityError, ProgrammingError, OperationalError, etc.) by @yeongseon.
- **DATA_LENGTH broker response validation (#188)** — `_send_and_receive()` in both sync `Connection` and `AsyncConnection` unpacked the 4-byte `DATA_LENGTH` header and immediately allocated `bytearray(data_length)` without bounds checking. A malformed or hostile broker response with a negative value would raise a raw `ValueError` from `bytearray()`, and an oversized value could trigger unbounded memory allocation (OOM). Added `_validate_data_length()` that rejects negative values and values exceeding `DataSize.MAX_PACKET_SIZE` (256 MiB) with a clean `OperationalError`, applied at both the handshake and main send/receive paths. Contributed by @yeongseon.

## [1.6.0] - 2026-07-18

This release bounds the memory retained by sync and async cursors while reading large results, and classifies broker errors using CAS error codes rather than relying mainly on message text.

### Fixed

- **Cursor memory bounding for large result sets (#203, PR #207)** — both sync `Cursor` and `AsyncCursor` previously accumulated the entire result set in `_rows` via `_rows.extend(packet.rows)` on every server fetch. Iterating a 10K-row result set via `fetchone()` would buffer all 10K rows in memory even though only one row was needed at a time. Fixed by decoupling the server-side fetch position (`_fetched_count`, tracking total rows received) from the local buffer cursor (`_row_index`, indexing into `_rows`). `_fetch_more_rows()` now REPLACES the buffer with the new batch instead of extending it, keeping memory bounded by the configurable `fetch_size` (default 100). `fetchall()` additionally clears the buffer after consuming all rows. This fixes a latent dual-purpose bug where `_row_index` was used both as buffer index AND server fetch position, which would have caused infinite re-fetching if any naive trim scheme had been applied. Contributed by @yeongseon.
- **CAS error code dispatch (#204)** — `_raise_error()` in `protocol.py` previously classified exceptions by text substring matching (looking for keywords like "unique", "syntax", "duplicate" in the error message). This was fragile across CUBRID versions. Replaced with deterministic CAS error code dispatch: `CAS_ERROR_TO_EXCEPTION` mapping in `error_codes.py` maps 16 specific codes to the correct PEP 249 exception class (IntegrityError, ProgrammingError, OperationalError, InternalError, DataError). Text heuristics are now only used as a fallback for code -1 (ER_DBMS passthrough), where CAS wraps server-engine errors with a generic code. Also fixed a duplicate `return error_message` statement in `_add_error_hints()`. Contributed by @yeongseon.

## [1.5.1] - 2026-07-18

This maintenance release handles malformed CAS responses safely in synchronous connections and checks LOB write acknowledgments, preventing a partial server write from silently appearing successful.

### Fixed

- **Sync `_send_and_receive` parse-error exception parity (#201)** — the sync connection path at `connection.py:_send_and_receive` previously caught only `OSError`, meaning malformed CAS broker responses that raised `struct.error`, `ValueError`, `IndexError`, or `UnicodeDecodeError` would bypass socket cleanup and leave the connection in a dirty state for reuse. The async path at `aio/connection.py:615-621` already handled these. Ported the full exception catch list to the sync path so parse errors now close the socket, mark `_connected = False`, and raise `OperationalError("malformed response from broker")` with the original cause chained. Contributed by @yeongseon.
- **LOB write server ACK verification (#202)** — `Lob.write()` returned `len(data)` unconditionally without checking whether the server actually wrote all the bytes. Under disk-full / quota-exceeded conditions, the server could write fewer bytes and the caller would never know — silent data truncation. `LOBWritePacket.parse()` now extracts `bytes_written` from the CAS response (the response code doubles as the byte count on success, matching `LOBReadPacket`'s existing pattern). `Lob.write()` compares `bytes_written` against `len(data)` and raises `OperationalError` on mismatch. Contributed by @yeongseon.

## [1.5.0] - 2026-05-23

This release documents and CI-checks the 1.x public API compatibility contract, fixes TLS handshake and reconnect behavior, and expands cross-platform and live TLS validation. Parameter binding remains client-side SQL-literal formatting rather than server-side prepared execution; the detailed caveats below remain important.

### Upgrade notes

- **A fetch after a transparent reconnect now raises `OperationalError` instead of returning fewer rows (#167).** When the broker releases the CAS worker and pycubrid reconnects on its own, a cursor that still has rows waiting on the server loses its server-side result set. Before 1.5.0, `fetchone()`, `fetchmany()` and `fetchall()` then quietly reported the end of the data, so the caller received a short result with no error. From 1.5.0, once the rows already buffered in the cursor are used up, the fetch raises `OperationalError` (`result set lost due to broker reconnect mid-fetch; re-execute the query to continue`). This applies to both `Cursor` and `AsyncCursor`. Commit and rollback keep their previous PEP 249 behavior. If your code reads large result sets across long pauses, catch `OperationalError` and run the query again, or fetch the whole result before the pause. Calling `execute()` or `close()` on the cursor clears the condition. Contributed by @yeongseon.

### Added

- **Session state restoration on transparent reconnect (#167)** — When the broker signals `CAS_INFO_STATUS_INACTIVE` and pycubrid reconnects transparently (matching JDBC's `UClientSideConnection.checkReconnect`), any session setting the caller has **explicitly** set on the connection (currently `autocommit`) is re-sent to the new CAS worker with `SetDbParameterPacket`. Settings the caller never touched stay at the broker default. The `ping(reconnect=True)` recovery path restores the same way. If the restore fails, the connection is torn down and the transport error is chained as `__cause__`. Sync `Connection.ping()` and async `AsyncConnection.ping()` attempt the reconnect and restore at most once per call by @yeongseon.
- **Mid-fetch reconnect raises `OperationalError` (#167)** — When the broker releases the CAS worker while a cursor still has rows pending on the server, the server-side query handle is no longer valid. `fetchone()`, `fetchmany()` and `fetchall()` raise `OperationalError` (`result set lost due to broker reconnect mid-fetch`) once the buffered rows are exhausted, instead of silently returning a truncated result set. Rows already buffered stay accessible, and `execute()` and `close()` reset the condition. See Upgrade notes by @yeongseon.

### Changed

- **`RELEASE_POLICY.md` added; public API surface is now CI-gated.** The project's previously implicit 1.x semantic-versioning contract is now an explicit, machine-checkable document at the repository root. The CI workflow gains a `compat-check` job that runs `scripts/check_public_api.py` against the committed `api-baseline.json`; any change to the public surface (functions, classes, methods, parameters of `pycubrid.connect`, `pycubrid.aio.connect`, `Connection`, `Cursor`, `AsyncConnection`, `AsyncCursor`, `Lob`, exception classes, and the PEP 249 contract constants `paramstyle`/`apilevel`/`threadsafety`) fails CI unless the developer regenerates the baseline in the same change. Type objects (e.g. `STRING`, `BINARY`) are tracked by presence and type name only, per `RELEASE_POLICY.md` §2 "What the gate does *not* detect". The 1.2.0 minor-release breaking change (removal of `Mapping` parameter style from `_bind_parameters`) is acknowledged in `RELEASE_POLICY.md` §4 as a historical violation rather than silently rewritten. README status line updated from "Beta" to "Stable (1.x)" across English and all five translated READMEs, matching the `Production/Stable` PyPI classifier shipped since 1.0.0. Contributed by @yeongseon.

### Fixed

- **Async TLS verification failures now surface promptly on Python 3.10** — On Python 3.10 only, `AsyncConnection` first probes TLS verification on a separate socket to the same endpoint, using the same `SSLContext` and `server_hostname`, immediately before `loop.start_tls()`. Any `ssl.SSLError` surfaces as `OperationalError`, as on 3.11+. This works around a known CPython 3.10 `asyncio` bug (fixed in 3.13/3.14) where `loop.start_tls()` hangs indefinitely on certificate-verification failures. It is a no-op on Python 3.11+; on 3.10 each connect costs one extra TCP round-trip (closes #156) by @yeongseon.
- **TLS handshake now matches CUBRID's STARTTLS-style upgrade** — both sync and async `connect()` previously wrapped the socket in TLS before any bytes were exchanged, which never worked against a real `SSL=ON` CUBRID broker. The driver now (1) opens a plaintext TCP socket, (2) sends the 10-byte ClientInfoExchange handshake using the SSL magic string `"CUBRS"` (vs `"CUBRK"` for plain), (3) reads the 4-byte broker status (negative codes now raise `OperationalError` instead of silently falling through), (4) reconnects to the redirected CAS worker on `new_connection_port > 0` without re-handshaking (matches upstream JDBC `BrokerHandler.connectBroker`), and (5) upgrades the connection to TLS before sending `OPEN_DATABASE`. The async path uses `loop.start_tls()` for Python 3.10 compatibility. Validated end-to-end against CUBRID 11.4 with `SSL=ON` and a self-signed broker certificate (#154) by @yeongseon.
- **Exception chaining preserved on async transport timeouts (#167)** — `AsyncConnection` handshake and read timeouts now raise `OperationalError ... from exc` instead of `from None`, so the underlying `asyncio.TimeoutError` stays available as `__cause__` for diagnostics by @yeongseon.

### Performance

- **Native `Connection.ping()` causally validated at application layer** — Tier 2 ORM benchmark in [cubrid-benchmark `2026-04-22_native-ping-hotpath`](https://github.com/cubrid-lab/cubrid-benchmark/tree/main/experiments/orm-overhead/runs/2026-04-22_native-ping-hotpath) (paired same-version A/B vs forced `SELECT 1`, 7 trials, bootstrap 95% CI) confirms native CHECK_CAS ping is **+279.9% throughput** on raw ping_only [+278.0, +283.9] and **+587.8% on SQLAlchemy `checkout_only`** [+581.8, +603.8] with `pool_pre_ping=True`. Performance Loop ping propagation gap closed. Contributed by @yeongseon.

### Documentation

- **Parameter binding contract documented** — new `docs/PARAMETER_BINDING.md` describes the driver-side literal binding for 1.x: the SQL literal used for each Python type, the `escape_string` and `no_backslash_escapes` modes, how placeholders are found around quoted strings, identifiers and comments, and what is not guaranteed (no server-side prepared statements, identifiers are not escaped, no `IN`-clause expansion, exception-message text is not contract). Linked from `README.md` and `docs/index.md`. Contributed by @yeongseon.
- **TLS and handshake documentation corrected and extended** — fixed factual errors in the protocol, connection and architecture docs (protocol version, OpenDatabase field order, the `MAGIC_STRING_SSL` name, the CAS reconnection diagram), listed the TLS 1.2 minimum in `SUPPORT_MATRIX.md`, added TLS examples, a Transport Security section in `SECURITY.md` and local TLS test instructions in `CONTRIBUTING.md`, and a `make integration-tls` target (#160, #161, #162, #163) by @yeongseon.
- **Async TLS handshake hang on Python 3.10 documented as a known limitation** — a known CPython asyncio TLS handshake bug on Python 3.10 causes `loop.start_tls()` to hang on certificate-verification failures on 3.10 only (fixed in 3.13/3.14); the docs describe the workaround and the negative-path test is skipped on 3.10 (#156) by @yeongseon.
- **Reconnect behavior documented** — `docs/CONNECTION.md` lists which session settings are restored on a transparent reconnect and which are not, and corrects the `autocommit` default note: pycubrid sends `auto_commit` with every `PrepareAndExecute`, so the broker's own `CUBRID_AUTO_COMMIT` setting is effectively overridden. `docs/API_REFERENCE.md` documents the `OperationalError` raised by `fetchone`/`fetchmany`/`fetchall` after a reconnect invalidates a partly read result set. Contributed by @yeongseon.
- **Python 3.10 async-TLS caveat citation corrected** — the upstream issue reference (`gh-142352`) was removed from the READMEs, docs, `SECURITY.md` and source comments because it describes a different `start_tls()` regression on 3.13/3.14/3.15, not the 3.10 certificate-verification hang pycubrid sees. The caveat is now described as a known CPython async-TLS handshake bug on Python 3.10, tracked as pycubrid #156. Contributed by @yeongseon.

### CI

- **Cross-platform CI**: offline tests now run on Ubuntu, Windows and macOS (Python 3.10–3.14, 15 combinations). Contributed by @yeongseon.
- **TLS regressions are caught before merge** — a per-PR `integration-tls` job (Python 3.14 × CUBRID 11.4) runs for changes to the async or sync connection code or the workflows, and `ci-gate` accepts it being skipped when none of those changed (closes #159) by @yeongseon.
- **TLS readiness probes now verify the broker certificate** — the probes in `integration-full.yml` used to disable hostname and certificate checks and so accepted any endpoint; they now use the verified default context with `CUBRID_TLS_TEST_CA_FILE`, so an untrusted broker certificate fails loudly (closes #157) by @yeongseon.
- **The expected-skip list now uses the full pytest node id** (`tests/test_aio_ssl_integration.py::test_aio_ssl_handshake_failure`), so renaming the test can no longer hide an unexpected skip (closes #159) by @yeongseon.
- **Nightly runs provision an `SSL=ON` CUBRID broker automatically** — `integration-full.yml` gains an `integration-tls` job (Python 3.10 and 3.14 × CUBRID 11.4) that enables `SSL=ON` with a self-signed certificate and runs `tests/test_aio_ssl_integration.py`, failing if any TLS test is skipped (closes #147, #155) by @yeongseon.

### Tests

- **Async TLS upgrade and handshake paths covered offline** — new `tests/test_aio_ssl_offline.py` runs 7 mocked tests, with no broker needed, for TLS upgrade argument forwarding (including `server_hostname` and the 10-second default handshake timeout), cleanup when `loop.start_tls()` fails, truncated broker status responses, and async parity for the redirect case that must not repeat the handshake (closes #158) by @yeongseon.
- **Sync/async lifecycle parity coverage expanded** — integration tests share scenarios across both drivers and cover `ping()`, CAS-inactive reconnect, auto-commit transitions, insert identity helpers, batch rowcount, close ordering, and the `AsyncConnection` `create_lob` `AttributeError` contract (closes #140) by @yeongseon.
- **Async TLS integration coverage** — new `tests/test_aio_ssl_integration.py` covers live async TLS success, failure, reconnect and shutdown against an explicitly configured TLS-enabled broker (#155) by @yeongseon.
- **Reconnect and exception-chaining tests** — 12 new tests in `tests/test_network_edge_cases.py` cover `__cause__` chaining on timeouts, session-state restore, mid-fetch `OperationalError` (sync and async) and `CancelledError` propagation when closing async streams (#167). Contributed by @yeongseon.

## [1.4.0] - 2026-05-13

This release adds **async TLS configuration and transport plumbing**, improves consistency between synchronous and asynchronous implementations, and strengthens type checking. **Working SSL=ON broker TLS was not yet available in this tag:** the transport upgraded too early in the handshake; the v1.5.0 release fixes the broker TLS sequence.

### Added

- **Async TLS options (not yet a working broker handshake)** — `AsyncConnection` accepts `ssl=True`, `ssl=False`, or an `ssl.SSLContext` with `asyncio.open_connection()` streams (#129, #136), but the v1.4.0 implementation starts TLS before the required broker handshake. SSL=ON connections require the corrected v1.5.0 protocol sequence. Contributed by @yeongseon.
- **`mypy --strict` CI gate** — typecheck job added to CI workflow to enforce strict typing (#130) by @yeongseon.
- **Sync/async parity integration tests** — expanded test coverage for bytes, datetime, fetch_size, JSON, and edge-case scenarios (#134) by @yeongseon.

### Changed

- **`ConnectionCommonMixin` extracted** — deduplicated ~70% of shared logic between `Connection` and `AsyncConnection` into a common mixin (#133, #135) by @yeongseon.
- **`CursorParamsMixin` extracted** — eliminated sync/async cursor parameter-handling duplication (#123, #127) by @yeongseon.
- **Driver-side binding semantics documented** — README, ARCHITECTURE, and PRD updated to clarify that `?` placeholders are interpolated locally, not via server-side prepared statements (#131) by @yeongseon.

### Fixed

- **`fetch_size` validation** — `Connection` and `AsyncConnection` constructors now reject non-positive `fetch_size` values (#132) by @yeongseon.
- **Dead `backports.zoneinfo` fallback removed** — eliminated unused Python 3.8 compatibility code. Contributed by @yeongseon.
- **Typed locals in async module** — replaced `str()` coercion with properly typed local variables. Contributed by @yeongseon.
- **`mypy --strict` errors resolved** — full strict-mode compliance across the codebase. Contributed by @yeongseon.
- **`asyncio.run()` for Python 3.14** — replaced deprecated `get_event_loop()` usage. Contributed by @yeongseon.

## [1.3.2] - 2026-04-21

This release adds an asynchronous CAS health check and makes the reconnect setting explicit for synchronous and asynchronous ping operations. With `reconnect=False`, ping does not initiate an implicit broker-handoff reconnect.

### Added

- **Native async `AsyncConnection.ping()`** using `CHECK_CAS` (FC=32) for lightweight CAS-level liveness checks, mirroring the sync `Connection.ping()` (#95) by @yeongseon.

### Fixed

- **Sync `Connection.ping(reconnect=False)` now honors broker handoff correctly** — native `CHECK_CAS` runs regardless of `CAS_INFO` status, while `reconnect=False` suppresses implicit broker-handoff reconnect via the new `_send_and_receive(..., allow_reconnect=False)` flag (#70) by @yeongseon.

## [1.3.1] - 2026-04-21

This is a documentation-focused update that makes timing instrumentation, bulk-execution guidance and sync/async capability differences easier to find. No driver runtime or public API change is recorded for this release.

### Documentation

- **Oracle audit fixes completed** — documentation gaps from the Oracle review were closed across the main guides, with no runtime or public API changes in `pycubrid/`. Contributed by @yeongseon.
- **Driver-level timing hooks documented** — `enable_timing=True` keyword and `PYCUBRID_ENABLE_TIMING` environment variable, `Connection.timing_stats` property, and the `TimingStats` accumulator are now covered in `docs/API_REFERENCE.md` and `docs/PERFORMANCE.md` (closes #16). The implementation has shipped since 1.0.0; this completes the "API documented" acceptance criterion. Contributed by @yeongseon.
- **Async parity wording clarified** — sync vs. async capability differences are now described consistently, including async-specific wording cleanups in the Korean docs. Contributed by @yeongseon.
- **`executemany()` guidance expanded** — bulk operation documentation now explains `executemany()` behavior and usage more clearly. Contributed by @yeongseon.
- **README translations synchronized** — Korean, German, Russian, Chinese, and Hindi READMEs were refreshed to match the current English documentation. Contributed by @yeongseon.

## [1.3.0] - 2026-04-20

This release adds TLS configuration options for synchronous connections and expands network and concurrency tests. **The tagged TLS implementation wraps the socket before the CUBRID broker handshake and does not establish a working TLS session with SSL=ON brokers; this was corrected in v1.5.0.** Async TLS is not supported in v1.3.0. The connection guide also documents the UTF-8-only encoding contract.

### Upgrade notes

- **Async TLS limitation** — SSL/TLS for async connections raises `NotSupportedError` — `asyncio.loop.sock_*` APIs reject `SSLSocket`. Neither the synchronous nor asynchronous interface in this release provides a verified working TLS connection to an SSL=ON CUBRID broker. Use a later version containing the v1.5.0 broker-handshake correction for encrypted connections; otherwise async connections remain unencrypted. Tracked for future asyncio integration. Contributed by @yeongseon.

### Added

- **Sync TLS configuration API introduced (not yet functional end-to-end)** — Added `ssl=True`, `ssl=False`/`None` and custom `ssl.SSLContext` options to `pycubrid.connect()` (#85). On an SSL=ON CUBRID broker, this implementation begins TLS before the initial broker handshake, so encrypted connections are not yet operational. See the v1.5.0 handshake fix. Contributed by @yeongseon.
- **Reconnect / network edge case test suite** — 17 tests covering connection reset, timeout, broken pipe, partial reads, reconnect-after-failure (#87) by @yeongseon.
- **Concurrency stress tests** — threaded (16 workers × 25 inserts, 32 readers) and asyncio.gather (16 workers, 32 readers) with own-Connection isolation. Contributed by @yeongseon.
- **Standalone version check script** — `scripts/check_version.py` AST-based pyproject/`__init__.py` consistency check, replaces fragile inline grep in CI (#88) by @yeongseon.
- **PyPI classifiers** — `Operating System :: OS Independent`, `Typing :: Typed`, `Programming Language :: Python :: 3 :: Only` (#89) by @yeongseon.
- **Character encoding documentation** — UTF-8-only contract documented in `docs/CONNECTION.md` (#86) by @yeongseon.

### Fixed

- **PEP 639 license conflict** — removed redundant `License ::` classifier; SPDX `license = "MIT"` is the single source of truth (follow-up #89) by @yeongseon.
- **`test_ping_reconnect_also_fails` dual-stack fragility** — patches `socket.create_connection` instead of `socket.socket`. Contributed by @yeongseon.

### Documentation

- **Deferred implementation work (#90): Sync/async deduplication** — refactor deferred per Oracle review (high regression risk vs. maintainability gain) by @yeongseon.

## [1.2.0] - 2026-04-19

This release adds opt-in JSON and collection decoding, richer error details, connection health checks and improved sync/async behavior. **Compatibility note:** dictionary-style parameter binding is no longer accepted; use positional `?` parameters and a tuple or list.

### Upgrade notes

- **Positional parameters required** — The internal `_bind_parameters()` path now accepts `Sequence` parameters rather than `Mapping` values. Applications using dictionary-style bindings must switch to positional `?` placeholders and ordered tuple/list parameters before upgrading ([release commit 5f6e255](https://github.com/cubrid-lab/pycubrid/commit/5f6e255a20)) by @yeongseon.

### Added

- **Connection health and error details** — Added `Connection.ping()` using the CAS `CHECK_CAS` operation, plus `errno` and `sqlstate` metadata on `DatabaseError` with an error-to-SQLSTATE mapping table ([commit 3e0a330](https://github.com/cubrid-lab/pycubrid/commit/3e0a3305c8)) by @yeongseon.

- **Opt-in decoded values** — Added `json_deserializer` for JSON values and `decode_collections` for SET, MULTISET and SEQUENCE rather than requiring applications to handle raw wire data. The CAS protocol definition advanced from v7 to v8 ([commit 3e0a330](https://github.com/cubrid-lab/pycubrid/commit/3e0a3305c8)) by @yeongseon.

- **Cursor and network options** — Added `cursor.nextset()`, configurable `fetch_size`, timezone-aware `DATETIMETZ`/`TIMESTAMPTZ` wire decoding and `DATETIMETZ` SQL literal formatting (IANA timezone key when available, UTC offset fallback), `bytearray` parameters alongside `bytes`, async `read_timeout`, IPv4/IPv6 fallback and aligned sync/async binding ([commit f4a31e1](https://github.com/cubrid-lab/pycubrid/commit/f4a31e18f0)) by @yeongseon.

### Fixed

- **Cursor lifecycle and connection handling** — Removed duplicate cursor registration, narrowed best-effort close handling and improved sync timeout behavior and test isolation ([commit f4a31e1](https://github.com/cubrid-lab/pycubrid/commit/f4a31e18f0), [dce902a](https://github.com/cubrid-lab/pycubrid/commit/dce902ada6)) by @yeongseon.

### Security

- **Safer parameter interpolation** — Escapes backslashes and control characters, rejects embedded null bytes, and rejects non-finite floating-point values in formatted parameters. These changes harden SQL literal handling but do not introduce server-side prepared binding ([commits 3e0a330](https://github.com/cubrid-lab/pycubrid/commit/3e0a3305c8), [dce902a](https://github.com/cubrid-lab/pycubrid/commit/dce902ada6)) by @yeongseon.

## [1.1.0] - 2026-04-18

This release adds native `asyncio` database operations without introducing a separate runtime dependency. It also includes opt-in connection timing measurements and protocol memory-use improvements; existing synchronous applications can keep using the regular `pycubrid` API.

### Added

- **Native async connections and queries** — Added `pycubrid.aio.connect()`, `AsyncConnection` and `AsyncCursor` with async context managers, transaction methods, SQL execution, row fetching and iteration. The implementation uses nonblocking `asyncio` socket operations while sharing the existing CAS packet protocol ([commit 2ec57fe](https://github.com/cubrid-lab/pycubrid/commit/2ec57fe8d9e767e9ace12199d662288900e58a81)) by @yeongseon.

- **Optional timing statistics** — `enable_timing=True` or `PYCUBRID_ENABLE_TIMING=1` enables connection-level timing counters for connect, execute, fetch and close through `TimingStats`. Timing is off by default ([commit 0a04866](https://github.com/cubrid-lab/pycubrid/commit/0a04866d3258a974e71da11ee3fe36fef58cdb45)) by @yeongseon.

### Fixed

- **Short CAS type fields** — Corrected parsing of one-byte and two-byte CAS type values as part of the protocol handling improvements ([commit f01ea96](https://github.com/cubrid-lab/pycubrid/commit/f01ea96e50c8fa23f8ee7a82aa757b960c9bd84d)) by @yeongseon.

### Performance

- **Reduce packet allocation overhead** — Updated packet skipping, connection reads, buffer handling and result metadata layout to reduce intermediate allocations; `lastrowid` uses a dedicated insert-ID packet while retaining its integer-or-`None` result contract ([commit f01ea96](https://github.com/cubrid-lab/pycubrid/commit/f01ea96e50c8fa23f8ee7a82aa757b960c9bd84d)) by @yeongseon.

### Tests

- **Async regression coverage** — Added 30 offline tests for the new async connection and cursor functionality ([commit 2ec57fe](https://github.com/cubrid-lab/pycubrid/commit/2ec57fe8d9e767e9ace12199d662288900e58a81)) by @yeongseon.

## [1.0.0] - 2026-04-11

This is pycubrid's first release designated Production/Stable. At the time, the project announced a stable 1.x public API contract for Python 3.10–3.13 and listed CUBRID 11.2 and 11.4 as supported environments. The tagged driver uses **CAS protocol version 7**; the old canonical notes incorrectly said version 8.

### Upgrade notes

- **1.x stability commitment** — This release announced that incompatible changes to the public API would be reserved for a later major version. This describes the published commitment at v1.0.0; any subsequent changes to that policy are documented in their respective later releases ([release commit 0eb299c](https://github.com/cubrid-lab/pycubrid/commit/0eb299cae81bdff049d1865723b19bb56edf3bdc)) by @yeongseon.

### Changed

- **Stable package classification** — Updated the project's published development-status classifier from Beta to Production/Stable ([release commit 0eb299c](https://github.com/cubrid-lab/pycubrid/commit/0eb299cae81bdff049d1865723b19bb56edf3bdc)) by @yeongseon.

### Fixed

- **Static typing checks** — Fixed mypy and pyright errors by initializing response fields in prepare/execute packets, clarifying connection helper return types, and guarding cursor class initialization. These changes concern type safety, not an expanded database feature set ([release commit 0eb299c](https://github.com/cubrid-lab/pycubrid/commit/0eb299cae81bdff049d1865723b19bb56edf3bdc)) by @yeongseon.

### Documentation

- **Getting started and architecture** — Added architecture diagrams, a MkDocs documentation site, updated examples and quickstart/FAQ material in the changes leading up to the first stable version ([docs commit 3e8f162](https://github.com/cubrid-lab/pycubrid/commit/3e8f16251f)) by @yeongseon.

## [0.7.0] - 2026-04-04

This stabilization release documents the intended support boundary for the upcoming 1.0 series. It adds no new database runtime API: its main changes are support guidance and a correction to the connection documentation.

### Fixed

- **README connection guide description** — Removed an incorrect claim that pycubrid itself supplies a connection pool ([commit 4f7caf2](https://github.com/cubrid-lab/pycubrid/commit/4f7caf26)) by @yeongseon.

### Documentation

- **Supported versions and feature limits** — Added a Support Matrix describing the then-documented Python 3.10–3.13 and CUBRID 10.2–11.4 range, DB-API support, data-type behavior and known limitations. At this version, collection values are returned as raw bytes, JSON decoding and native async are unavailable, and there is no built-in connection pool ([commit 4f7caf2](https://github.com/cubrid-lab/pycubrid/commit/4f7caf26)) by @yeongseon.

- **External connection pooling** — The Connection guide now explains that each `pycubrid.connect()` establishes a connection and suggests SQLAlchemy's pool or an external pooling library when an application needs reuse ([commit 4f7caf2](https://github.com/cubrid-lab/pycubrid/commit/4f7caf26)) by @yeongseon.

## [0.6.0] - 2026-03-28

This release improves recovery when the broker releases a CAS session and reduces overhead for query fetching and batch DML. **Reconnection applies when the broker explicitly marks the CAS inactive**, not to every network failure.

### Added

- **Broker-signaled CAS reconnection** — Before requests, the driver checks `CAS_INFO_STATUS=INACTIVE` and reconnects when the broker has released the CAS. It also invalidates stale query handles across transaction boundaries to avoid using them on a replacement session ([commit eb5765d](https://github.com/cubrid-lab/pycubrid/commit/eb5765d2faefaa83ff837f8bad231ce892ee007d)) by @paikend (first contribution 🎉).

### Changed

- **Track current CAS state** — The request path now checks for broker-directed reconnection and updates `CAS_INFO` from each server response for the next operation ([commit eb5765d](https://github.com/cubrid-lab/pycubrid/commit/eb5765d2faefaa83ff837f8bad231ce892ee007d)) by @paikend.

### Fixed

- **DDL followed by DML** — Fixed the connection-lost receive error that could occur after CREATE or ALTER TABLE followed by DML on the same connection ([issue #23](https://github.com/cubrid-lab/pycubrid/issues/23), [commit eb5765d](https://github.com/cubrid-lab/pycubrid/commit/eb5765d2faefaa83ff837f8bad231ce892ee007d)) by @paikend.

- **Fetching subsequent results** — Passed column metadata to subsequent fetch packets, avoiding incomplete result decoding after the initial fetch ([commit bb687dc](https://github.com/cubrid-lab/pycubrid/commit/bb687dc37a4403804c7dfe36b1b096672a239d6d)) by @yeongseon.

### Performance

- **Faster buffered fetching and protocol handling** — Reused binary struct parsers, type-reader dispatch, direct slices of buffered rows, preallocated receive buffers and cached cursor lookup. Connections also set `TCP_NODELAY` and `SO_KEEPALIVE` ([commits 16a8634](https://github.com/cubrid-lab/pycubrid/commit/16a863497177d697179105f97b0869bc99f7aedd), [e348288](https://github.com/cubrid-lab/pycubrid/commit/e34828897ddab28acf0459bc2deb889a81f9c205)) by @yeongseon.

- **Batch DML** — `executemany()` sends a single batch request for data-changing SQL instead of a separate request for each parameter set. SELECT statements retain the per-row fallback so cursor results are handled separately ([commit e348288](https://github.com/cubrid-lab/pycubrid/commit/e34828897ddab28acf0459bc2deb889a81f9c205)) by @yeongseon.

- **Historical benchmark observations** — Original release measurements reported fetching 10,000 SELECT rows in 78 ms versus 96 ms previously, connecting in 1.66 ms versus 2.24 ms, and inserting in 7.10 ms versus 7.81 ms. These results depend on their original test environment and are not universal guarantees.

## [0.5.0] - 2026-03-12

This release documents using pycubrid with the separately installed SQLAlchemy dialect and fixes several CUBRID protocol and result-handling problems. **Compatibility note:** `Cursor.lastrowid` now returns `int | None` instead of `str | None`; callers expecting a string should adapt.

### Changed

- **Inserted-row ID type** — `Cursor.lastrowid` now obtains `LAST_INSERT_ID()` through SQL and returns an integer or `None`, rather than the previous string form. Code that parses or compares the value as text may need to change ([commit cc8753b](https://github.com/cubrid-lab/pycubrid/commit/cc8753b38722969ed9954758bf52995994ff762e)) by @yeongseon.

### Fixed

- **CAS responses and batch execution** — Corrected reply-frame length handling and broker protocol-version fields, and updated batch execution for newer protocol versions, including timeout and per-result parsing ([commit cc8753b](https://github.com/cubrid-lab/pycubrid/commit/cc8753b38722969ed9954758bf52995994ff762e)) by @yeongseon.

- **Data decoding and DB-API errors** — Corrected date/time wire-format handling and differentiated integrity-constraint and SQL-programming errors from general database errors ([commit cc8753b](https://github.com/cubrid-lab/pycubrid/commit/cc8753b38722969ed9954758bf52995994ff762e)) by @yeongseon.

### Documentation

- **SQLAlchemy connection setup** — Added README installation and usage examples for the separately installed `sqlalchemy-cubrid` dialect (identified as v2.1.0 at the time) with `cubrid+pycubrid://`. SQLAlchemy ORM and engine features come from that dialect, not a new pycubrid API ([commit 1d6c74c](https://github.com/cubrid-lab/pycubrid/commit/1d6c74c310fe188c176a599cb922279f60fa8496)) by @yeongseon.

### Tests

- **Live database coverage** — Added integration tests exercising the connection, cursor and data-handling behavior with CUBRID 11.2 ([commit cc8753b](https://github.com/cubrid-lab/pycubrid/commit/cc8753b38722969ed9954758bf52995994ff762e)) by @yeongseon.

## [0.4.0] - 2026-03-12

This release adds ways to work with CUBRID large objects, inspect database schema information and execute batches of SQL statements. It builds on the connection and cursor support introduced in v0.3.0. This version exists only as a Git tag and GitHub Release: it was never uploaded to PyPI, where the first available release is 0.5.0.

### Added

- **Read and write large objects** — Added the `Lob` API for BLOB and CLOB values, with creation, reading and writing. `Connection.create_lob()` creates a server-side LOB; `Lob` is also available from the package's top-level imports ([commit f3fd1ca](https://github.com/cubrid-lab/pycubrid/commit/f3fd1cac17a678a647c889d9c2d2870b29dbd36a)) by @yeongseon.

- **Inspect database structure** — Added `Connection.get_schema_info()` to retrieve schema information through the CAS protocol ([commit f3fd1ca](https://github.com/cubrid-lab/pycubrid/commit/f3fd1cac17a678a647c889d9c2d2870b29dbd36a)) by @yeongseon.

- **Execute SQL batches** — Added `Cursor.executemany_batch()` to send multiple SQL statements in one batch request, with an optional auto-commit setting ([commit f3fd1ca](https://github.com/cubrid-lab/pycubrid/commit/f3fd1cac17a678a647c889d9c2d2870b29dbd36a)) by @yeongseon.

## [0.3.0] - 2026-03-12

This release adds the first usable DB-API connection and cursor implementation on top of the CUBRID CAS protocol. It supports basic SQL queries and transactions; later versions expand the driver's capabilities. This version exists only as a Git tag and GitHub Release: it was never uploaded to PyPI, where the first available release is 0.5.0.

### Added

- **Connect to a database** — Added `Connection` with the CUBRID broker handshake, TCP socket handling and session setup, enabling the package's `connect()` function to establish a database connection ([commit 12974e3](https://github.com/cubrid-lab/pycubrid/commit/12974e396c75e7dd8c19e839b54789c3e2daf0d2)) by @yeongseon.

- **Execute SQL and read results** — Added `Cursor` with `execute()`, `executemany()`, `fetchone()`, `fetchmany()`, `fetchall()`, result `description` and `rowcount`. Client-side binding covers common Python values, including strings, numbers, bytes and date/time values ([commit 12974e3](https://github.com/cubrid-lab/pycubrid/commit/12974e396c75e7dd8c19e839b54789c3e2daf0d2)) by @yeongseon.

- **Manage transactions and resources** — Added `commit()`, `rollback()`, `autocommit` and `close()`, cursor/connection context managers, server-version and last-insert-ID helpers. `callproc()` executes a basic `CALL` with positional arguments but does not handle output parameters; `setinputsizes()` and `setoutputsize()` are no-op size hints ([commit 12974e3](https://github.com/cubrid-lab/pycubrid/commit/12974e396c75e7dd8c19e839b54789c3e2daf0d2)) by @yeongseon.

### Fixed

- **CAS reply parsing** — Corrected a double-parsing error by passing the response body without its length prefix to the packet parser ([commit 12974e3](https://github.com/cubrid-lab/pycubrid/commit/12974e396c75e7dd8c19e839b54789c3e2daf0d2)) by @yeongseon.

## [0.2.0] - 2026-03-12

This release introduces the binary protocol building blocks for communicating with a CUBRID broker, including packet formats and decoded result values. It is not yet a working database connector: the v0.2.0 tag still has no `pycubrid.connection` implementation. This version exists only as a Git tag and GitHub Release: it was never uploaded to PyPI, where the first available release is 0.5.0.

### Added

- **Binary packet framing** — Added `PacketWriter` and `PacketReader` for the length-prefixed, big-endian CAS wire format used by later database operations ([commit 8340a03](https://github.com/cubrid-lab/pycubrid/commit/8340a03acde19bfa954748bcb0ae996c639d2271)) by @yeongseon.

- **CAS request and response packets** — Implemented 19 packet classes for connection handshakes, statement preparation and execution, fetching rows, transaction operations, schema queries and large-object handling. These are protocol building blocks, not a new usable connection API ([commit 3846d98](https://github.com/cubrid-lab/pycubrid/commit/3846d98a7d541a914e7adb83e4cd6cd86b1934a7)) by @yeongseon.

- **Result decoding and metadata** — Added response-parsing helpers and the `ColumnMetaData` and `ResultInfo` types to represent query metadata and decode CUBRID wire values, including the supported data-type families ([commit 3846d98](https://github.com/cubrid-lab/pycubrid/commit/3846d98a7d541a914e7adb83e4cd6cd86b1934a7)) by @yeongseon.

## [0.1.0] - 2026-03-12

This first release establishes the pycubrid package and its DB-API 2.0 foundations, including error types and CUBRID protocol constants. The v0.1.0 tag does not yet contain the database connection implementation; usable connections and queries arrived in later releases. This version exists only as a Git tag and GitHub Release: it was never uploaded to PyPI, where the first available release is 0.5.0.

### Added

- **Python package setup** — Introduced the `pycubrid` package, installable from a source checkout, and its DB-API module attributes `apilevel`, `threadsafety` and `paramstyle`. Although `connect()` is exported, the v0.1.0 tag has no `pycubrid.connection` module, so it cannot open a working connection ([commit 9e4041a](https://github.com/cubrid-lab/pycubrid/commit/9e4041a22edee7a9ca8396f86c618240016a53b5)) by @yeongseon.

- **DB-API errors and type helpers** — Added the PEP 249 exception hierarchy, the `STRING`, `BINARY`, `NUMBER`, `DATETIME` and `ROWID` type objects, and standard date, time, timestamp and binary constructors ([commit 9e4041a](https://github.com/cubrid-lab/pycubrid/commit/9e4041a22edee7a9ca8396f86c618240016a53b5)) by @yeongseon.

- **CUBRID protocol definitions** — Added CAS function-code, database-type and isolation-level constants to support subsequent connection and query implementations ([commit 9e4041a](https://github.com/cubrid-lab/pycubrid/commit/9e4041a22edee7a9ca8396f86c618240016a53b5)) by @yeongseon.
