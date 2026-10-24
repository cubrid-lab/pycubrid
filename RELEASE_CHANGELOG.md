# Changelog

## [1.13.3](https://github.com/cubrid-lab/pycubrid/compare/v1.13.2...v1.13.3) (2026-10-24)


### CI

* **release:** verify against the current cookbook and restart after the abandoned 1.13.2 ([#1040](https://github.com/cubrid-lab/pycubrid/issues/1040)) ([be7817e](https://github.com/cubrid-lab/pycubrid/commit/be7817e17ec59397af105dfe193eee550afdd38c)), closes [#1016](https://github.com/cubrid-lab/pycubrid/issues/1016)

## [1.13.2](https://github.com/cubrid-lab/pycubrid/compare/v1.13.1...v1.13.2) (2026-10-24)


### Fixed

* **test:** isolate make integration from the manual Docker Compose project ([#1027](https://github.com/cubrid-lab/pycubrid/issues/1027)) ([588cd8a](https://github.com/cubrid-lab/pycubrid/commit/588cd8affe771e1ca7b741b7022d84fa94e69a58))


### Documentation

* **advanced:** introduce CUBRID CAS architecture and protocol concepts ([#1019](https://github.com/cubrid-lab/pycubrid/issues/1019)) ([dd70ef1](https://github.com/cubrid-lab/pycubrid/commit/dd70ef1f2d05aa7e9cf2d619eedd28a294e24b73))
* **agents:** require repository-local issue authoring checks ([#1014](https://github.com/cubrid-lab/pycubrid/issues/1014)) ([03ca2f9](https://github.com/cubrid-lab/pycubrid/commit/03ca2f96de47865cd3f596755b3bda8ae1156bae))
* enforce repository-local issue drafting ([03ca2f9](https://github.com/cubrid-lab/pycubrid/commit/03ca2f96de47865cd3f596755b3bda8ae1156bae))
* **triage:** clarify PR overlap and execution precedence ([d2c9318](https://github.com/cubrid-lab/pycubrid/commit/d2c93180672105b46cf1d29797cc5808386a85d6)), closes [#1025](https://github.com/cubrid-lab/pycubrid/issues/1025)

## [1.13.1](https://github.com/cubrid-lab/pycubrid/compare/v1.13.0...v1.13.1) (2026-10-21)


### Fixed

* **agent-eval:** copy the held worktree by descriptor and validate prepared_epoch ([#986](https://github.com/cubrid-lab/pycubrid/issues/986)) ([81f327d](https://github.com/cubrid-lab/pycubrid/commit/81f327d683f868ac53af6103fe65e82f7eef29b9))
* **agent-eval:** create and read the runner start record without following links ([#987](https://github.com/cubrid-lab/pycubrid/issues/987)) ([14ebed1](https://github.com/cubrid-lab/pycubrid/commit/14ebed12a1137ea1949286bea724e2446c9e6bd2))
* **agent-review:** do not count empty reply-only Copilot records as review rounds ([#949](https://github.com/cubrid-lab/pycubrid/issues/949)) ([10fb72c](https://github.com/cubrid-lab/pycubrid/commit/10fb72c65a40e522cc1c2d4e287a3e815431bb29)), closes [#948](https://github.com/cubrid-lab/pycubrid/issues/948)
* **aio:** keep error_source when copying a setup failure for waiting tasks ([#961](https://github.com/cubrid-lab/pycubrid/issues/961)) ([4cde43a](https://github.com/cubrid-lab/pycubrid/commit/4cde43afd2d84a4bdd4003ebcecdac01efc7f095)), closes [#952](https://github.com/cubrid-lab/pycubrid/issues/952)
* **binding:** keep subnormal floats and -0.0 on the literal path ([#989](https://github.com/cubrid-lab/pycubrid/issues/989)) ([2ed16bc](https://github.com/cubrid-lab/pycubrid/commit/2ed16bc1b117fc3837a39924d9d7925be08f3bf3)), closes [#988](https://github.com/cubrid-lab/pycubrid/issues/988)
* **context:** keep the original error when leaving a context after the session was retired ([#959](https://github.com/cubrid-lab/pycubrid/issues/959)) ([a535d9c](https://github.com/cubrid-lab/pycubrid/commit/a535d9cd5c9438289e03635532c6f7bf1afb14df))
* **docs:** close four parser gaps in the deterministic documentation check ([#940](https://github.com/cubrid-lab/pycubrid/issues/940)) ([703a1c2](https://github.com/cubrid-lab/pycubrid/commit/703a1c29ba4f76996a2574ade6a9fb6c89124203))
* **docs:** follow the configured site slugifier and toc id rules in the docs-check parser ([#976](https://github.com/cubrid-lab/pycubrid/issues/976)) ([a0f8260](https://github.com/cubrid-lab/pycubrid/commit/a0f826089d8275a881efffa59f7d77e43ba7664b)), closes [#975](https://github.com/cubrid-lab/pycubrid/issues/975)
* **docs:** handle adjacent shell operators, decoded titles and comment backslashes in the docs check ([#947](https://github.com/cubrid-lab/pycubrid/issues/947)) ([ebb3170](https://github.com/cubrid-lab/pycubrid/commit/ebb3170b582ac5f59ac80ef3d655c3aba3b7bba9))
* **docs:** read arithmetic, empty and alternated case forms correctly in the docs-check shell scanner ([#978](https://github.com/cubrid-lab/pycubrid/issues/978)) ([0edf209](https://github.com/cubrid-lab/pycubrid/commit/0edf209c30456b6ff81457b0225d8d245576d237))
* **docs:** sync the docs-check parser with sqlalchemy-cubrid ([#954](https://github.com/cubrid-lab/pycubrid/issues/954)) ([4495ffb](https://github.com/cubrid-lab/pycubrid/commit/4495ffbdf5bd4ffbe6e294f5d73b4ccf5a940855)), closes [#953](https://github.com/cubrid-lab/pycubrid/issues/953)
* **errors:** explain the CAS marker count for a ? in a quoted identifier ([#991](https://github.com/cubrid-lab/pycubrid/issues/991)) ([bb07791](https://github.com/cubrid-lab/pycubrid/commit/bb0779125793404ab90ff8e818e7ff33d5ec52a3)), closes [#990](https://github.com/cubrid-lab/pycubrid/issues/990)
* **lob:** reject LOB handles bound to a replaced physical session ([#934](https://github.com/cubrid-lab/pycubrid/issues/934)) ([03d7f70](https://github.com/cubrid-lab/pycubrid/commit/03d7f70871ca28c00dd3627f76bfbc77fbf06b0e))
* **profile:** create a per-run scratch table and clean up only what the run owns ([#962](https://github.com/cubrid-lab/pycubrid/issues/962)) ([b01a663](https://github.com/cubrid-lab/pycubrid/commit/b01a66336f1e7d608397daa20fb9e73c1e7f8cf4))
* **protocol:** decode an empty BIT VARYING collection element as b"" instead of None ([#999](https://github.com/cubrid-lab/pycubrid/issues/999)) ([224b4be](https://github.com/cubrid-lab/pycubrid/commit/224b4be6bfe809a693bfcc440fb61ef5c9ba1feb))
* **protocol:** fetch an empty BIT VARYING value as b"" instead of None ([#995](https://github.com/cubrid-lab/pycubrid/issues/995)) ([87ed36a](https://github.com/cubrid-lab/pycubrid/commit/87ed36ae0c7ebd1895791591794a20307e11fe9a))
* **test:** keep the CUBRID 11.4 Docker test database in a named volume ([#1004](https://github.com/cubrid-lab/pycubrid/issues/1004)) ([ec2d9ed](https://github.com/cubrid-lab/pycubrid/commit/ec2d9ed1720519b9f3c058cacaf730fbc9ad0195))
* **test:** keep the first databases.txt backup in docker-fix-db-host ([#1003](https://github.com/cubrid-lab/pycubrid/issues/1003)) ([4843d47](https://github.com/cubrid-lab/pycubrid/commit/4843d47ccfb9c3d9f299f58ab42e86658f849900)), closes [#1000](https://github.com/cubrid-lab/pycubrid/issues/1000)
* **test:** register the Docker test database for a stable host ([#1001](https://github.com/cubrid-lab/pycubrid/issues/1001)) ([48cbd78](https://github.com/cubrid-lab/pycubrid/commit/48cbd78e9e7d6971867a3c8bc3e9a23a7f2d2ba2))


### Documentation

* **agents:** name a maintainer's review as the standalone-agent alternative to Copilot ([#944](https://github.com/cubrid-lab/pycubrid/issues/944)) ([b99f02d](https://github.com/cubrid-lab/pycubrid/commit/b99f02d410cf0fcc8d946d08a4acf3d17e210b83)), closes [#943](https://github.com/cubrid-lab/pycubrid/issues/943)
* **contributing:** clarify good-first-issue access for human contributors ([#929](https://github.com/cubrid-lab/pycubrid/issues/929)) ([5b0bc2a](https://github.com/cubrid-lab/pycubrid/commit/5b0bc2a7672b719e000286e86bcf4fa77a7a2416))
* **errors:** record the sqlstate and result-type changes from the marker and float fixes ([#993](https://github.com/cubrid-lab/pycubrid/issues/993)) ([72f15c1](https://github.com/cubrid-lab/pycubrid/commit/72f15c14eb8c3ad85dac9e8aa781df6fc0f0e010)), closes [#992](https://github.com/cubrid-lab/pycubrid/issues/992)
* **onboarding:** guide CUBRID beginners from server preparation to their first saved row ([#956](https://github.com/cubrid-lab/pycubrid/issues/956)) ([bc4bffd](https://github.com/cubrid-lab/pycubrid/commit/bc4bffdf40f3d829b300932145ce7790f9fb5b1f))
* **quickstart:** remove the lab container's volume with docker rm -f -v ([#966](https://github.com/cubrid-lab/pycubrid/issues/966)) ([4efe130](https://github.com/cubrid-lab/pycubrid/commit/4efe1307031b43a300bbb7fd675292e14cedc933)), closes [#964](https://github.com/cubrid-lab/pycubrid/issues/964)
* **quickstart:** split the first query into its own lesson and map page roles ([#963](https://github.com/cubrid-lab/pycubrid/issues/963)) ([aca7dc2](https://github.com/cubrid-lab/pycubrid/commit/aca7dc20ee96cab2e5454945d6a255cb32e15720)), closes [#958](https://github.com/cubrid-lab/pycubrid/issues/958)
* **quickstart:** split the remaining lessons and group navigation by reader ([#965](https://github.com/cubrid-lab/pycubrid/issues/965)) ([5d23755](https://github.com/cubrid-lab/pycubrid/commit/5d237551c56f4f1f756c027d5c24021c83f1d29e)), closes [#958](https://github.com/cubrid-lab/pycubrid/issues/958)
* **release:** add a release review checklist to the runbook ([#968](https://github.com/cubrid-lab/pycubrid/issues/968)) ([4d5d84c](https://github.com/cubrid-lab/pycubrid/commit/4d5d84ccca773ec56090d30ada19364627ece29f))
* **release:** set last-release-sha when an abandoned release has no tag ([#930](https://github.com/cubrid-lab/pycubrid/issues/930)) ([2e97ee9](https://github.com/cubrid-lab/pycubrid/commit/2e97ee94357252084e80abdc7b8535c5ecf37eb6))
* **site:** keep wrapped lines that start with an issue number from rendering as headings ([#942](https://github.com/cubrid-lab/pycubrid/issues/942)) ([52c4ed4](https://github.com/cubrid-lab/pycubrid/commit/52c4ed435f1d508fcc569de1b2178f9926cdaa3b)), closes [#938](https://github.com/cubrid-lab/pycubrid/issues/938)
* **site:** label the site as main-branch documentation with its build commit ([#967](https://github.com/cubrid-lab/pycubrid/issues/967)) ([36f2532](https://github.com/cubrid-lab/pycubrid/commit/36f253215dd71468212aceb0afcf1d2e52c8bc6e)), closes [#960](https://github.com/cubrid-lab/pycubrid/issues/960)
* **troubleshooting:** clarify sync and async read timeouts ([#957](https://github.com/cubrid-lab/pycubrid/issues/957)) ([c324703](https://github.com/cubrid-lab/pycubrid/commit/c324703e05d2162cf27ba40a2ddbb5b83a6c61cb))

## [1.13.0](https://github.com/cubrid-lab/pycubrid/compare/v1.12.0...v1.13.0) (2026-10-14)


### Added

* **cursor:** deprecate fetchmany() sizes of 0 or less ([#876](https://github.com/cubrid-lab/pycubrid/issues/876)) ([fb30d0a](https://github.com/cubrid-lab/pycubrid/commit/fb30d0a6c3eeaa9d29e4409209b156f23ec7d081)), closes [#678](https://github.com/cubrid-lab/pycubrid/issues/678)
* **errors:** identify and classify CAS-generated and authentication error codes ([#915](https://github.com/cubrid-lab/pycubrid/issues/915)) ([d9435cc](https://github.com/cubrid-lab/pycubrid/commit/d9435cca61fa3bd55c03c47240fdf2fd3a1d9829)), closes [#903](https://github.com/cubrid-lab/pycubrid/issues/903)


### Fixed

* **aio:** keep get_schema_info() from dropping a session another task is setting up ([#902](https://github.com/cubrid-lab/pycubrid/issues/902)) ([ffa0069](https://github.com/cubrid-lab/pycubrid/commit/ffa006989c234aedf7b4f07f7d4b529342e7cb6f))
* **binding:** keep a negative number after a minus sign from starting a comment ([#901](https://github.com/cubrid-lab/pycubrid/issues/901)) ([626e2f4](https://github.com/cubrid-lab/pycubrid/commit/626e2f4dcff36ee50f0fe39c7a31fe4779e738af))
* **connection:** align ping() reconnect failures and redirect port checks across drivers ([#905](https://github.com/cubrid-lab/pycubrid/issues/905)) ([4562360](https://github.com/cubrid-lab/pycubrid/commit/4562360f3b80a1df5eb3b975f07825e87ce7f9e4)), closes [#896](https://github.com/cubrid-lab/pycubrid/issues/896)
* **cursor:** reject sub-minute offsets, detect verbs before comments and tighten Lob.write() ([#907](https://github.com/cubrid-lab/pycubrid/issues/907)) ([5ce7ae7](https://github.com/cubrid-lab/pycubrid/commit/5ce7ae7ebea0d4749ebf23529cd7dfdb5507e0d0))
* **cursor:** suggest the awaited fetchmany() replacement on the async cursor ([#879](https://github.com/cubrid-lab/pycubrid/issues/879)) ([0f507ca](https://github.com/cubrid-lab/pycubrid/commit/0f507cab93a080c3906703883ebfd9305de85eee)), closes [#678](https://github.com/cubrid-lab/pycubrid/issues/678)
* **errors:** map CAS error -4 to an interrupted statement, not a communication error ([#904](https://github.com/cubrid-lab/pycubrid/issues/904)) ([411bfd4](https://github.com/cubrid-lab/pycubrid/commit/411bfd4609998ce054c8b456a366d50954780976))
* **protocol:** reject malformed FC20/FC40 replies and map undecodable JSON to DataError ([#906](https://github.com/cubrid-lab/pycubrid/issues/906)) ([25a0708](https://github.com/cubrid-lab/pycubrid/commit/25a070838004584974ce6456db132ce9a262e2e6))
* **timing:** count executes and fetches consistently and as documented ([#908](https://github.com/cubrid-lab/pycubrid/issues/908)) ([46012b3](https://github.com/cubrid-lab/pycubrid/commit/46012b325e6e4cacade5fed5dc278ab6835697a0))


### Documentation

* **agents:** refresh the module map, test, docs and commit sections ([#877](https://github.com/cubrid-lab/pycubrid/issues/877)) ([6e05ce0](https://github.com/cubrid-lab/pycubrid/commit/6e05ce0fce91e92dde2c9e249dcf5eba9e8cac1f))
* **brand:** add shared Driver icon to README ([#920](https://github.com/cubrid-lab/pycubrid/issues/920)) ([05f1db3](https://github.com/cubrid-lab/pycubrid/commit/05f1db339de618113ee6305f5988dd71c47cf7db))
* **brand:** show the centered repository icon in the README and docs site ([#921](https://github.com/cubrid-lab/pycubrid/issues/921)) ([0719025](https://github.com/cubrid-lab/pycubrid/commit/0719025d48398a7b691c576c9501fa35ab06f13b))
* **development:** match the structure diagrams to the repository ([#880](https://github.com/cubrid-lab/pycubrid/issues/880)) ([f6a2734](https://github.com/cubrid-lab/pycubrid/commit/f6a2734655aca5506c4551f85387c50cc8fbec05))
* **i18n:** update the Python requirement in the German README ([#870](https://github.com/cubrid-lab/pycubrid/issues/870)) ([5a7638e](https://github.com/cubrid-lab/pycubrid/commit/5a7638e6634ef776632b57e4476e61713bb84a33)), closes [#702](https://github.com/cubrid-lab/pycubrid/issues/702)
* **i18n:** update the Python requirement in the Hindi README ([#871](https://github.com/cubrid-lab/pycubrid/issues/871)) ([cc0b879](https://github.com/cubrid-lab/pycubrid/commit/cc0b8794099754d4d08779a7c843df13bed6bd97)), closes [#703](https://github.com/cubrid-lab/pycubrid/issues/703)
* **i18n:** update the Python requirement in the Russian README ([#873](https://github.com/cubrid-lab/pycubrid/issues/873)) ([4f81fa3](https://github.com/cubrid-lab/pycubrid/commit/4f81fa3561d1a167679b9fc151bdb1b4539854f6)), closes [#704](https://github.com/cubrid-lab/pycubrid/issues/704)

## [1.12.0](https://github.com/cubrid-lab/pycubrid/compare/v1.11.0...v1.12.0) (2026-10-12)


### Added

* **cursor:** opt-in EXECUTE_ARRAY prepared batch for executemany() ([#848](https://github.com/cubrid-lab/pycubrid/issues/848)) ([7f1747d](https://github.com/cubrid-lab/pycubrid/commit/7f1747d006bf5ac13d009ac191794d6c5cc066ca))
* **cursor:** report which executemany() batch rows failed ([#852](https://github.com/cubrid-lab/pycubrid/issues/852)) ([b646cb6](https://github.com/cubrid-lab/pycubrid/commit/b646cb64873cf13c1eefc228559bb325dc549802))
* **protocol:** encode BIGINT, DOUBLE, NUMERIC, temporal and binary prepared parameters ([#846](https://github.com/cubrid-lab/pycubrid/issues/846)) ([29fd19c](https://github.com/cubrid-lab/pycubrid/commit/29fd19cafc3af665c2f25664bca6caceff03b922))
* **typing:** annotate context manager and iterator returns with typing.Self ([#773](https://github.com/cubrid-lab/pycubrid/issues/773)) ([a441d70](https://github.com/cubrid-lab/pycubrid/commit/a441d7092ab254b47aa4ae052d15238e669861de))


### Performance

* **cursor:** split the executemany() SQL on its placeholders once per call ([#853](https://github.com/cubrid-lab/pycubrid/issues/853)) ([98b8263](https://github.com/cubrid-lab/pycubrid/commit/98b8263ba8d17f022e232524c06590d80601b7da))


### Documentation

* **i18n:** update the Python requirement in the Chinese README ([#847](https://github.com/cubrid-lab/pycubrid/issues/847)) ([f0b09d2](https://github.com/cubrid-lab/pycubrid/commit/f0b09d240fadf13ec70787152b7a2a474880d349))

## [1.11.0](https://github.com/cubrid-lab/pycubrid/compare/v1.10.0...v1.11.0) (2026-10-11)


### Added

* **compat:** accept collection parameters in wrapper execute and executemany ([#813](https://github.com/cubrid-lab/pycubrid/issues/813)) ([1accfaa](https://github.com/cubrid-lab/pycubrid/commit/1accfaa22945d3bd868f5940a68f085dae32a3ec))
* **python:** declare Python 3.15 support ([#831](https://github.com/cubrid-lab/pycubrid/issues/831)) ([0563613](https://github.com/cubrid-lab/pycubrid/commit/056361386e3cb41af0558bd7cd974eb07d75a0a1))


### Fixed

* **collect_repro:** redact passwords of CUBRID_TEST_URL values without "://" ([#778](https://github.com/cubrid-lab/pycubrid/issues/778)) ([dc0683f](https://github.com/cubrid-lab/pycubrid/commit/dc0683fd27ad1a7f0500b640baa0c74276c1030c))
* **collect_repro:** treat non-IPv6 bracketed hosts as unparseable ([#776](https://github.com/cubrid-lab/pycubrid/issues/776)) ([164d0d5](https://github.com/cubrid-lab/pycubrid/commit/164d0d58819aea81281606708e3acfa07492c916))
* **types:** bind the second occurrence of a repeated DST hour as itself ([#820](https://github.com/cubrid-lab/pycubrid/issues/820)) ([2228d66](https://github.com/cubrid-lab/pycubrid/commit/2228d663b6e2c9462c648b9dd941720219c464ae))


### Documentation

* **agents:** move stale planning context out of AGENTS.md ([#779](https://github.com/cubrid-lab/pycubrid/issues/779)) ([98acc87](https://github.com/cubrid-lab/pycubrid/commit/98acc8707dea72576c2394a3cd87487fe0e23871))
* **aio:** correct Python version notes in TLS docstrings ([#824](https://github.com/cubrid-lab/pycubrid/issues/824)) ([b20a1b6](https://github.com/cubrid-lab/pycubrid/commit/b20a1b63a4f4e23527b44504924510a980cecefd))
* **dev:** point the development guide to the current CI cost evidence ([#808](https://github.com/cubrid-lab/pycubrid/issues/808)) ([7d43169](https://github.com/cubrid-lab/pycubrid/commit/7d43169ee2beba9ecad700829a119460254389c3)), closes [#750](https://github.com/cubrid-lab/pycubrid/issues/750)
* explain the executemany() gap against CUBRIDdb in the performance guide ([#838](https://github.com/cubrid-lab/pycubrid/issues/838)) ([f8d906d](https://github.com/cubrid-lab/pycubrid/commit/f8d906df0eb0ad0099e1e0342bf702937ae7ec2c))
* name private vulnerability reporting as the preferred security channel ([#816](https://github.com/cubrid-lab/pycubrid/issues/816)) ([fe49031](https://github.com/cubrid-lab/pycubrid/commit/fe4903168c903c5680c44be3010bccd1a137da29)), closes [#751](https://github.com/cubrid-lab/pycubrid/issues/751)
* **performance:** focus the guide on pycubrid on CUBRID and mark historical numbers ([#802](https://github.com/cubrid-lab/pycubrid/issues/802)) ([7cc4b20](https://github.com/cubrid-lab/pycubrid/commit/7cc4b20f5e0997d71da0568e35c3901358f45c1e)), closes [#795](https://github.com/cubrid-lab/pycubrid/issues/795)
* publish the current performance baseline ([#823](https://github.com/cubrid-lab/pycubrid/issues/823)) ([35770b1](https://github.com/cubrid-lab/pycubrid/commit/35770b10346f4bae644e2af61da9b2550a39760a))
* **python:** state the Python 3.11+ requirement in the connection guide and PRD ([#825](https://github.com/cubrid-lab/pycubrid/issues/825)) ([4f6c3ff](https://github.com/cubrid-lab/pycubrid/commit/4f6c3ff69393e23689e19850f548d32ce19f15fa))
* **python:** state the Python 3.11+ requirement in the contributor and development guides ([#826](https://github.com/cubrid-lab/pycubrid/issues/826)) ([d727224](https://github.com/cubrid-lab/pycubrid/commit/d72722467c81cb690f002c921dc2e468110192fe)), closes [#700](https://github.com/cubrid-lab/pycubrid/issues/700)
* **readme:** describe the full compatibility matrix as 4 × 4 on dispatch and release ([#803](https://github.com/cubrid-lab/pycubrid/issues/803)) ([d30bedb](https://github.com/cubrid-lab/pycubrid/commit/d30bedb55c74aa97b96a790f607d7c41967f06e3))
* record the prepared batch evaluation for executemany() in the performance guide ([#843](https://github.com/cubrid-lab/pycubrid/issues/843)) ([6b28f5b](https://github.com/cubrid-lab/pycubrid/commit/6b28f5b943628bac0ddb2a7ac1508feebe40ef08))
* **release:** list timed-out runs as rerunnable in the blocked-preparation recovery ([#800](https://github.com/cubrid-lab/pycubrid/issues/800)) ([2ff0d39](https://github.com/cubrid-lab/pycubrid/commit/2ff0d390c7241842a868d648971395c79f0ed7c3))
* **release:** skip the green-publisher step for an abandoned release ([#806](https://github.com/cubrid-lab/pycubrid/issues/806)) ([f66e8de](https://github.com/cubrid-lab/pycubrid/commit/f66e8de1675e8f5dfa53d7e55d877f5d4e15d034)), closes [#801](https://github.com/cubrid-lab/pycubrid/issues/801)
* **review:** specify tool-based review routes and thread closure ([#821](https://github.com/cubrid-lab/pycubrid/issues/821)) ([5f5fef1](https://github.com/cubrid-lab/pycubrid/commit/5f5fef17dd503140990dfbc13011ac37244ddd38))
* update Python requirement in LLM index ([#804](https://github.com/cubrid-lab/pycubrid/issues/804)) ([0bc0a21](https://github.com/cubrid-lab/pycubrid/commit/0bc0a214cc48f12fb204a610581df95fd1060d8c))

## [1.10.0](https://github.com/cubrid-lab/pycubrid/compare/v1.9.0...v1.10.0) (2026-10-08)


### Features

* **python:** require Python 3.11 or later ([#708](https://github.com/cubrid-lab/pycubrid/issues/708)) ([5b3d9ec](https://github.com/cubrid-lab/pycubrid/commit/5b3d9ecfe8632c5194d1201e9540e4fdf8b8d98e)), closes [#684](https://github.com/cubrid-lab/pycubrid/issues/684)


### Bug Fixes

* **aio:** restore asyncio.wait_for for driver deadlines on Python 3.11.0–3.11.2 ([#754](https://github.com/cubrid-lab/pycubrid/issues/754)) ([1a44790](https://github.com/cubrid-lab/pycubrid/commit/1a44790cd5e3a6c37357fa42783ef86533106fab))


### Documentation

* **agents:** align contributor ownership and good-first-issue guardrails ([#752](https://github.com/cubrid-lab/pycubrid/issues/752)) ([fb3da8c](https://github.com/cubrid-lab/pycubrid/commit/fb3da8c94b86cf1ad576989f84c6ba465b9e0894)), closes [#743](https://github.com/cubrid-lab/pycubrid/issues/743)
* **api:** document get_error_description and close the Korean API reference gaps ([#717](https://github.com/cubrid-lab/pycubrid/issues/717)) ([221339e](https://github.com/cubrid-lab/pycubrid/commit/221339eec34912ec0a0adec665673a25541fccdf)), closes [#711](https://github.com/cubrid-lab/pycubrid/issues/711)
* **ci:** correct what enable-cache auto does on the release path ([#764](https://github.com/cubrid-lab/pycubrid/issues/764)) ([e0e475b](https://github.com/cubrid-lab/pycubrid/commit/e0e475b62741f22a3d6deeed56c7e8ba1cbf3007)), closes [#759](https://github.com/cubrid-lab/pycubrid/issues/759)
* **i18n:** add the Korean documentation home page ([#720](https://github.com/cubrid-lab/pycubrid/issues/720)) ([73e4ea7](https://github.com/cubrid-lab/pycubrid/commit/73e4ea756a1efda3d09e43e22b6ff13aa585174a)), closes [#714](https://github.com/cubrid-lab/pycubrid/issues/714)
* **i18n:** correct Korean sections that fell behind the English wording ([#727](https://github.com/cubrid-lab/pycubrid/issues/727)) ([ed6e7d9](https://github.com/cubrid-lab/pycubrid/commit/ed6e7d934e2931ebd286761e5b9a69e327ce9db9)), closes [#726](https://github.com/cubrid-lab/pycubrid/issues/726)
* **i18n:** synchronize the Korean CONNECTION, DEVELOPMENT and TROUBLESHOOTING guides ([#718](https://github.com/cubrid-lab/pycubrid/issues/718)) ([16a0665](https://github.com/cubrid-lab/pycubrid/commit/16a06659597ea9a93b899291a480d0a2e1050d8b)), closes [#712](https://github.com/cubrid-lab/pycubrid/issues/712)
* **i18n:** synchronize the Korean SUPPORT_MATRIX and translate CI_POLICY in full ([#719](https://github.com/cubrid-lab/pycubrid/issues/719)) ([66e2c5e](https://github.com/cubrid-lab/pycubrid/commit/66e2c5e346c3f270bff3cfc0c0b2797648bccc2a)), closes [#713](https://github.com/cubrid-lab/pycubrid/issues/713)
* **i18n:** translate PRD and PREPARED_BINDING_DESIGN into Korean ([#722](https://github.com/cubrid-lab/pycubrid/issues/722)) ([328720d](https://github.com/cubrid-lab/pycubrid/commit/328720d4044d53db8288f95f74f8f9e5894b6a36)), closes [#715](https://github.com/cubrid-lab/pycubrid/issues/715)
* **licenses:** make the third-party license inventory reproducible and accurate ([#771](https://github.com/cubrid-lab/pycubrid/issues/771)) ([aacb5b5](https://github.com/cubrid-lab/pycubrid/commit/aacb5b588be7a15f4126a76a20f77334828fd5a4))
* **prd:** replace stale version, test and coverage figures with links ([#725](https://github.com/cubrid-lab/pycubrid/issues/725)) ([8a3304b](https://github.com/cubrid-lab/pycubrid/commit/8a3304bc7acebeb4368e24d3275940e6ef3839ed)), closes [#724](https://github.com/cubrid-lab/pycubrid/issues/724)
* **python:** state Python 3.11 or later in the README and quickstart ([#739](https://github.com/cubrid-lab/pycubrid/issues/739)) ([27e35be](https://github.com/cubrid-lab/pycubrid/commit/27e35be9eee6518c1c673e51195fa25954dd5da4)), closes [#699](https://github.com/cubrid-lab/pycubrid/issues/699)
* **site:** generate heading anchors that match the links in the documents ([#729](https://github.com/cubrid-lab/pycubrid/issues/729)) ([c2a4f1d](https://github.com/cubrid-lab/pycubrid/commit/c2a4f1db875c65a3c497daf6427711d643401dec)), closes [#728](https://github.com/cubrid-lab/pycubrid/issues/728)
* **tls:** remove Python 3.10 preflight probe guidance ([#737](https://github.com/cubrid-lab/pycubrid/issues/737)) ([4df5db0](https://github.com/cubrid-lab/pycubrid/commit/4df5db0331c2072440d22b0b08ed20323ecaec93)), closes [#686](https://github.com/cubrid-lab/pycubrid/issues/686)

## [1.9.0](https://github.com/cubrid-lab/pycubrid/compare/v1.8.0...v1.9.0) (2026-10-04)


### Features

* add charset connection option ([#510](https://github.com/cubrid-lab/pycubrid/issues/510)) ([3874252](https://github.com/cubrid-lab/pycubrid/commit/387425245e70c63e6b5d99265d5e8cbca61affb0))
* **compat:** add fixed-session connection utilities ([#667](https://github.com/cubrid-lab/pycubrid/issues/667)) ([9fbbc05](https://github.com/cubrid-lab/pycubrid/commit/9fbbc05b494e52197907bea138b36a3380ff6421)), closes [#666](https://github.com/cubrid-lab/pycubrid/issues/666)
* **compat:** add native cursor seek and tell ([ff8eee7](https://github.com/cubrid-lab/pycubrid/commit/ff8eee7fd4c8c403b48000747293bf55af3bb3c3)), closes [#444](https://github.com/cubrid-lab/pycubrid/issues/444) [#444](https://github.com/cubrid-lab/pycubrid/issues/444)
* **compat:** bind collections with official native set API ([#607](https://github.com/cubrid-lab/pycubrid/issues/607)) ([c316c39](https://github.com/cubrid-lab/pycubrid/commit/c316c39a3c7ef993c541b93114380dc10ed778dc))
* **compat:** expose cached result_info without moving rows ([#642](https://github.com/cubrid-lab/pycubrid/issues/642)) ([ee62e28](https://github.com/cubrid-lab/pycubrid/commit/ee62e28a5f316e779da20cfe575a1cd0ed2dda30))
* **compat:** expose wrapper transaction boundaries ([#664](https://github.com/cubrid-lab/pycubrid/issues/664)) ([44aafb9](https://github.com/cubrid-lab/pycubrid/commit/44aafb9ed7cf413eb558d0a3709c88fcbb19e38c)), closes [#662](https://github.com/cubrid-lab/pycubrid/issues/662)
* **compat:** fetch and bind LOB handles with official native API ([#616](https://github.com/cubrid-lab/pycubrid/issues/616)) ([068325e](https://github.com/cubrid-lab/pycubrid/commit/068325e75abb068a6b9d4f9bc841038cb7743a1e))
* **compat:** preserve official wrapper rows on supported scalars ([2215d0a](https://github.com/cubrid-lab/pycubrid/commit/2215d0a739252a25e93a399a9e3fc84a96501f48))
* **compat:** separate cached native settings from effective setters ([7a22678](https://github.com/cubrid-lab/pycubrid/commit/7a226783103136fa518f5a479622799b2bea8b4d))
* **compat:** separate native LOB streams from ordinary offset APIs ([bebef4d](https://github.com/cubrid-lab/pycubrid/commit/bebef4d3b9abdc65feef62d770d014e8ececff4c))
* **driver:** add typed Set, Multiset and Sequence parameters for ordinary cursors ([#568](https://github.com/cubrid-lab/pycubrid/issues/568)) ([17cc5dc](https://github.com/cubrid-lab/pycubrid/commit/17cc5dca98ee33ad052ce5ba8d04bfe6b9826812))
* preserve native LOB file workflows on failure ([#653](https://github.com/cubrid-lab/pycubrid/issues/653)) ([5d40a0f](https://github.com/cubrid-lab/pycubrid/commit/5d40a0f8bfc1f51ff43ab4b1c8bd3b119783a999)), closes [#443](https://github.com/cubrid-lab/pycubrid/issues/443)
* **protocol:** add the internal typed collection binding wire contract ([#569](https://github.com/cubrid-lab/pycubrid/issues/569)) ([bf53d2c](https://github.com/cubrid-lab/pycubrid/commit/bf53d2ca45290533ad8d6b47cbb54c4a9ba533c5))


### Bug Fixes

* **aio:** bound async TLS connect when the handshake stalls or is reset ([#534](https://github.com/cubrid-lab/pycubrid/issues/534)) ([bd878fe](https://github.com/cubrid-lab/pycubrid/commit/bd878fe1af7b4e0eb5e76e47c9da3407d5394944))
* **async:** isolate setup cancellation from waiting operations ([#578](https://github.com/cubrid-lab/pycubrid/issues/578)) ([7f5fe22](https://github.com/cubrid-lab/pycubrid/commit/7f5fe229000ebec13bc14427f0dfe277acfb5b09))
* **binding:** keep invalid timezone failures within DB-API errors ([4fcd9c8](https://github.com/cubrid-lab/pycubrid/commit/4fcd9c82be27e56fcdf1ab8d992c56d1d8b0c994))
* **changelog:** reject duplicate release subsections ([#648](https://github.com/cubrid-lab/pycubrid/issues/648)) ([3c57bb9](https://github.com/cubrid-lab/pycubrid/commit/3c57bb9ffab347f5c57e27d080d4a9b205c64083))
* **ci:** report a skipped cookbook verification as failed in the release summary ([#548](https://github.com/cubrid-lab/pycubrid/issues/548)) ([45f3374](https://github.com/cubrid-lab/pycubrid/commit/45f3374b4cfa9d191316c935b3801e827aa94b1b))
* classify foreign-key restrict failures (-924, -1284) as IntegrityError ([95a34e6](https://github.com/cubrid-lab/pycubrid/commit/95a34e6fcb6dcca27ed10a11e24aadb18cb34c2d))
* clear previous results after failed execute ([#531](https://github.com/cubrid-lab/pycubrid/issues/531)) ([32195b1](https://github.com/cubrid-lab/pycubrid/commit/32195b1f856b8d499a6e45ebfb89ee4a7ebc57ea))
* **compat:** refresh failed plans only on the next explicit execution ([e39e00e](https://github.com/cubrid-lab/pycubrid/commit/e39e00e8e61b2c04f3e8250a7b82cde1a3f6a82a))
* **connection:** preserve deferred handle ownership across interrupts ([1686920](https://github.com/cubrid-lab/pycubrid/commit/16869201ca04a8b9e0b4ee5e73cb996bb38c0625))
* **connection:** retire cursor handles on transport failures and clarify timeout errors ([3d72216](https://github.com/cubrid-lab/pycubrid/commit/3d72216ff807f9a1fe6d7292cf754f44359b2626)), closes [#556](https://github.com/cubrid-lab/pycubrid/issues/556)
* **cursor:** retire pooling-off handles at transaction replies ([#598](https://github.com/cubrid-lab/pycubrid/issues/598)) ([1251165](https://github.com/cubrid-lab/pycubrid/commit/125116574a7eeb05c942bcc24563be28eef9ee2b)), closes [#584](https://github.com/cubrid-lab/pycubrid/issues/584)
* DBAPIType should not treat bool values as integer type ([#479](https://github.com/cubrid-lab/pycubrid/issues/479)) ([4efe599](https://github.com/cubrid-lab/pycubrid/commit/4efe599d29965e606e3ce77682c162be9830fcd7)), closes [#369](https://github.com/cubrid-lab/pycubrid/issues/369) [#369](https://github.com/cubrid-lab/pycubrid/issues/369)
* decode nonempty NULL-only collection results ([c588055](https://github.com/cubrid-lab/pycubrid/commit/c588055f9bbe31ae6872430bd75aede26060cbec))
* decode nonempty NULL-only collection results ([e379116](https://github.com/cubrid-lab/pycubrid/commit/e379116e618791b8a65a060c108139954ff3fea9)), closes [#483](https://github.com/cubrid-lab/pycubrid/issues/483)
* **driver:** keep already fetched rows when a later fetch page raises DataError ([#536](https://github.com/cubrid-lab/pycubrid/issues/536)) ([ef815f8](https://github.com/cubrid-lab/pycubrid/commit/ef815f853b5167daae016416c05701b4a420332e))
* **driver:** keep the autocommit setter's requests on one CAS session ([#552](https://github.com/cubrid-lab/pycubrid/issues/552)) ([7ac5a3e](https://github.com/cubrid-lab/pycubrid/commit/7ac5a3e528efb97516f638d607e5f3547fc3d69b))
* **driver:** render Decimal parameters in plain notation so CUBRID keeps them NUMERIC ([#526](https://github.com/cubrid-lab/pycubrid/issues/526)) ([a7a3563](https://github.com/cubrid-lab/pycubrid/commit/a7a3563e4ee2fecc06c9208ecbe42392f575749f))
* **driver:** render int, float and Decimal subclasses by value instead of str() ([#527](https://github.com/cubrid-lab/pycubrid/issues/527)) ([0577cd8](https://github.com/cubrid-lab/pycubrid/commit/0577cd82e1de3858b3694b0105f3f13c622082f9)), closes [#518](https://github.com/cubrid-lab/pycubrid/issues/518)
* **driver:** render str, bytes, date and time parameters without calling overridable methods ([#529](https://github.com/cubrid-lab/pycubrid/issues/529)) ([dde9a07](https://github.com/cubrid-lab/pycubrid/commit/dde9a071f44ae275a5db486dea8ecf1521889761))
* **integration:** preserve volumes during automatic Docker cleanup ([#647](https://github.com/cubrid-lab/pycubrid/issues/647)) ([f3b2e69](https://github.com/cubrid-lab/pycubrid/commit/f3b2e6983e62ae3d5d72149188d3357de96e5445))
* keep the session when a complete broker reply carries invalid UTF-8 ([734fb29](https://github.com/cubrid-lab/pycubrid/commit/734fb294276927fa50c6887e99871a82a2c8eebc))
* **lob:** keep the handle size current and add internal LOB-handle binding ([#615](https://github.com/cubrid-lab/pycubrid/issues/615)) ([35d4e2c](https://github.com/cubrid-lab/pycubrid/commit/35d4e2cbedbea85bbdac0d76d047572e5cffb6ff))
* **protocol:** avoid blaming following tokens for reserved identifiers ([5fde41f](https://github.com/cubrid-lab/pycubrid/commit/5fde41f37dd030f7c6098f3a7a8d70a9ecbc5f4a))
* **protocol:** decode CALL and NULL-typed cells with the two-byte type header ([#549](https://github.com/cubrid-lab/pycubrid/issues/549)) ([e329a11](https://github.com/cubrid-lab/pycubrid/commit/e329a117c153d891e85b53a1cfd24580ca38766e))
* **protocol:** finish framing validation before raising DataError and close remaining FC41 count gaps ([#588](https://github.com/cubrid-lab/pycubrid/issues/588)) ([d19a981](https://github.com/cubrid-lab/pycubrid/commit/d19a9819067262d64f3c0e2f09a009edd7a5fc71)), closes [#581](https://github.com/cubrid-lab/pycubrid/issues/581)
* **protocol:** keep collection errors from hiding malformed elements ([#597](https://github.com/cubrid-lab/pycubrid/issues/597)) ([1a93ecf](https://github.com/cubrid-lab/pycubrid/commit/1a93ecf6b69012dd8a3a85be9d16a65898d5ec60)), closes [#591](https://github.com/cubrid-lab/pycubrid/issues/591) [#595](https://github.com/cubrid-lab/pycubrid/issues/595)
* **protocol:** raise DataError for invalid JSON in a complete reply ([#571](https://github.com/cubrid-lab/pycubrid/issues/571)) ([c1a5de5](https://github.com/cubrid-lab/pycubrid/commit/c1a5de55db5a5ecca164bb6826e55726dd56f8aa))
* **protocol:** raise DataError for zero DATE and DATETIME values instead of closing the connection ([#532](https://github.com/cubrid-lab/pycubrid/issues/532)) ([5357f66](https://github.com/cubrid-lab/pycubrid/commit/5357f660a39ef048e4d90f06d38e91a47b742bc8))
* **protocol:** reject byte reads past the end of a broker reply ([#533](https://github.com/cubrid-lab/pycubrid/issues/533)) ([4605671](https://github.com/cubrid-lab/pycubrid/commit/4605671725dce24c895996770ed29d51bb1adb25))
* **protocol:** reject negative FC41 metadata lengths and column counts ([#574](https://github.com/cubrid-lab/pycubrid/issues/574)) ([a341cec](https://github.com/cubrid-lab/pycubrid/commit/a341cec58f36d33b8ae75734b3854619c7287911)), closes [#555](https://github.com/cubrid-lab/pycubrid/issues/555)
* **protocol:** validate reply tails before metadata DataError ([#596](https://github.com/cubrid-lab/pycubrid/issues/596)) ([62f5866](https://github.com/cubrid-lab/pycubrid/commit/62f5866898818faa9146249712bc40a0fef80f4f)), closes [#591](https://github.com/cubrid-lab/pycubrid/issues/591)
* raise DataError for unresolvable TZ zones and honor DST abbreviations ([180dd56](https://github.com/cubrid-lab/pycubrid/commit/180dd56b1b48c9fb6649285b109d8d7dbfaa752b))
* raise DataError for unresolvable TZ zones and honor DST abbreviations ([209b201](https://github.com/cubrid-lab/pycubrid/commit/209b20154591783e80dbb5c758ef4629dfc3367d)), closes [#413](https://github.com/cubrid-lab/pycubrid/issues/413)
* reject malformed fetch sizes before rows or pages are consumed ([#679](https://github.com/cubrid-lab/pycubrid/issues/679)) ([8dd1198](https://github.com/cubrid-lab/pycubrid/commit/8dd11989167594f36774da824224064073883240)), closes [#371](https://github.com/cubrid-lab/pycubrid/issues/371)
* reject malformed qualified callproc names ([#515](https://github.com/cubrid-lab/pycubrid/issues/515)) ([601a879](https://github.com/cubrid-lab/pycubrid/commit/601a879f52b951ccdc366d95fde47b3d2d84b75a)), closes [#372](https://github.com/cubrid-lab/pycubrid/issues/372) [#372](https://github.com/cubrid-lab/pycubrid/issues/372)
* **test:** fail instead of skip when a configured CUBRID endpoint is unreachable ([#541](https://github.com/cubrid-lab/pycubrid/issues/541)) ([68851c9](https://github.com/cubrid-lab/pycubrid/commit/68851c9aa9b125496250f7fff2ae6c4a3821f2b5))
* tighten TZ fold selection and reject out-of-range offsets ([98d8150](https://github.com/cubrid-lab/pycubrid/commit/98d8150403a525dc27f7d6003d8f5944984cdadc))
* **timeouts:** validate configuration before transport acquisition ([#646](https://github.com/cubrid-lab/pycubrid/issues/646)) ([7577e4d](https://github.com/cubrid-lab/pycubrid/commit/7577e4d5c33911ee7c7ca7707fbd37e2ad9e04d4))
* **tls:** bound sync TLS handshakes without read_timeout and close sockets on the Python 3.10 pre-connect path ([#586](https://github.com/cubrid-lab/pycubrid/issues/586)) ([3af12ae](https://github.com/cubrid-lab/pycubrid/commit/3af12ae7d97ea6e1c5e30e616babc0f54fc7ddd0))
* **tls:** preserve probe failures while notifying TLS peers ([fa7b22a](https://github.com/cubrid-lab/pycubrid/commit/fa7b22ad4707510830d8cccb1518c3a875d1c330))
* **tls:** preserve the total verification probe deadline ([#594](https://github.com/cubrid-lab/pycubrid/issues/594)) ([7dddab3](https://github.com/cubrid-lab/pycubrid/commit/7dddab31c22f8bb8b837c30d6e942d483abe5f10)), closes [#593](https://github.com/cubrid-lab/pycubrid/issues/593)
* **types:** make typed collection parameters truly immutable, copyable and picklable ([#580](https://github.com/cubrid-lab/pycubrid/issues/580)) ([882b704](https://github.com/cubrid-lab/pycubrid/commit/882b704165ac7d1b614c2831995a8071c3b654ac))
* validate LOB offset and length types before serialization ([#516](https://github.com/cubrid-lab/pycubrid/issues/516)) ([dad392e](https://github.com/cubrid-lab/pycubrid/commit/dad392e8396ae82c7db962671c1c714b3dfe2dd5))
* validate size of empty NULL-type collections ([775f38c](https://github.com/cubrid-lab/pycubrid/commit/775f38c1d2b2422617ae8546ce11b3ccd445bc7f))


### Performance Improvements

* **protocol:** benchmark fetch parsing and trim per-cell overhead ([#602](https://github.com/cubrid-lab/pycubrid/issues/602)) ([cfecfba](https://github.com/cubrid-lab/pycubrid/commit/cfecfba3cc1251b38465669427352579691e3b17)), closes [#559](https://github.com/cubrid-lab/pycubrid/issues/559)
* release unclosed cursor handles in autocommit mode with a deferred close queue ([fe222fb](https://github.com/cubrid-lab/pycubrid/commit/fe222fb51ebb6fccf69f4d88941a1ecff9d82479)), closes [#488](https://github.com/cubrid-lab/pycubrid/issues/488)


### Reverts

* release 1.9.0 ([#672](https://github.com/cubrid-lab/pycubrid/issues/672)) ([cf2fe9d](https://github.com/cubrid-lab/pycubrid/commit/cf2fe9d474a02a56234e13c5992fbcd8e5c362a1))


### Documentation

* anchor current verification claims to maintained sources ([#654](https://github.com/cubrid-lab/pycubrid/issues/654)) ([127c6a1](https://github.com/cubrid-lab/pycubrid/commit/127c6a1f5bcf67eeefbcd6623a577887745b304e)), closes [#415](https://github.com/cubrid-lab/pycubrid/issues/415)
* CHANGELOG, RELEASE_POLICY, PROTOCOL (EN+KO), regenerated llms-full. ([a341cec](https://github.com/cubrid-lab/pycubrid/commit/a341cec58f36d33b8ae75734b3854619c7287911))
* **contributing:** keep issue specifications current and assign owners ([ea32de4](https://github.com/cubrid-lab/pycubrid/commit/ea32de472b1319182af73826af52ecdd3917be99))
* **demo:** replace simulated walkthroughs with verified recordings ([#665](https://github.com/cubrid-lab/pycubrid/issues/665)) ([a44e250](https://github.com/cubrid-lab/pycubrid/commit/a44e2502568a9492017a4655402eefd10bf07b0a)), closes [#320](https://github.com/cubrid-lab/pycubrid/issues/320)
* keep the release backlog review grounded in current issue scopes ([#680](https://github.com/cubrid-lab/pycubrid/issues/680)) ([4a9c598](https://github.com/cubrid-lab/pycubrid/commit/4a9c598555f2d3daa42e0055d64c9dbcf43d1e24)), closes [#566](https://github.com/cubrid-lab/pycubrid/issues/566)
* let the mitigated differential lane ship without claiming an upstream fix ([#676](https://github.com/cubrid-lab/pycubrid/issues/676)) ([14f7398](https://github.com/cubrid-lab/pycubrid/commit/14f739866040c1bb9efc47d7426b50661007f3bd)), closes [#614](https://github.com/cubrid-lab/pycubrid/issues/614)
* make current contributor procedures accessible in Korean ([ac89eb3](https://github.com/cubrid-lab/pycubrid/commit/ac89eb36fefd8c2b6a61b0b3cb11ed6dc9615440)), closes [#329](https://github.com/cubrid-lab/pycubrid/issues/329)
* make the quick start usable without missing table columns ([#677](https://github.com/cubrid-lab/pycubrid/issues/677)) ([518aa8b](https://github.com/cubrid-lab/pycubrid/commit/518aa8be4e2209b05e1a0c0faf68f9f3b7853df0)), closes [#327](https://github.com/cubrid-lab/pycubrid/issues/327) [#327](https://github.com/cubrid-lab/pycubrid/issues/327)
* narrow the plan-reuse wording and catch deleted llms artifacts in CI ([48aeefc](https://github.com/cubrid-lab/pycubrid/commit/48aeefc4ea049d7a764ca8cdd01e5cdb1cf7dc06))
* note that LTZ values follow the session time zone kept across commit ([2f47b69](https://github.com/cubrid-lab/pycubrid/commit/2f47b699ddae5274b4a3e2557cb1b263fca7b274))
* note that LTZ values follow the session time zone kept across commit ([d4b370a](https://github.com/cubrid-lab/pycubrid/commit/d4b370aed70563ab898a701b970d307d18d114a9))
* **performance:** make the Korean guide discoverable and navigable ([5b394b7](https://github.com/cubrid-lab/pycubrid/commit/5b394b70fd178fc529d2482e45faa91ac20f0384))
* pin cookbook smoke-test fallback to the exact release ([dd621c4](https://github.com/cubrid-lab/pycubrid/commit/dd621c427b883b7b987563049ce8d398f35ee2ed))
* pin cookbook smoke-test fallback to the exact release ([330682c](https://github.com/cubrid-lab/pycubrid/commit/330682c6d1c18900a0a81dd6a3133dc90bfe7d4a))
* **protocol:** retain raw errno after renewed-code evaluation ([#656](https://github.com/cubrid-lab/pycubrid/issues/656)) ([eed27ab](https://github.com/cubrid-lab/pycubrid/commit/eed27abf8f4d350b84baaa6d7562812d90fc0aff)), closes [#505](https://github.com/cubrid-lab/pycubrid/issues/505)
* provide Korean contributor procedures ([#663](https://github.com/cubrid-lab/pycubrid/issues/663)) ([ac89eb3](https://github.com/cubrid-lab/pycubrid/commit/ac89eb36fefd8c2b6a61b0b3cb11ed6dc9615440))
* **python:** announce Python 3.10 support retirement ([#658](https://github.com/cubrid-lab/pycubrid/issues/658)) ([ba58b80](https://github.com/cubrid-lab/pycubrid/commit/ba58b80f5cc7af26f664f06a4b5c5e78bade827b))
* **release:** add 1.9.0 upgrade notes and mark new compat names provisional ([#671](https://github.com/cubrid-lab/pycubrid/issues/671)) ([7fba209](https://github.com/cubrid-lab/pycubrid/commit/7fba209adb241e7873bbc2671299eb516d73a21e)), closes [#396](https://github.com/cubrid-lab/pycubrid/issues/396)
* **release:** document release-please review and recovery ([fc922fe](https://github.com/cubrid-lab/pycubrid/commit/fc922fe1ebee46696124c12d2b5dad6a68ca64ef))
* **release:** fix create-release recovery command; make SBOM re-upload idempotent ([9784b0a](https://github.com/cubrid-lab/pycubrid/commit/9784b0a9677172a5fb1346d6f3011729c1640800))
* replace fixed Docker startup sleeps ([8154952](https://github.com/cubrid-lab/pycubrid/commit/8154952a475a772aca7dae96505d52b02eee551f))
* scope the time zone persistence to a retained CAS session ([8814081](https://github.com/cubrid-lab/pycubrid/commit/881408120342f51868805fc61563ab9d9a4a27f1))
* **security:** align maintenance and TLS guidance ([#649](https://github.com/cubrid-lab/pycubrid/issues/649)) ([ac63fd3](https://github.com/cubrid-lab/pycubrid/commit/ac63fd38fc946b6ad3cfab91333784cafc76a45f))
* single-source llms.txt and remove unsupported prepared-statement claims ([6039dbe](https://github.com/cubrid-lab/pycubrid/commit/6039dbec69f88091578083011d8a93924317b8b6))
* single-source llms.txt and remove unsupported prepared-statement claims ([8957eaf](https://github.com/cubrid-lab/pycubrid/commit/8957eaf6e6c92e127f0768af72df219200cb544d)), closes [#414](https://github.com/cubrid-lab/pycubrid/issues/414)
* **types:** state that a bound datetime.time drops microseconds ([#670](https://github.com/cubrid-lab/pycubrid/issues/670)) ([8332517](https://github.com/cubrid-lab/pycubrid/commit/833251756ff92c1300d59fde59e2b9015776054d))
* warn that pre-commit hooks need an active .[dev] environment ([acb4caa](https://github.com/cubrid-lab/pycubrid/commit/acb4caac56c2cbd479471f5a06124040be4ac29d))
