# Changelog

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
