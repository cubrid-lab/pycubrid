# Changelog

## [1.17.1](https://github.com/cubrid-lab/pycubrid/compare/v1.17.0...v1.17.1) (2026-11-14)


### Fixed

* **release:** remove correlated duplicate changelog entries ([#1182](https://github.com/cubrid-lab/pycubrid/issues/1182)) ([583391a](https://github.com/cubrid-lab/pycubrid/commit/583391a3f85d6b9b64a5efda10882cea2d57f69e))


### Documentation

* add a module docstring to pycubrid.connection ([#1216](https://github.com/cubrid-lab/pycubrid/issues/1216)) ([3ce04c4](https://github.com/cubrid-lab/pycubrid/commit/3ce04c4b174a4708a7e28d4950027c99b71f0210)), closes [#1211](https://github.com/cubrid-lab/pycubrid/issues/1211)
* add the first-connection guide to translated READMEs ([#1214](https://github.com/cubrid-lab/pycubrid/issues/1214)) ([22dc3de](https://github.com/cubrid-lab/pycubrid/commit/22dc3de804def9b1fb0b80c528feecd28f8e4048)), closes [#1089](https://github.com/cubrid-lab/pycubrid/issues/1089)
* clarify issue handoff and work states ([6c2cba8](https://github.com/cubrid-lab/pycubrid/commit/6c2cba8fe7c4742b96f38151a59a85077a156655))
* **compat:** show how to read the isolation level and lock timeout ([#1136](https://github.com/cubrid-lab/pycubrid/issues/1136)) ([3880a6e](https://github.com/cubrid-lab/pycubrid/commit/3880a6ee2ca99a65b412d1519232184899680dc5))
* **compat:** warn against rewrapping a LOB handle across connections ([#1141](https://github.com/cubrid-lab/pycubrid/issues/1141)) ([5c7f769](https://github.com/cubrid-lab/pycubrid/commit/5c7f7698d3179b2106d84d940e8d0cd165be2caf)), closes [#1130](https://github.com/cubrid-lab/pycubrid/issues/1130) [#1122](https://github.com/cubrid-lab/pycubrid/issues/1122)
* correct the database-not-found symptom and createdb command ([#1218](https://github.com/cubrid-lab/pycubrid/issues/1218)) ([0efa9fd](https://github.com/cubrid-lab/pycubrid/commit/0efa9fd4b03a8d63601ba2d76542cb10c64b9dac)), closes [#1082](https://github.com/cubrid-lab/pycubrid/issues/1082)
* cover the documentation site in the third-party license inventory ([#1220](https://github.com/cubrid-lab/pycubrid/issues/1220)) ([1f85a7d](https://github.com/cubrid-lab/pycubrid/commit/1f85a7d194c7de402738bd5ba9d9c5ab8690a331))
* credit contributors in the unreleased release notes ([#1237](https://github.com/cubrid-lab/pycubrid/issues/1237)) ([ab8c019](https://github.com/cubrid-lab/pycubrid/commit/ab8c0196977b9400898f11f806cc5ffffb1fb349))
* credit verified human authors in the v0.1.0 to v1.6.2 release notes ([#1232](https://github.com/cubrid-lab/pycubrid/issues/1232)) ([524b1a1](https://github.com/cubrid-lab/pycubrid/commit/524b1a18b32d12d1be5d2b0897cc3fc6a064b3c7))
* credit verified human authors in the v1.13.0 to v1.13.5 release notes ([#1235](https://github.com/cubrid-lab/pycubrid/issues/1235)) ([ab54723](https://github.com/cubrid-lab/pycubrid/commit/ab547233333c4b818cf7dbd0402ca9cf5f1e9e00))
* credit verified human authors in the v1.14.0 to v1.17.0 release notes ([#1236](https://github.com/cubrid-lab/pycubrid/issues/1236)) ([4f14bc1](https://github.com/cubrid-lab/pycubrid/commit/4f14bc17232288a3388855507757068fd3b7161e))
* credit verified human authors in the v1.7 to v1.12 release notes ([#1233](https://github.com/cubrid-lab/pycubrid/issues/1233)) ([82dfb9e](https://github.com/cubrid-lab/pycubrid/commit/82dfb9e73148f5cfefa6d4e0c36024be08c35c5e))
* credit verified human authors in the v1.9.0 and v1.11.0 release notes ([#1234](https://github.com/cubrid-lab/pycubrid/issues/1234)) ([e7c7e86](https://github.com/cubrid-lab/pycubrid/commit/e7c7e86440c6f028f0237f63a88da8c833bac714))
* define a review and removal rule for temporary tooling ([#1224](https://github.com/cubrid-lab/pycubrid/issues/1224)) ([08c6019](https://github.com/cubrid-lab/pycubrid/commit/08c601919d659079baef75c93eda56894ba8e924)), closes [#1169](https://github.com/cubrid-lab/pycubrid/issues/1169)
* define issue handoff and development states ([#1230](https://github.com/cubrid-lab/pycubrid/issues/1230)) ([6c2cba8](https://github.com/cubrid-lab/pycubrid/commit/6c2cba8fe7c4742b96f38151a59a85077a156655))
* document the runnable example scripts and their cleanup ([#1208](https://github.com/cubrid-lab/pycubrid/issues/1208)) ([486ffe4](https://github.com/cubrid-lab/pycubrid/commit/486ffe4372c4dbf137503c03a4aec619434bb01f)), closes [#1087](https://github.com/cubrid-lab/pycubrid/issues/1087)
* give quickstart-lab commands in Docker troubleshooting ([#1219](https://github.com/cubrid-lab/pycubrid/issues/1219)) ([b83ef33](https://github.com/cubrid-lab/pycubrid/commit/b83ef33f1c524c5e383b1bb4cb6dd1bee1850827)), closes [#1084](https://github.com/cubrid-lab/pycubrid/issues/1084)
* keep PyCUBRID contribution rules inside the repository ([#1203](https://github.com/cubrid-lab/pycubrid/issues/1203)) ([9fc5c7b](https://github.com/cubrid-lab/pycubrid/commit/9fc5c7be88fb839bf98f0aa2dd1e60eb6f3926a4))
* make the help and reporting path consistent across the docs ([#1221](https://github.com/cubrid-lab/pycubrid/issues/1221)) ([9bbee7f](https://github.com/cubrid-lab/pycubrid/commit/9bbee7f4c63f8108d90b11c70dd77bbbea5dc648)), closes [#1093](https://github.com/cubrid-lab/pycubrid/issues/1093)
* merge duplicate generated entries in the v1.10.0 and v1.12.0 release notes ([#1242](https://github.com/cubrid-lab/pycubrid/issues/1242)) ([2c3ec84](https://github.com/cubrid-lab/pycubrid/commit/2c3ec842de36b21e78f150d244dd69777df53b9a)), closes [#1165](https://github.com/cubrid-lab/pycubrid/issues/1165)
* merge duplicate generated entries in the v1.13.0 to v1.13.5 release notes ([#1243](https://github.com/cubrid-lab/pycubrid/issues/1243)) ([27355e1](https://github.com/cubrid-lab/pycubrid/commit/27355e1ab3fd5829f45ecc6da82914c145bec51b)), closes [#1164](https://github.com/cubrid-lab/pycubrid/issues/1164)
* merge duplicate generated entries in the v1.14.0 to v1.17.0 release notes ([#1244](https://github.com/cubrid-lab/pycubrid/issues/1244)) ([e0a7e3a](https://github.com/cubrid-lab/pycubrid/commit/e0a7e3a6ffc1163441d2c2ead00bf8b00f5c0f2e)), closes [#1170](https://github.com/cubrid-lab/pycubrid/issues/1170)
* merge duplicate generated entries in the v1.9.0 and v1.11.0 release notes ([#1241](https://github.com/cubrid-lab/pycubrid/issues/1241)) ([533f0fa](https://github.com/cubrid-lab/pycubrid/commit/533f0fae378ac35e88034e4602338ca55f07767d)), closes [#1171](https://github.com/cubrid-lab/pycubrid/issues/1171)
* **performance:** show how to import large inputs with bounded memory ([#1152](https://github.com/cubrid-lab/pycubrid/issues/1152)) ([d2939bd](https://github.com/cubrid-lab/pycubrid/commit/d2939bd1f162f364e9ca0e4de15c51ac709b7e09))
* **quickstart:** add a Windows venv activation command ([#1159](https://github.com/cubrid-lab/pycubrid/issues/1159)) ([5da6732](https://github.com/cubrid-lab/pycubrid/commit/5da67329f5261e14fabb0496590d9b97d38b055d)), closes [#1092](https://github.com/cubrid-lab/pycubrid/issues/1092)
* **release:** clarify five published v1.13.x release notes ([#1196](https://github.com/cubrid-lab/pycubrid/issues/1196)) ([cc7b67c](https://github.com/cubrid-lab/pycubrid/commit/cc7b67c109a26af73a826bfb828accd900597664))
* **release:** clarify the initial v0.1.0 release notes ([#1181](https://github.com/cubrid-lab/pycubrid/issues/1181)) ([90a7a39](https://github.com/cubrid-lab/pycubrid/commit/90a7a39d6525482565c607becb59cf479ad4c3ce)), closes [#1166](https://github.com/cubrid-lab/pycubrid/issues/1166)
* **release:** clarify v0.2.0 protocol foundations ([#1183](https://github.com/cubrid-lab/pycubrid/issues/1183)) ([6c2aac7](https://github.com/cubrid-lab/pycubrid/commit/6c2aac72105bcccd916af0349c582ea240a7a88b)), closes [#1166](https://github.com/cubrid-lab/pycubrid/issues/1166)
* **release:** clarify v0.3.0 connection and cursor support ([#1184](https://github.com/cubrid-lab/pycubrid/issues/1184)) ([3fd62b2](https://github.com/cubrid-lab/pycubrid/commit/3fd62b2aa5954cc8e9a7feccd7da374b256e2bb4)), closes [#1166](https://github.com/cubrid-lab/pycubrid/issues/1166)
* **release:** clarify v0.6.0 CAS reconnection and DML batching ([#1187](https://github.com/cubrid-lab/pycubrid/issues/1187)) ([b65e70b](https://github.com/cubrid-lab/pycubrid/commit/b65e70b85299813cfa86f995f1e97c2605c6b493)), closes [#1166](https://github.com/cubrid-lab/pycubrid/issues/1166)
* **release:** clarify v0.7.0 support boundaries and pooling ([#1188](https://github.com/cubrid-lab/pycubrid/issues/1188)) ([6e60d33](https://github.com/cubrid-lab/pycubrid/commit/6e60d3373104be5727d5c16b6e2cf4a2214a29dd)), closes [#1166](https://github.com/cubrid-lab/pycubrid/issues/1166)
* **release:** clarify v1.14–v1.17 and Compat policy reversal ([#1197](https://github.com/cubrid-lab/pycubrid/issues/1197)) ([54ff913](https://github.com/cubrid-lab/pycubrid/commit/54ff91318cb2d4e21b80b4b0415363e7f052b244))
* **release:** clarify v1.2.0 decoding and parameter compatibility ([#1191](https://github.com/cubrid-lab/pycubrid/issues/1191)) ([bcb63c1](https://github.com/cubrid-lab/pycubrid/commit/bcb63c17fce080e573e9c8c9e071f91277171ff5)), closes [#1166](https://github.com/cubrid-lab/pycubrid/issues/1166)
* **release:** clarify v1.7.0 compatibility and LOB safeguards ([#1193](https://github.com/cubrid-lab/pycubrid/issues/1193)) ([2f1396d](https://github.com/cubrid-lab/pycubrid/commit/2f1396dfda40510aae4f9c02f2123a034cdf4d9a)), closes [#1165](https://github.com/cubrid-lab/pycubrid/issues/1165)
* **release:** clarify v1.7.1, v1.8.0, v1.10.0 and v1.12.0 ([#1194](https://github.com/cubrid-lab/pycubrid/issues/1194)) ([235935d](https://github.com/cubrid-lab/pycubrid/commit/235935df210184a8b373f7635a0df0cc02098838)), closes [#1165](https://github.com/cubrid-lab/pycubrid/issues/1165)
* **release:** correct v1.0.0 protocol version and stability notes ([#1189](https://github.com/cubrid-lab/pycubrid/issues/1189)) ([6cbe004](https://github.com/cubrid-lab/pycubrid/commit/6cbe0049b29f71e411a01e98ee17c7f95fe52f05)), closes [#1166](https://github.com/cubrid-lab/pycubrid/issues/1166)
* **release:** credit verified authors of the first four releases ([#1198](https://github.com/cubrid-lab/pycubrid/issues/1198)) ([643d7dd](https://github.com/cubrid-lab/pycubrid/commit/643d7dd1a9fb3071048e855eca435ab24b43c678)), closes [#1173](https://github.com/cubrid-lab/pycubrid/issues/1173)
* **release:** document v0.5.0 compatibility fixes and SQLAlchemy setup ([#1186](https://github.com/cubrid-lab/pycubrid/issues/1186)) ([e5e3c04](https://github.com/cubrid-lab/pycubrid/commit/e5e3c04e4af8b2afc236baadcf1631ec44dd49b6)), closes [#1166](https://github.com/cubrid-lab/pycubrid/issues/1166)
* **release:** document v1.1.0 asyncio and timing improvements ([#1190](https://github.com/cubrid-lab/pycubrid/issues/1190)) ([acf393b](https://github.com/cubrid-lab/pycubrid/commit/acf393b21dcff5e2802c8e1abe2a30415c4d13eb)), closes [#1166](https://github.com/cubrid-lab/pycubrid/issues/1166)
* **release:** establish a reader-focused release-note format ([#1178](https://github.com/cubrid-lab/pycubrid/issues/1178)) ([0aada1e](https://github.com/cubrid-lab/pycubrid/commit/0aada1eab2663a20b11a92ce79497a2b4c358cf2)), closes [#1177](https://github.com/cubrid-lab/pycubrid/issues/1177)
* **release:** explain v0.4.0 LOB, schema and batch features ([#1185](https://github.com/cubrid-lab/pycubrid/issues/1185)) ([a67cd9c](https://github.com/cubrid-lab/pycubrid/commit/a67cd9cd08dd39419dafb45776277db183bce2a0)), closes [#1166](https://github.com/cubrid-lab/pycubrid/issues/1166)
* **release:** freeze release candidates only when their review starts ([#1213](https://github.com/cubrid-lab/pycubrid/issues/1213)) ([7b2d0cf](https://github.com/cubrid-lab/pycubrid/commit/7b2d0cfb7ec64a0b3833ebc37dc4dc7738f6eacb))
* **release:** standardize v1.3.0 through v1.6.2 release notes ([#1192](https://github.com/cubrid-lab/pycubrid/issues/1192)) ([2bd3057](https://github.com/cubrid-lab/pycubrid/commit/2bd305729f1a80248eb6644904a7e06ddde7a979)), closes [#1172](https://github.com/cubrid-lab/pycubrid/issues/1172)
* **release:** standardize v1.9.0 and v1.11.0 notes ([#1195](https://github.com/cubrid-lab/pycubrid/issues/1195)) ([755638a](https://github.com/cubrid-lab/pycubrid/commit/755638af572e6cace5905212165d0283bdf96544)), closes [#1171](https://github.com/cubrid-lab/pycubrid/issues/1171)
* remove contest deadline from Korean README sync guidance ([#1162](https://github.com/cubrid-lab/pycubrid/issues/1162)) ([2e42b0a](https://github.com/cubrid-lab/pycubrid/commit/2e42b0a927f6956a21e97d653dbc3d96050fc1bf))
* remove deprecated organization roadmap and board links ([#1199](https://github.com/cubrid-lab/pycubrid/issues/1199)) ([12caa41](https://github.com/cubrid-lab/pycubrid/commit/12caa4161e3c74e15261cf425cec58b392090908))
* remove obsolete ecosystem roadmap references from translations ([#1200](https://github.com/cubrid-lab/pycubrid/issues/1200)) ([cdd4f13](https://github.com/cubrid-lab/pycubrid/commit/cdd4f136f2eb54f3d99ec985725c1bf293bb96d2))
* restore seven v1.9.0 co-author credits broken by a credit rewrite ([#1240](https://github.com/cubrid-lab/pycubrid/issues/1240)) ([2839466](https://github.com/cubrid-lab/pycubrid/commit/2839466e99dd0c2e541ce01a8ed76e4d21aa00c0)), closes [#1173](https://github.com/cubrid-lab/pycubrid/issues/1173) [#1171](https://github.com/cubrid-lab/pycubrid/issues/1171)
* route usage questions to Discussions from the issue chooser ([#1227](https://github.com/cubrid-lab/pycubrid/issues/1227)) ([401d9ce](https://github.com/cubrid-lab/pycubrid/commit/401d9ce2538fe7e1efda9d3a50b5a1119cd9a6f6))
* say that v0.1.0 to v0.4.0 were never published to PyPI ([#1239](https://github.com/cubrid-lab/pycubrid/issues/1239)) ([83def3b](https://github.com/cubrid-lab/pycubrid/commit/83def3b906ecedbe50a90d19723ccad77497c5f3)), closes [#1175](https://github.com/cubrid-lab/pycubrid/issues/1175) [#1166](https://github.com/cubrid-lab/pycubrid/issues/1166)
* show the wrapped connection errors in Troubleshooting ([#1212](https://github.com/cubrid-lab/pycubrid/issues/1212)) ([5584e07](https://github.com/cubrid-lab/pycubrid/commit/5584e0753ec6ee199d15ce1a92bcc9e5bc407fa9))
* **site:** stop linking published pages to unpublished repository files ([#1147](https://github.com/cubrid-lab/pycubrid/issues/1147)) ([fa5ae28](https://github.com/cubrid-lab/pycubrid/commit/fa5ae284f1e1c9d049a36fa9465c75d539f62f0e))
* track issue work without project boards ([1c32dec](https://github.com/cubrid-lab/pycubrid/commit/1c32deca2089606fc8d5aab3b5b046894ed093a0))
* **troubleshooting:** wait for a real Docker login and bind locally ([deba70f](https://github.com/cubrid-lab/pycubrid/commit/deba70fac7041b336e83f2a667727e0000f815fb)), closes [#1083](https://github.com/cubrid-lab/pycubrid/issues/1083)
* use a read-only query in the README context-manager example ([#1226](https://github.com/cubrid-lab/pycubrid/issues/1226)) ([ad1e244](https://github.com/cubrid-lab/pycubrid/commit/ad1e244f73b2d83e33fcc4222dd9a1ec5ef79a66))
* use absolute URLs in the README for the PyPI page ([#1215](https://github.com/cubrid-lab/pycubrid/issues/1215)) ([7d7b29c](https://github.com/cubrid-lab/pycubrid/commit/7d7b29c94383c6b7be6e63f88746444f4f5225cf))
* use issues and pull requests without project boards ([#1231](https://github.com/cubrid-lab/pycubrid/issues/1231)) ([1c32dec](https://github.com/cubrid-lab/pycubrid/commit/1c32deca2089606fc8d5aab3b5b046894ed093a0))

## [1.17.0](https://github.com/cubrid-lab/pycubrid/compare/v1.16.0...v1.17.0) (2026-11-05)


### Added

* **compat:** warn that the compat namespaces are deprecated ([#1128](https://github.com/cubrid-lab/pycubrid/issues/1128)) ([24c4518](https://github.com/cubrid-lab/pycubrid/commit/24c4518fd848876d2da3b8998c4c9e7116584f01))


### Documentation

* **release:** adopt deprecation-based MINOR retirement for Compat ([#1125](https://github.com/cubrid-lab/pycubrid/issues/1125)) ([2c52d45](https://github.com/cubrid-lab/pycubrid/commit/2c52d459999ac5a2332f2de51921bd01e4775ab1))

## [1.16.0](https://github.com/cubrid-lab/pycubrid/compare/v1.15.0...v1.16.0) (2026-11-05)


### Added

* **compat:** add native first-row schema_info() for verified schema types ([#1057](https://github.com/cubrid-lab/pycubrid/issues/1057)) ([4e110ec](https://github.com/cubrid-lab/pycubrid/commit/4e110ec29c1c5679383bba3e8c7bf65f835cb3e1))


### Documentation

* align issue titles with organization writing standard ([#1067](https://github.com/cubrid-lab/pycubrid/issues/1067)) ([629a3ac](https://github.com/cubrid-lab/pycubrid/commit/629a3ac3b9e8492207db9ac092d78d5abcefdd7c))
* **changelog:** move the schema_info note to the unreleased section ([#1117](https://github.com/cubrid-lab/pycubrid/issues/1117)) ([28b7cdc](https://github.com/cubrid-lab/pycubrid/commit/28b7cdcc16917792393aaccaa0974b905ab178c6))
* **faq:** clarify supported Python versions ([#1099](https://github.com/cubrid-lab/pycubrid/issues/1099)) ([5b6a814](https://github.com/cubrid-lab/pycubrid/commit/5b6a814ac6fdcaf4c41afdc4cfe8d82a366a51e0))
* **readme:** repin the brand icon to the transparent role marks ([#1097](https://github.com/cubrid-lab/pycubrid/issues/1097)) ([d9e2e1b](https://github.com/cubrid-lab/pycubrid/commit/d9e2e1b8cb5c02a4af205e2d7b61469f738b7c25))
* require human-readable agent communication and self-explanatory comments ([#1062](https://github.com/cubrid-lab/pycubrid/issues/1062)) ([83fbbca](https://github.com/cubrid-lab/pycubrid/commit/83fbbca04828b15ea24fe4d3e962d187deb77940))
* **troubleshooting:** list the connection-closed errors the driver raises ([#1095](https://github.com/cubrid-lab/pycubrid/issues/1095)) ([232c15e](https://github.com/cubrid-lab/pycubrid/commit/232c15ebd3129dbe345da8d3bb426f7eb6dc4c04))
* **troubleshooting:** make verbose logging example runnable ([#1104](https://github.com/cubrid-lab/pycubrid/issues/1104)) ([818dd04](https://github.com/cubrid-lab/pycubrid/commit/818dd04dfdd43c86223e5cdb6a04ba0eff866a6f))
* update bug report environment guidance ([#1102](https://github.com/cubrid-lab/pycubrid/issues/1102)) ([e588438](https://github.com/cubrid-lab/pycubrid/commit/e588438a7a8e004f4fdda430ca1f746e7b59344b))
* use the Python yellow from the brand tokens in the site theme ([#1103](https://github.com/cubrid-lab/pycubrid/issues/1103)) ([c18d35a](https://github.com/cubrid-lab/pycubrid/commit/c18d35a3057e879989868163481068a351bdcbaa))

## [1.15.0](https://github.com/cubrid-lab/pycubrid/compare/v1.14.0...v1.15.0) (2026-10-27)


### Added

* **make:** write an aggregate verification summary under test-results/verify ([#1054](https://github.com/cubrid-lab/pycubrid/issues/1054)) ([a7e0be2](https://github.com/cubrid-lab/pycubrid/commit/a7e0be2a80f79331cbdf4fec8770c5c3ab645ea4))


### Documentation

* **profile:** warn that the profilers write to the target database ([#1063](https://github.com/cubrid-lab/pycubrid/issues/1063)) ([1789ebc](https://github.com/cubrid-lab/pycubrid/commit/1789ebc32fab0a2648cbad21f567e3da32ed1e53)), closes [#918](https://github.com/cubrid-lab/pycubrid/issues/918)
* **readme:** repin the brand icon to the 2026-10-25 brand revision ([#1059](https://github.com/cubrid-lab/pycubrid/issues/1059)) ([a3f7d6b](https://github.com/cubrid-lab/pycubrid/commit/a3f7d6bb30652a592d43a8f492cc88bdcd5d459c))
* **readme:** repin the brand icon to the cube redesign ([#1061](https://github.com/cubrid-lab/pycubrid/issues/1061)) ([675739e](https://github.com/cubrid-lab/pycubrid/commit/675739ee5fc541fac02f6b289aa533eeb42daba8))
* **readme:** repin the brand icon to the emblem-aligned keyline ([#1070](https://github.com/cubrid-lab/pycubrid/issues/1070)) ([223d83a](https://github.com/cubrid-lab/pycubrid/commit/223d83a85e187d766f3b256790b3f15671ecd5d0))
* **readme:** repin the brand icon to the shared white marks ([#1072](https://github.com/cubrid-lab/pycubrid/issues/1072)) ([5dcd360](https://github.com/cubrid-lab/pycubrid/commit/5dcd3608bac3d1e3f71ba85e636a5de6eb974c79))

## [1.14.0](https://github.com/cubrid-lab/pycubrid/compare/v1.13.5...v1.14.0) (2026-10-25)


### Added

* **make:** run the live integration lane in make verify ([#1050](https://github.com/cubrid-lab/pycubrid/issues/1050)) ([4f63593](https://github.com/cubrid-lab/pycubrid/commit/4f63593d3d7e456edececf75ba566d9ed65f758f))

## [1.13.5](https://github.com/cubrid-lab/pycubrid/compare/v1.13.4...v1.13.5) (2026-10-25)


### Documentation

* **agents:** clarify commit attribution and validation evidence ([#1048](https://github.com/cubrid-lab/pycubrid/issues/1048)) ([48d8d0e](https://github.com/cubrid-lab/pycubrid/commit/48d8d0eeca8341422d29622a8bb2148e0327a2ed))

## [1.13.4](https://github.com/cubrid-lab/pycubrid/compare/v1.13.3...v1.13.4) (2026-10-24)


### Fixed

* **reporting:** redact credentials in saved integration command metadata ([#1046](https://github.com/cubrid-lab/pycubrid/issues/1046)) ([80e9c07](https://github.com/cubrid-lab/pycubrid/commit/80e9c07caf8ccaabcbdc8caa0f0c4b03d197af0f))


### Documentation

* **examples:** add a bounded opt-in prepared executemany example ([#1039](https://github.com/cubrid-lab/pycubrid/issues/1039)) ([1f1ddd9](https://github.com/cubrid-lab/pycubrid/commit/1f1ddd994403e19eb8bfe0176cd9a4531189a62d))
* **examples:** document prepared executemany batches ([1f1ddd9](https://github.com/cubrid-lab/pycubrid/commit/1f1ddd994403e19eb8bfe0176cd9a4531189a62d)), closes [#891](https://github.com/cubrid-lab/pycubrid/issues/891)

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
