# Changelog

## [18.3.0](https://github.com/web3-storage/w3up/compare/access-v18.2.0...access-v18.3.0) (2024-03-26)


### Features

* upgrade ucanto/transport to 9.1.0 in all packages to get more verbose errors from HTTP transport on non-ok response ([#1312](https://github.com/web3-storage/w3up/issues/1312)) ([d6978d7](https://github.com/web3-storage/w3up/commit/d6978d7ab299be76987c6533d18e6857f6998fe6))

## [18.2.0](https://github.com/web3-storage/w3up/compare/access-v18.1.1...access-v18.2.0) (2024-01-29)


### Features

* two more interface tweaks ([#1287](https://github.com/web3-storage/w3up/issues/1287)) ([bc3c364](https://github.com/web3-storage/w3up/commit/bc3c36452454398ea8e0f574aed44b318561ad94))

## [18.1.1](https://github.com/web3-storage/w3up/compare/access-v18.1.0...access-v18.1.1) (2024-01-17)


### Fixes

* upgrade type-fest in access ([#1263](https://github.com/web3-storage/w3up/issues/1263)) ([47a4589](https://github.com/web3-storage/w3up/commit/47a458964aaf1ebe07db4e29db60e558b9871fb6))

## [18.1.0](https://github.com/web3-storage/w3up/compare/access-v18.0.7...access-v18.1.0) (2023-12-14)


### Features

* expose OwnedSpace and SharedSpace from access-client ([#1244](https://github.com/web3-storage/w3up/issues/1244)) ([8ec1b44](https://github.com/web3-storage/w3up/commit/8ec1b446590399aa236904c1b6937b7be5d83054))


### Fixes

* point `main` at files included in the package ([#1241](https://github.com/web3-storage/w3up/issues/1241)) ([c0b306d](https://github.com/web3-storage/w3up/commit/c0b306df75b21d0979e407f04f0a23f67d5248af))

## [18.0.7](https://github.com/web3-storage/w3up/compare/access-v18.0.6...access-v18.0.7) (2023-12-13)


### Fixes

* support storing ArrayBuffers in conf ([#1236](https://github.com/web3-storage/w3up/issues/1236)) ([9b1aafb](https://github.com/web3-storage/w3up/commit/9b1aafbcf241d268e4f365ed99005458dda1a05a))

## [18.0.6](https://github.com/web3-storage/w3up/compare/access-v18.0.5...access-v18.0.6) (2023-12-07)


### Fixes

* use an ArrayBuffer for delegation bits in AgentData ([#1219](https://github.com/web3-storage/w3up/issues/1219)) ([bddf874](https://github.com/web3-storage/w3up/commit/bddf87445755fa977768d636481eaee678a06e79))

## [18.0.5](https://github.com/web3-storage/w3up/compare/access-v18.0.4...access-v18.0.5) (2023-11-29)


### Fixes

* fix IndexedDB reset function ([#1199](https://github.com/web3-storage/w3up/issues/1199)) ([48cf555](https://github.com/web3-storage/w3up/commit/48cf55596162f68833f4cea49364a9dd5a845362))
* sync space names from proofs ([#1193](https://github.com/web3-storage/w3up/issues/1193)) ([f552036](https://github.com/web3-storage/w3up/commit/f552036913cf7172e93e83e27fd4af6f7b6a4673))

## [18.0.4](https://github.com/web3-storage/w3up/compare/access-v18.0.3...access-v18.0.4) (2023-11-29)


### Fixes

* floating promises and add no-floating-promises to eslint-config-w3up ([#1198](https://github.com/web3-storage/w3up/issues/1198)) ([1b8c5aa](https://github.com/web3-storage/w3up/commit/1b8c5aa86ec3d177bf77df4e2916699c1f522598))

## [18.0.3](https://github.com/web3-storage/w3up/compare/access-v18.0.2...access-v18.0.3) (2023-11-22)


### Fixes

* don't error when we can't figure out a name for a space ([#1177](https://github.com/web3-storage/w3up/issues/1177)) ([a31f667](https://github.com/web3-storage/w3up/commit/a31f6671b52d37b8493ca1690ca737ddd311558b))
* package metadata ([#1161](https://github.com/web3-storage/w3up/issues/1161)) ([b8a1cc2](https://github.com/web3-storage/w3up/commit/b8a1cc2e125a91be582998bda295e1ae1caab087))

## [18.0.2](https://github.com/web3-storage/w3up/compare/access-v18.0.1...access-v18.0.2) (2023-11-17)


### Bug Fixes

* access-client package.json uses https instead of git for one-webcrypto dep to help with yarn compat ([#1157](https://github.com/web3-storage/w3up/issues/1157)) ([e1d0798](https://github.com/web3-storage/w3up/commit/e1d079811cceb0a68da371ba422ba6147e0fae4a))

## [18.0.1](https://github.com/web3-storage/w3up/compare/access-v18.0.0...access-v18.0.1) (2023-11-16)


### Bug Fixes

* issue where typedoc docs would only show full docs for w3up-client ([#1141](https://github.com/web3-storage/w3up/issues/1141)) ([0b8d3f3](https://github.com/web3-storage/w3up/commit/0b8d3f3b52918b1b4d3b76ea6fea3fb0c837cd73))
* upgrade @ucanto/validator with bugfix ([#1151](https://github.com/web3-storage/w3up/issues/1151)) ([d4e961b](https://github.com/web3-storage/w3up/commit/d4e961bab09e88245e7d9323146849271e78eb57))

## [18.0.0](https://github.com/web3-storage/w3up/compare/access-v17.1.0...access-v18.0.0) (2023-11-15)


### ⚠ BREAKING CHANGES

* coupon ([#1136](https://github.com/web3-storage/w3up/issues/1136))

### Features

* coupon ([#1136](https://github.com/web3-storage/w3up/issues/1136)) ([1b94f2d](https://github.com/web3-storage/w3up/commit/1b94f2d3f6538d717d38b21dcb76657fd1f3e268))


### Bug Fixes

* upgrade ucanto core ([#1127](https://github.com/web3-storage/w3up/issues/1127)) ([5ce4d22](https://github.com/web3-storage/w3up/commit/5ce4d2292d7e980da4a2ea0f1583f608a81157d2))

## [17.1.0](https://github.com/web3-storage/w3up/compare/access-v17.0.0...access-v17.1.0) (2023-11-14)


### Features

* w3up client login ([#1120](https://github.com/web3-storage/w3up/issues/1120)) ([8279bf6](https://github.com/web3-storage/w3up/commit/8279bf6371182709b46e83e5ac86d89ed1f292e8))

## [17.0.0](https://github.com/web3-storage/w3up/compare/access-v16.4.0...access-v17.0.0) (2023-11-09)


### ⚠ BREAKING CHANGES

* tweak readmes to get release-please to bump major version ([#1102](https://github.com/web3-storage/w3up/issues/1102))

### Features

* add `subscription/list` capability ([#1088](https://github.com/web3-storage/w3up/issues/1088)) ([471d7e5](https://github.com/web3-storage/w3up/commit/471d7e5db24e12a06c1c52ae76bf95ff9471bac8))
* add usage/report capability ([#1079](https://github.com/web3-storage/w3up/issues/1079)) ([6418b4b](https://github.com/web3-storage/w3up/commit/6418b4b22329a118fb258928bd9a6a45ced5ce45))
* tweak readmes to get release-please to bump major version ([#1102](https://github.com/web3-storage/w3up/issues/1102)) ([a411255](https://github.com/web3-storage/w3up/commit/a4112551f5dbac00f4b5a0da8c81ea35783f3ef9))


### Bug Fixes

* fix export paths for JS files ([#1089](https://github.com/web3-storage/w3up/issues/1089)) ([1a5d1aa](https://github.com/web3-storage/w3up/commit/1a5d1aa1b1bfdb188cb69712a74404b89d8200af))

## [16.5.1](https://github.com/web3-storage/w3up/compare/access-v16.5.0...access-v16.5.1) (2023-11-08)


### Bug Fixes

* fix export paths for JS files ([#1089](https://github.com/web3-storage/w3up/issues/1089)) ([1a5d1aa](https://github.com/web3-storage/w3up/commit/1a5d1aa1b1bfdb188cb69712a74404b89d8200af))

## [16.5.0](https://github.com/web3-storage/w3up/compare/access-v16.4.0...access-v16.5.0) (2023-11-07)


### Features

* add usage/report capability ([#1079](https://github.com/web3-storage/w3up/issues/1079)) ([6418b4b](https://github.com/web3-storage/w3up/commit/6418b4b22329a118fb258928bd9a6a45ced5ce45))

## [16.4.0](https://github.com/web3-storage/w3up/compare/access-v16.3.0...access-v16.4.0) (2023-11-01)


### Features

* access agent proofs method would fail to return some session proofs ([#1047](https://github.com/web3-storage/w3up/issues/1047)) ([d23a1c9](https://github.com/web3-storage/w3up/commit/d23a1c972f91b855ee91f862da15bab0e68cca0a))


### Bug Fixes

* use the issuer as the resource in revocation ([#992](https://github.com/web3-storage/w3up/issues/992)) ([7346d1f](https://github.com/web3-storage/w3up/commit/7346d1f70c5931123babc31c4d9819559ef284a5))

## [16.3.0](https://github.com/web3-storage/w3up/compare/access-v16.2.1...access-v16.3.0) (2023-10-27)


### Features

* implement `plan/get` capability ([#1005](https://github.com/web3-storage/w3up/issues/1005)) ([f0456d2](https://github.com/web3-storage/w3up/commit/f0456d2e2aab462666810e22abd7dfb7e1ce21be))


### Bug Fixes

* make `plan/get` return value is typed properly ([#1029](https://github.com/web3-storage/w3up/issues/1029)) ([075e341](https://github.com/web3-storage/w3up/commit/075e3414f528dfdf872ba29c8daf7ea5ba1cf8c9))

## [16.2.1](https://github.com/web3-storage/w3up/compare/access-v16.2.0...access-v16.2.1) (2023-10-25)


### Bug Fixes

* fix arethetypesworking errors in all packages ([#1004](https://github.com/web3-storage/w3up/issues/1004)) ([2e2936a](https://github.com/web3-storage/w3up/commit/2e2936a3831389dd13be5be5146a04e2b15553c5))
* package.json files excludes 'src' and includes .js and .js.map in dist for packages that now export their module from dist  ([#1012](https://github.com/web3-storage/w3up/issues/1012)) ([d2537de](https://github.com/web3-storage/w3up/commit/d2537deed533a39f39e312a1dfcfbd048e1d83e5))

## [16.2.0](https://github.com/web3-storage/w3up/compare/access-v16.1.0...access-v16.2.0) (2023-10-20)


### Features

* add more capabilities to the set we request on authorize ([#990](https://github.com/web3-storage/w3up/issues/990)) ([e61b3ce](https://github.com/web3-storage/w3up/commit/e61b3cef426a15c377f370406a4134bdc8567898))

## [16.1.0](https://github.com/web3-storage/w3up/compare/access-v16.0.0...access-v16.1.0) (2023-10-18)


### Features

* add revocation to access-client and w3up-client ([#975](https://github.com/web3-storage/w3up/issues/975)) ([6c877aa](https://github.com/web3-storage/w3up/commit/6c877aac78eddb924e999dc3270cba010e48e30a))

## [16.0.0](https://github.com/web3-storage/w3up/compare/access-v15.3.0...access-v16.0.0) (2023-10-10)


### ⚠ BREAKING CHANGES

* remove websocket support ([#966](https://github.com/web3-storage/w3up/issues/966))

### Bug Fixes

* remove websocket support ([#966](https://github.com/web3-storage/w3up/issues/966)) ([77bf7ea](https://github.com/web3-storage/w3up/commit/77bf7ea8c67c5bb1bbce9b298fd72919dad7bd43))
* upgrade to latest ts ([#962](https://github.com/web3-storage/w3up/issues/962)) ([711e3f7](https://github.com/web3-storage/w3up/commit/711e3f73f6905fde0d929952fff70be845a55fa1))

## [15.3.0](https://github.com/web3-storage/w3up/compare/access-v15.2.1...access-v15.3.0) (2023-10-06)


### Features

* upgrade to ucanto@9 ([#951](https://github.com/web3-storage/w3up/issues/951)) ([d72faf1](https://github.com/web3-storage/w3up/commit/d72faf1bb07dd11462ae6dff8ee0469f8ae7e9e7))

## [15.2.1](https://github.com/web3-storage/w3up/compare/access-v15.2.0...access-v15.2.1) (2023-09-13)


### Bug Fixes

* add providers to space/info result type ([#911](https://github.com/web3-storage/w3up/issues/911)) ([877f1a8](https://github.com/web3-storage/w3up/commit/877f1a8cf03884dcd40f979c0974b9123be8d915))

## [15.2.0](https://github.com/web3-storage/w3up/compare/access-v15.1.1...access-v15.2.0) (2023-08-29)


### Features

* make agent Service generic ([#875](https://github.com/web3-storage/w3up/issues/875)) ([cdfe36d](https://github.com/web3-storage/w3up/commit/cdfe36dc7298e92066d0454144f598b0e0535b19))

## [15.1.1](https://github.com/web3-storage/w3up/compare/access-v15.1.0...access-v15.1.1) (2023-08-25)


### Bug Fixes

* update docs to bump version ([#870](https://github.com/web3-storage/w3up/issues/870)) ([d2eec7c](https://github.com/web3-storage/w3up/commit/d2eec7cff1125898c0388957aa7a91fbba2e54f2))

## [15.1.0](https://github.com/web3-storage/w3up/compare/access-v15.0.0...access-v15.1.0) (2023-08-09)


### Features

* In-Memory Driver ([#847](https://github.com/web3-storage/w3up/issues/847)) ([23bb83a](https://github.com/web3-storage/w3up/commit/23bb83a3a8d4761385819f4d8af194d9f52466b0))

## [15.0.0](https://github.com/web3-storage/w3up/compare/access-v14.0.0...access-v15.0.0) (2023-07-21)


### ⚠ BREAKING CHANGES

* stop using access.web3.storage ([#833](https://github.com/web3-storage/w3up/issues/833))

### Features

* stop using access.web3.storage ([#833](https://github.com/web3-storage/w3up/issues/833)) ([0df3f2c](https://github.com/web3-storage/w3up/commit/0df3f2c0341244b2404702e8a8878cf0f6e31bc0))

## [14.0.0](https://github.com/web3-storage/w3up/compare/access-v13.0.2...access-v14.0.0) (2023-06-07)


### ⚠ BREAKING CHANGES

* merge `@web3-storage/access-api` into `@web3-storage/upload-api` ([#790](https://github.com/web3-storage/w3up/issues/790))

### Features

* merge `@web3-storage/access-api` into `@web3-storage/upload-api` ([#790](https://github.com/web3-storage/w3up/issues/790)) ([4f6ddb6](https://github.com/web3-storage/w3up/commit/4f6ddb690c365a42a3dc4c5c6898e4999bd0f868))
* w3 aggregate protocol client and api implementation ([#787](https://github.com/web3-storage/w3up/issues/787)) ([b58069d](https://github.com/web3-storage/w3up/commit/b58069d7960efe09283f3b23fed77515b62d4639))

## [13.0.2](https://github.com/web3-storage/w3up/compare/access-v13.0.1...access-v13.0.2) (2023-05-23)


### Bug Fixes

* upgrade remaining ucanto deps ([#798](https://github.com/web3-storage/w3up/issues/798)) ([7211501](https://github.com/web3-storage/w3up/commit/72115010663a62140127cdeed21f2dc37f59da08))

## [13.0.1](https://github.com/web3-storage/w3up/compare/access-v13.0.0...access-v13.0.1) (2023-05-22)


### Bug Fixes

* upgrade ucanto to 8 ([#794](https://github.com/web3-storage/w3up/issues/794)) ([00b011d](https://github.com/web3-storage/w3up/commit/00b011d87f628d4b3040398ca6cba567a69713ff))

## [13.0.0](https://github.com/web3-storage/w3up/compare/access-v12.0.2...access-v13.0.0) (2023-05-02)


### ⚠ BREAKING CHANGES

* upgrade to ucanto7.x.x ([#774](https://github.com/web3-storage/w3up/issues/774))

### Features

* upgrade to ucanto7.x.x ([#774](https://github.com/web3-storage/w3up/issues/774)) ([0cc6e66](https://github.com/web3-storage/w3up/commit/0cc6e66a80476e05c75bea94c1bee9bd12cbacf5))

## [12.0.2](https://github.com/web3-storage/w3up/compare/access-v12.0.1...access-v12.0.2) (2023-05-01)


### Bug Fixes

* account for edge cases in polling abort ([#763](https://github.com/web3-storage/w3up/issues/763)) ([e11a37d](https://github.com/web3-storage/w3up/commit/e11a37d1ff609f93b3f450546b75a62313e152a9))

## [12.0.1](https://github.com/web3-storage/w3up/compare/access-v12.0.0...access-v12.0.1) (2023-04-14)


### Bug Fixes

* stop polling after authorizeWaitAndClaim is aborted ([#756](https://github.com/web3-storage/w3up/issues/756)) ([55cd89d](https://github.com/web3-storage/w3up/commit/55cd89de5ef491248c5f8acdcb0b3da0e6f4351d))

## [12.0.0](https://github.com/web3-storage/w3up/compare/access-v11.2.0...access-v12.0.0) (2023-04-06)


### ⚠ BREAKING CHANGES

* add did mailto package, replacing `createDidMailtoFromEmail` ([#722](https://github.com/web3-storage/w3up/issues/722))

### Features

* add did mailto package, replacing `createDidMailtoFromEmail` ([#722](https://github.com/web3-storage/w3up/issues/722)) ([b48c256](https://github.com/web3-storage/w3up/commit/b48c256bfa57dda5d1762f77e41af1ecddf35846))

## [11.2.0](https://github.com/web3-storage/w3protocol/compare/access-v11.1.0...access-v11.2.0) (2023-03-29)


### Features

* allow importing a space with restricted abilities ([#685](https://github.com/web3-storage/w3protocol/issues/685)) ([a711b9b](https://github.com/web3-storage/w3protocol/commit/a711b9ba92c1b3d2a25e91c538234de62af6f485))

## [11.1.0](https://github.com/web3-storage/w3protocol/compare/access-v11.0.1...access-v11.1.0) (2023-03-29)


### Features

* get `access/claim` authorization wait function working ([#666](https://github.com/web3-storage/w3protocol/issues/666)) ([83971de](https://github.com/web3-storage/w3protocol/commit/83971de683b5fccbbc7ae36b7cb34d62a9930349))


### Bug Fixes

* verify proofs exist for requested delegation capabilities ([#670](https://github.com/web3-storage/w3protocol/issues/670)) ([068e801](https://github.com/web3-storage/w3protocol/commit/068e801ef849c9ebeacdc85eda75005e28a67b86))

## [11.0.1](https://github.com/web3-storage/w3protocol/compare/access-v11.0.0...access-v11.0.1) (2023-03-28)


### Bug Fixes

* @web3-storage/access/agent no longer exports authorizeWithPollClaim ([#656](https://github.com/web3-storage/w3protocol/issues/656)) ([a8be429](https://github.com/web3-storage/w3protocol/commit/a8be429e9d60bfe3a32ae7ca0fe7f2ef6e88ff4d))

## [11.0.0](https://github.com/web3-storage/w3protocol/compare/access-v11.0.0-rc.0...access-v11.0.0) (2023-03-23)


### Features

* move access-api delegation bytes out of d1 and into r2 ([#578](https://github.com/web3-storage/w3protocol/issues/578)) ([4510c9a](https://github.com/web3-storage/w3protocol/commit/4510c9a8c4389ca975d66f5c9592bce326bbc1c7))

## [11.0.0-rc.0](https://github.com/web3-storage/w3protocol/compare/access-v10.0.0...access-v11.0.0-rc.0) (2023-03-20)


### ⚠ BREAKING CHANGES

* implement new account-based multi-device flow ([#433](https://github.com/web3-storage/w3protocol/issues/433))

### Features

* define `access/confirm` handler and use it in ucanto-test-utils registerSpaces + validate-email handler ([#530](https://github.com/web3-storage/w3protocol/issues/530)) ([b1bbc90](https://github.com/web3-storage/w3protocol/commit/b1bbc907c96cfc7788f50fb0c154d9b54894e03e))
* implement new account-based multi-device flow ([#433](https://github.com/web3-storage/w3protocol/issues/433)) ([1ddc6a0](https://github.com/web3-storage/w3protocol/commit/1ddc6a0c53f8cdb6837a315d8aaf567100dfb8d7))
* move validation flow to a Durable Object to make it ⏩ fast ⏩ fast ⏩ fast ⏩  ([#449](https://github.com/web3-storage/w3protocol/issues/449)) ([02d7552](https://github.com/web3-storage/w3protocol/commit/02d75522b1ed794d267880e5f8a4fc3964066992))
* space/info will not error for spaces that have had storage provider added via provider/add ([#510](https://github.com/web3-storage/w3protocol/issues/510)) ([ea4e872](https://github.com/web3-storage/w3protocol/commit/ea4e872475c74165b08016c210e65b4062a2ffb6))


### Miscellaneous Chores

* **access-client:** release 11.0.0-rc.0 ([#573](https://github.com/web3-storage/w3protocol/issues/573)) ([be4386d](https://github.com/web3-storage/w3protocol/commit/be4386d66ceea393f289adb3c79273c250542807))

## [10.0.0](https://github.com/web3-storage/w3protocol/compare/access-v9.4.0...access-v10.0.0) (2023-03-08)


### ⚠ BREAKING CHANGES

* upgrade capabilities to latest ucanto ([#463](https://github.com/web3-storage/w3protocol/issues/463))

### Features

* access-api handles provider/add invocations ([#462](https://github.com/web3-storage/w3protocol/issues/462)) ([5fb56f7](https://github.com/web3-storage/w3protocol/commit/5fb56f794529f3d4de2b4597c47503002767fabb))
* access-api serves access/claim invocations ([#456](https://github.com/web3-storage/w3protocol/issues/456)) ([baacf35](https://github.com/web3-storage/w3protocol/commit/baacf3553ce8de0ca75e0815da849cca65ec880a))
* handle access/delegate invocations without error ([#427](https://github.com/web3-storage/w3protocol/issues/427)) ([4f0bd1c](https://github.com/web3-storage/w3protocol/commit/4f0bd1c1cd3cfb1c848892ad418c6d7b2197045a))
* upgrade capabilities to latest ucanto ([#463](https://github.com/web3-storage/w3protocol/issues/463)) ([2d786ee](https://github.com/web3-storage/w3protocol/commit/2d786ee81a6eb72c4782548ad3e3796fe3947fa5))
* upgrade to new ucanto ([#498](https://github.com/web3-storage/w3protocol/issues/498)) ([dcb41a9](https://github.com/web3-storage/w3protocol/commit/dcb41a9981c2b6bebbdbd29debcad9f510383680))


### Bug Fixes

* allow injecting email ([#466](https://github.com/web3-storage/w3protocol/issues/466)) ([e19847f](https://github.com/web3-storage/w3protocol/commit/e19847fef804fed33f709ec8b78640fff21ca01e))

## [9.4.0](https://github.com/web3-storage/w3protocol/compare/access-v9.3.0...access-v9.4.0) (2023-02-23)


### Features

* add support for access/authorize and update ([#392](https://github.com/web3-storage/w3protocol/issues/392)) ([9c8ca0b](https://github.com/web3-storage/w3protocol/commit/9c8ca0b385c940c8f0c21ee9edde093d2dcab8b8)), closes [#386](https://github.com/web3-storage/w3protocol/issues/386)


### Bug Fixes

* look for URL in channel before falling back to default ([#440](https://github.com/web3-storage/w3protocol/issues/440)) ([0741295](https://github.com/web3-storage/w3protocol/commit/0741295768af977dc5b3e35acabe4de85f9660b5))

## [9.4.0](https://github.com/web3-storage/w3protocol/compare/access-v9.3.0...access-v9.4.0) (2023-02-21)


### Features

* add support for access/authorize and update ([#392](https://github.com/web3-storage/w3protocol/issues/392)) ([9c8ca0b](https://github.com/web3-storage/w3protocol/commit/9c8ca0b385c940c8f0c21ee9edde093d2dcab8b8)), closes [#386](https://github.com/web3-storage/w3protocol/issues/386)


### Bug Fixes

* look for URL in channel before falling back to default ([#440](https://github.com/web3-storage/w3protocol/issues/440)) ([0741295](https://github.com/web3-storage/w3protocol/commit/0741295768af977dc5b3e35acabe4de85f9660b5))

## [9.3.0](https://github.com/web3-storage/w3protocol/compare/access-v9.2.0...access-v9.3.0) (2023-01-30)


### Features

* access-api forwards store/ and upload/ invocations to upload-api ([#334](https://github.com/web3-storage/w3protocol/issues/334)) ([b773376](https://github.com/web3-storage/w3protocol/commit/b77337692d9e4580031c429c429d4055d6f6ebff))
* access-api handling store/info for space not in db returns failure with name ([#391](https://github.com/web3-storage/w3protocol/issues/391)) ([9610fcf](https://github.com/web3-storage/w3protocol/commit/9610fcf31b9dbf57cc5ec3ca31f642f7ab6190de))
* update @ucanto/* to ~4.2.3 ([#405](https://github.com/web3-storage/w3protocol/issues/405)) ([50c0c80](https://github.com/web3-storage/w3protocol/commit/50c0c80789c26b777e854b7208b7391499d2ef18))
* update access-api ucanto proxy to not need a signer ([#390](https://github.com/web3-storage/w3protocol/issues/390)) ([71cbeb7](https://github.com/web3-storage/w3protocol/commit/71cbeb718d0a5132b97efa1173a5aaf9c75cbe80))


### Bug Fixes

* remove unecessary awaits ([#352](https://github.com/web3-storage/w3protocol/issues/352)) ([64da6e5](https://github.com/web3-storage/w3protocol/commit/64da6e50c4d0d0ef3b78c00298769665463e421d))

## [9.2.0](https://github.com/web3-storage/w3protocol/compare/access-v9.1.1...access-v9.2.0) (2022-12-14)


### Features

* embedded key resolution ([#312](https://github.com/web3-storage/w3protocol/issues/312)) ([4da91d5](https://github.com/web3-storage/w3protocol/commit/4da91d5f7f798d0d46c4df2aaf224610a8760d9e))

## [9.1.1](https://github.com/web3-storage/w3protocol/compare/access-v9.1.0...access-v9.1.1) (2022-12-14)


### Bug Fixes

* access-client/src/agent default PRINCIPAL is did:web:web3.storage ([#296](https://github.com/web3-storage/w3protocol/issues/296)) ([27f2f60](https://github.com/web3-storage/w3protocol/commit/27f2f60dac7c95cb9efd42a28f5abfef8bdeb197))
* add support for did:web in the cli ([#301](https://github.com/web3-storage/w3protocol/issues/301)) ([885f7c1](https://github.com/web3-storage/w3protocol/commit/885f7c15cec7a0724fcc4a8dd5eb0146a918373d))
* fix client cli service did resolve ([#292](https://github.com/web3-storage/w3protocol/issues/292)) ([6be9608](https://github.com/web3-storage/w3protocol/commit/6be9608a907665a8123938ef804bebfffc5c7232))

## [9.1.0](https://github.com/web3-storage/w3protocol/compare/access-v9.0.1...access-v9.1.0) (2022-12-13)


### Features

* sync encode/decode delegations ([#276](https://github.com/web3-storage/w3protocol/issues/276)) ([ab981fb](https://github.com/web3-storage/w3protocol/commit/ab981fb6e33799153022c0f6d06c282917e7af7c))


### Bug Fixes

* destructured save ([#272](https://github.com/web3-storage/w3protocol/issues/272)) ([a4f20a9](https://github.com/web3-storage/w3protocol/commit/a4f20a928ceddc05c22f1aed5c80c2716848a284))
* handle Buffer serialization ([#277](https://github.com/web3-storage/w3protocol/issues/277)) ([6dc77ca](https://github.com/web3-storage/w3protocol/commit/6dc77ca51d4e59406f33c30014073916c9034a24))
* make d1 spaces.metadata nullable and change to kysely ([#284](https://github.com/web3-storage/w3protocol/issues/284)) ([c8a9ce5](https://github.com/web3-storage/w3protocol/commit/c8a9ce544226b3c8456d45b15e29cec84894aeb8)), closes [#280](https://github.com/web3-storage/w3protocol/issues/280)

## [9.0.1](https://github.com/web3-storage/w3protocol/compare/access-v9.0.0...access-v9.0.1) (2022-12-08)


### Bug Fixes

* validate agent name ([#271](https://github.com/web3-storage/w3protocol/issues/271)) ([cdccbd3](https://github.com/web3-storage/w3protocol/commit/cdccbd3a7bcc4eaddb0ecee68336c04ac35d7996))

## [9.0.0](https://github.com/web3-storage/w3protocol/compare/access-v8.0.1...access-v9.0.0) (2022-12-07)


### ⚠ BREAKING CHANGES

* upgrade access-api @ucanto/* and @ipld/dag-ucan major versions ([#246](https://github.com/web3-storage/w3protocol/issues/246))

### Features

* upgrade access-api @ucanto/* and @ipld/dag-ucan major versions ([#246](https://github.com/web3-storage/w3protocol/issues/246)) ([5e663d1](https://github.com/web3-storage/w3protocol/commit/5e663d12ccea7d21cc8e7c36869f144a08eaa1b0))

## [8.0.1](https://github.com/web3-storage/w3protocol/compare/access-v8.0.0...access-v8.0.1) (2022-12-07)


### Bug Fixes

* conf driver can store top level undefined ([c4b216b](https://github.com/web3-storage/w3protocol/commit/c4b216b364fff62b7be9b2f76d6626bfc01c2332))

## [8.0.0](https://github.com/web3-storage/w3protocol/compare/access-v7.0.2...access-v8.0.0) (2022-12-06)


### ⚠ BREAKING CHANGES

* access-client store decoupling ([#228](https://github.com/web3-storage/w3protocol/issues/228))
* upgrade to `@ucanto/{interface,principal}`@^4.0.0 ([#238](https://github.com/web3-storage/w3protocol/issues/238))
* follow up on the capabilities extract ([#239](https://github.com/web3-storage/w3protocol/issues/239))

### Features

* **access-client:** cli and recover ([#207](https://github.com/web3-storage/w3protocol/issues/207)) ([adb3a8d](https://github.com/web3-storage/w3protocol/commit/adb3a8d61d42b31f106e86b95faa3e442f5dc2c7))
* follow up on the capabilities extract ([#239](https://github.com/web3-storage/w3protocol/issues/239)) ([ef5e779](https://github.com/web3-storage/w3protocol/commit/ef5e77922b67155f0c3e5cb37c12e32f9a56cce1))
* Revert "feat!: upgrade to `@ucanto/{interface,principal}`@^4.0.0" ([#245](https://github.com/web3-storage/w3protocol/issues/245)) ([c182bbe](https://github.com/web3-storage/w3protocol/commit/c182bbe5e8c5a7d5c74b10cbf4b7a45b51e9b184))
* upgrade to `@ucanto/{interface,principal}`@^4.0.0 ([#238](https://github.com/web3-storage/w3protocol/issues/238)) ([2f3bab8](https://github.com/web3-storage/w3protocol/commit/2f3bab8924fe7f34a5db64d2521730fc85739d3a))


### Bug Fixes

* connection method is not async ([#222](https://github.com/web3-storage/w3protocol/issues/222)) ([0dd1633](https://github.com/web3-storage/w3protocol/commit/0dd16338836d96bd0dcb920385e4ed90f16c45ad))


### Code Refactoring

* access-client store decoupling ([#228](https://github.com/web3-storage/w3protocol/issues/228)) ([a785278](https://github.com/web3-storage/w3protocol/commit/a7852785e2ac783bcb21790b4a87ee5ad0a1380e))

## [7.0.2](https://github.com/web3-storage/w3protocol/compare/access-v7.0.1...access-v7.0.2) (2022-11-28)


### Bug Fixes

* round trip delegations in IndexedDB store ([#216](https://github.com/web3-storage/w3protocol/issues/216)) ([e111ea3](https://github.com/web3-storage/w3protocol/commit/e111ea310ae8fbe88c33c87a82c3269a46dd2955))

## [7.0.1](https://github.com/web3-storage/w3protocol/compare/access-v7.0.0...access-v7.0.1) (2022-11-24)


### Bug Fixes

* add proof of voucher redeem ([#213](https://github.com/web3-storage/w3protocol/issues/213)) ([f96f813](https://github.com/web3-storage/w3protocol/commit/f96f813e3e0656ef293bfb2191c7e97cb07e01bb))

## [7.0.0](https://github.com/web3-storage/w3protocol/compare/access-v6.1.0...access-v7.0.0) (2022-11-24)


### ⚠ BREAKING CHANGES

* rename static indexeddb store method create to open ([#211](https://github.com/web3-storage/w3protocol/issues/211))

### Features

* rename static indexeddb store method create to open ([#211](https://github.com/web3-storage/w3protocol/issues/211)) ([8744a1e](https://github.com/web3-storage/w3protocol/commit/8744a1e12bd720cc373af6140ce5a18c6b094e2e))


### Bug Fixes

* allow custom store name ([81f27eb](https://github.com/web3-storage/w3protocol/commit/81f27ebdbbeedb7eaa258284cee2bd11d245d823))
* export map for agent ([#212](https://github.com/web3-storage/w3protocol/issues/212)) ([a6367ee](https://github.com/web3-storage/w3protocol/commit/a6367eeaa1f9a368001078af498df3a536f6da62))

## [6.1.0](https://github.com/web3-storage/w3protocol/compare/access-v6.0.1...access-v6.1.0) (2022-11-23)


### Features

* optional space name ([#202](https://github.com/web3-storage/w3protocol/issues/202)) ([4b7cf64](https://github.com/web3-storage/w3protocol/commit/4b7cf64a526930fdabf531adcb4b0198a37098d5))

## [6.0.1](https://github.com/web3-storage/w3protocol/compare/access-v6.0.0...access-v6.0.1) (2022-11-22)


### Bug Fixes

* make sure client dont break each other builds because of types. ([#195](https://github.com/web3-storage/w3protocol/issues/195)) ([ab395af](https://github.com/web3-storage/w3protocol/commit/ab395af2ec4c313025d036d83126ee933b027f60))

## [6.0.0](https://github.com/web3-storage/w3protocol/compare/access-v5.0.2...access-v6.0.0) (2022-11-22)


### ⚠ BREAKING CHANGES

* **access-client:** bump to major
* store/list and upload/list types now require nb object with optional properties

### Features

* [#153](https://github.com/web3-storage/w3protocol/issues/153) ([#177](https://github.com/web3-storage/w3protocol/issues/177)) ([d6d448c](https://github.com/web3-storage/w3protocol/commit/d6d448c16f188398c30f2d1b83f69e1d7becd450))
* **access-client:** bump to major ([4d5899f](https://github.com/web3-storage/w3protocol/commit/4d5899f7ad7e7b4901dcc773d7348d3d09c4dca9))
* account recover with email ([#149](https://github.com/web3-storage/w3protocol/issues/149)) ([6c659ba](https://github.com/web3-storage/w3protocol/commit/6c659ba68d23c3448d5150bc76f1ddcb91ae18d8))
* add support for list pagination in list capability invocations ([#184](https://github.com/web3-storage/w3protocol/issues/184)) ([ced23db](https://github.com/web3-storage/w3protocol/commit/ced23db27f3b2a6122d4d0a684264f64b26ac95f))

## [5.0.2](https://github.com/web3-storage/w3protocol/compare/access-v5.0.1...access-v5.0.2) (2022-11-16)


### Bug Fixes

* workaround for ts bug in upload capabilities ([#171](https://github.com/web3-storage/w3protocol/issues/171)) ([b8d05b2](https://github.com/web3-storage/w3protocol/commit/b8d05b2ebabafa3081378bbc186fc766dde256c3))

## [5.0.1](https://github.com/web3-storage/w3protocol/compare/access-v5.0.0...access-v5.0.1) (2022-11-16)


### Bug Fixes

* workaround ts bug & generate valid typedefs ([#169](https://github.com/web3-storage/w3protocol/issues/169)) ([0b02d14](https://github.com/web3-storage/w3protocol/commit/0b02d14cc5b51ac0a0b8c879a917430ce0617dc7))

## [5.0.0](https://github.com/web3-storage/w3protocol/compare/access-v4.0.2...access-v5.0.0) (2022-11-15)


### ⚠ BREAKING CHANGES

* doc capabilities & make requierd nb non-optionals (#159)

### Features

* doc capabilities & make requierd nb non-optionals ([#159](https://github.com/web3-storage/w3protocol/issues/159)) ([6496773](https://github.com/web3-storage/w3protocol/commit/6496773f2a4977e06126ade37ae9dfc218b05f7f))

## [4.0.2](https://github.com/web3-storage/w3-protocol/compare/access-v4.0.1...access-v4.0.2) (2022-11-04)


### Bug Fixes

* upload/add capability root validation ([#136](https://github.com/web3-storage/w3-protocol/issues/136)) ([aae5b66](https://github.com/web3-storage/w3-protocol/commit/aae5b66112e6783054302b1f718f4c351aa80f3f))

## [4.0.1](https://github.com/web3-storage/w3-protocol/compare/access-v4.0.0...access-v4.0.1) (2022-11-04)


### Bug Fixes

* make multiformats 9 go away ([#133](https://github.com/web3-storage/w3-protocol/issues/133)) ([2668880](https://github.com/web3-storage/w3-protocol/commit/2668880a23c28ee45596fb1bc978564908a17e18))

## [4.0.0](https://github.com/web3-storage/w3-protocol/compare/access-v3.1.2...access-v4.0.0) (2022-11-01)


### ⚠ BREAKING CHANGES

* Remove 0.8 caps and add account delegation to the service (#123)

### Features

* add cancel create account ([#132](https://github.com/web3-storage/w3-protocol/issues/132)) ([feec113](https://github.com/web3-storage/w3-protocol/commit/feec113f04bd3b5c2eb570ba88e8c14274f91ed6))
* Remove 0.8 caps and add account delegation to the service ([#123](https://github.com/web3-storage/w3-protocol/issues/123)) ([878f8c9](https://github.com/web3-storage/w3-protocol/commit/878f8c9a38f02dac509ef0b4437ab3d1b8467eb3)), closes [#117](https://github.com/web3-storage/w3-protocol/issues/117) [#121](https://github.com/web3-storage/w3-protocol/issues/121)


### Bug Fixes

* throw error or return value ([#131](https://github.com/web3-storage/w3-protocol/issues/131)) ([cddf7b9](https://github.com/web3-storage/w3-protocol/commit/cddf7b9884d5f7c83f734e1c9d8543a1fc0a80ad))

## [3.1.2](https://github.com/web3-storage/w3-protocol/compare/access-v3.1.1...access-v3.1.2) (2022-10-27)


### Bug Fixes

* publish all dist ts files ([#127](https://github.com/web3-storage/w3-protocol/issues/127)) ([f705073](https://github.com/web3-storage/w3-protocol/commit/f70507305080c08665f51f7b75bd388048be86f7))

## [3.1.1](https://github.com/web3-storage/w3-protocol/compare/access-v3.1.0...access-v3.1.1) (2022-10-27)


### Bug Fixes

* export stores ([#125](https://github.com/web3-storage/w3-protocol/issues/125)) ([d3fa14d](https://github.com/web3-storage/w3-protocol/commit/d3fa14d6fe72c6b306b4a5b05276fbf137ab28f0))

## [3.1.0](https://github.com/web3-storage/w3-protocol/compare/access-v3.0.0...access-v3.1.0) (2022-10-26)


### Features

* add IndexedDB Store implementation ([#120](https://github.com/web3-storage/w3-protocol/issues/120)) ([9d73a26](https://github.com/web3-storage/w3-protocol/commit/9d73a26f7ab81f5baf9e7486ab99c1404a3dfff4))

## [3.0.0](https://github.com/web3-storage/w3-protocol/compare/access-v2.1.1...access-v3.0.0) (2022-10-24)


### ⚠ BREAKING CHANGES

* bump to 0.9 (#116)

### Features

* bump to 0.9 ([#116](https://github.com/web3-storage/w3-protocol/issues/116)) ([3e0b63f](https://github.com/web3-storage/w3-protocol/commit/3e0b63f38aace3a86655a1aa40e529c1501dc136))
* use modules and setup ([#99](https://github.com/web3-storage/w3-protocol/issues/99)) ([b060c0b](https://github.com/web3-storage/w3-protocol/commit/b060c0b299ee55dbe7820231c63be90129a39652)), closes [#98](https://github.com/web3-storage/w3-protocol/issues/98)


### Bug Fixes

* 0.9 ([#78](https://github.com/web3-storage/w3-protocol/issues/78)) ([1b1ed01](https://github.com/web3-storage/w3-protocol/commit/1b1ed01d537e88bbdeb5ea2aeb967b27bd11f87d))

## [2.1.1](https://github.com/web3-storage/w3-protocol/compare/access-v2.1.0...access-v2.1.1) (2022-10-10)


### Bug Fixes

* regression from `store -> all` name change ([#90](https://github.com/web3-storage/w3-protocol/issues/90)) ([14a6a5b](https://github.com/web3-storage/w3-protocol/commit/14a6a5b72deca8391420aa0c2dba9eac2d912ef2))

## [2.1.0](https://github.com/web3-storage/w3-protocol/compare/access-v2.0.0...access-v2.1.0) (2022-10-04)


### Features

* implement store/add size constraint ([#89](https://github.com/web3-storage/w3-protocol/issues/89)) ([efd8a2f](https://github.com/web3-storage/w3-protocol/commit/efd8a2faa0348ba9d467ca0c306ddec95aa6d05f))
* upload/* capabilities ([#81](https://github.com/web3-storage/w3-protocol/issues/81)) ([6c0e24f](https://github.com/web3-storage/w3-protocol/commit/6c0e24f50e08ece893666be2a5b46237df5cc83f))

## [2.0.0](https://github.com/web3-storage/w3-protocol/compare/access-v1.0.0...access-v2.0.0) (2022-09-30)


### ⚠ BREAKING CHANGES

* new accounts (#72)

### Features

* new accounts ([#72](https://github.com/web3-storage/w3-protocol/issues/72)) ([9f6cb41](https://github.com/web3-storage/w3-protocol/commit/9f6cb419d33b9446dd80f8541228096cf2677d45))

## [1.0.0](https://github.com/web3-storage/ucan-protocol/compare/access-v0.2.0...access-v1.0.0) (2022-09-21)


### ⚠ BREAKING CHANGES

* awake (#66)

### Features

* awake ([#66](https://github.com/web3-storage/ucan-protocol/issues/66)) ([bb66f57](https://github.com/web3-storage/ucan-protocol/commit/bb66f5772049e3363a753ea5b336c2fa1e42911e))

## [0.2.0](https://github.com/web3-storage/ucan-protocol/compare/access-v0.1.1...access-v0.2.0) (2022-09-16)


### Features

* abortable pullRegisterDelegation ([#56](https://github.com/web3-storage/ucan-protocol/issues/56)) ([adc0568](https://github.com/web3-storage/ucan-protocol/commit/adc0568e9f521d978b9886f483c42e61d27515b6))
* **access-api:** new email template ([cc19320](https://github.com/web3-storage/ucan-protocol/commit/cc193202e385d8079144aa90e989af07cf743b0b))
* fail validate for register email and add metrics ([0916ba6](https://github.com/web3-storage/ucan-protocol/commit/0916ba6bda8ad46ccc4f6bb0c6f4a48dd99db0c8))
* update deps ([d276375](https://github.com/web3-storage/ucan-protocol/commit/d2763750159ad56132f0b002ff5f50cc36fce20c))


### Bug Fixes

* add analytics to staging and prod ([14941d9](https://github.com/web3-storage/ucan-protocol/commit/14941d901e48e92896cc962b3e93488731afa381))
* new email for notifications ([57b6845](https://github.com/web3-storage/ucan-protocol/commit/57b6845a56f534505eeabdf5ab2e20f8b37c9532))
* temporary patch to allow fetch usage in browser ([#57](https://github.com/web3-storage/ucan-protocol/issues/57)) ([beff06e](https://github.com/web3-storage/ucan-protocol/commit/beff06ea5f4b0adb08f6fce59422373d818c3b9e))

## [0.1.1](https://github.com/web3-storage/ucan-protocol/compare/access-v0.1.0...access-v0.1.1) (2022-08-25)


### Bug Fixes

* proper envs and update deps ([d5dccb6](https://github.com/web3-storage/ucan-protocol/commit/d5dccb6e9c23b5ddbdffa4c67c04d195524b38f2))

## [0.1.0](https://github.com/web3-storage/ucan-protocol/compare/access-v0.0.1...access-v0.1.0) (2022-08-24)


### Features

* resync ([5cae9cd](https://github.com/web3-storage/ucan-protocol/commit/5cae9cd55cfcc06046eb23a2f33931299dd07ff5))
* sdk ([305b2d3](https://github.com/web3-storage/ucan-protocol/commit/305b2d317ba4b8743a1594e9dbe0d22bac90c229))
* sdk and cli ([2373447](https://github.com/web3-storage/ucan-protocol/commit/2373447db93ee16276f45fbfe40e4b98c28b6ab7))


### Bug Fixes

* **access:** export cap types ([ae70810](https://github.com/web3-storage/ucan-protocol/commit/ae7081015422abf88e8dbf0bedede805d6b05297))
