# Source manifest

The raw benchmark artifacts are authoritative. These identities make the
source/binary state independently checkable:

- Nitro parent revision: `ca1688de5850c180722e4186b29c68aba3af9b57`
  (the benchmark was captured from the current dirty optimization worktree).
- Nitro baseline tag: `pre-architecture-baseline-jdk26-20260723`
  (the exact optimization worktree and the raw baseline artifacts were frozen
  together before architectural cleanup began).
- Nitro `src/` aggregate SHA-256:
  `6fc03559542713480cd6a089adaec6af66351bc67ddd24f2bfbb425c843b09a9`.
- Nitro `pom.xml` SHA-256:
  `05b68ba224ccd78aa0bb4defeb454f64eb4d53da205926b69757d57e4ea149ae`.
- Velox parent revision: `0671c9d52ae8277e2a4ed0291b63e4766107e461`.
- Velox TPC-H binary SHA-256:
  `dc80651e3526e458a3444fe698679bbad55392fc1ff6b5999504180e16ae22aa`.
- Velox TPC-DS binary SHA-256 after the q14 current-source rebuild:
  `0b5aeb2532dc247cf29103d878553533ef7d64549f55b18ce3b8c6a87a37170a`.
- Velox ClickBench binary SHA-256:
  `b725d2999bfdd1c8a5575877a695ce93bac4ede5349286d7baf7166afe4a7ec4`.
- Velox harness source SHA-256:
  - TPC-H: `a46e8cf1501ea2d12f3ce055d690096411b790b55c634ce5595ecdbe0539ad19`
  - TPC-DS: `d7253d7ceb066561072be0fdc253ef5d4041d001b37fe082478b89cc352d1ddf`
  - ClickBench: `f5e3d12eca030767fd7e76c58854bd0962f83f6633f06452f81f824b7bac300c`

The q14 rebuild changes only the newly restored q14 switch case. Main-sweep
TPC-DS q01–q13 and q15–q99 artifacts retain the pre-rebuild binary invocation;
q14 is explicitly sourced from `recapture-q14-restored/`.
