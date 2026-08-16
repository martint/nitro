# Post-module-split Nitro performance sweep

This directory compares Nitro at `30fc8bc1` (`Load connectors through isolated
SPI`) with the pre-architecture baseline in
`../20260723-pre-architecture-baseline`.

## Method

- JDK `/opt/java/openjdk`, version 26.0.1;
- one unpinned JMH thread, `-Xmx12g`, transparent-huge-page request;
- five one-second warmups, five one-second measurements, one fork for the broad
  sweep;
- normalized JMH allocation plus instructions, cycles, L1 data-cache loads and
  misses, data-TLB loads and misses, branches, and branch misses;
- three forks with the same warmup, measurement, allocation, and counter policy
  for focused outlier recaptures;
- suites run serially with a 16 GiB available-memory admission check;
- no JFR recording.

The broad sweep ran from 2026-07-27T23:24:57Z through
2026-07-28T00:12:07Z. Focused recaptures completed at 00:28:11Z. There was no
active swap-in or swap-out during spot checks.

## Correctness result

The current revision produced valid measurements for only 143 of the 165
baseline queries:

- TPCH: 20/22 valid; q03 and q22 failed;
- TPC-DS: 93/99 valid; q02, q23, q51, q58, q59, and q78 failed;
- ClickBench: 30/44 valid; q13, q17, q18, q22, q29, q34, q35, q37, q38, q39,
  q40, q41, q42, and q43 failed.

Six failures are type-identity incompatibilities introduced at the isolated
connector boundary:

- grouping rejects `benchmark:date`: TPCH q03; TPC-DS q23 and q51;
- hash join rejects `benchmark:integer`: TPC-DS q02, q59, and q78.

The other 16 failures throw `IllegalStateException: Output already closed`
while borrowing a `VectorColumnGeneration` through
`NitroParquetScanOperator.CompatibilityVectorColumnIngress`: TPCH q22,
TPC-DS q58, and all 14 failed ClickBench queries.

## Broad matched-row comparison

Ratios are current divided by the July 23 baseline; values above 1.0 are worse
for all four metrics.

| Suite | Rows | Duration gmean | Allocation gmean | Instructions gmean | Cycles gmean |
|---|---:|---:|---:|---:|---:|
| TPCH | 20 | 1.043 | 1.005 | 1.049 | 1.051 |
| TPC-DS | 93 | 1.010 | 1.000 | 1.018 | 1.033 |
| ClickBench | 30 | 1.041 | 1.019 | 1.049 | 1.044 |
| All matched | 143 | 1.021 | 1.005 | 1.029 | 1.038 |

ClickBench q07 alone contributes heavily to the aggregate. Excluding q07, the
142-row duration geometric mean is 1.015, allocation is 1.003, instructions
are 1.023, and cycles are 1.032.

## Focused three-fork recaptures

| Query | Duration | Allocation | Instructions | Cycles | Interpretation |
|---|---:|---:|---:|---:|---|
| TPCH q02 | 1.029 | 1.005 | 1.020 | 1.031 | small regression |
| TPCH q16 | 1.072 | 0.995 | 1.169 | 1.068 | instruction regression |
| TPCH q20 | 1.014 | 1.008 | 1.002 | 1.002 | broad slowdown mostly noise |
| TPCH q21 | 1.082 | 1.040 | 1.181 | 1.088 | real instruction/allocation regression |
| TPC-DS q70 | 1.447 | 1.004 | 1.151 | 1.433 | unstable path: two forks near 615–660 ms, one at 1.26–1.50 s |
| TPC-DS q72 | 1.129 | 1.006 | 1.048 | 1.142 | variable duration/cycles |
| TPC-DS q82 | 1.036 | 1.013 | 1.022 | 1.046 | broad slowdown mostly noise |
| TPC-DS q84 | 1.117 | 1.019 | 1.104 | 1.136 | stable regression |
| ClickBench q07 | 2.337 | 1.230 | 2.379 | 2.330 | large, stable regression also present in both July 26 sweeps |
| ClickBench q10 | 1.147 | 1.039 | 1.238 | 1.157 | real instruction regression |
| ClickBench q33 | 1.082 | 1.002 | 1.040 | 1.105 | moderate stable regression |

## Conclusion

The architecture cleanup is not yet performance- or correctness-neutral.
Allocation is broadly close to baseline, but matched valid queries execute
about 2.9% more instructions and consume about 3.8% more cycles in the broad
sweep. The strongest confirmed performance issues are ClickBench q07, q10,
and q33; TPCH q16 and q21; stable TPC-DS q84; and the fork-dependent slow path
in TPC-DS q70.

Correctness takes priority: repair connector-boundary type identity and
compatibility-ingress output ownership, rerun focused failed-query tests, and
then recapture the complete 165-query board before using overall performance
aggregates as release evidence.
