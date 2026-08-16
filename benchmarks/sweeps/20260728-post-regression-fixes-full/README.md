# Post-regression-fix Nitro performance sweep

The broad sweep measures Nitro at `3d43366a` (`Separate source reborrow from
availability`) after the initial architecture-cleanup regressions were
investigated and repaired. A lower-noise follow-up in this directory also
localizes and repairs the remaining TPC-DS q39 regression. The comparison
baseline is the frozen July 23 capture in
`../20260723-pre-architecture-baseline`.

## Method

- JDK `/opt/java/openjdk`, version 26.0.1
- one unpinned JMH thread, `-Xmx12g`, transparent-huge-page request
- five one-second warmups, five one-second measurements, one fork
- normalized allocation plus instructions, cycles, L1 data-cache loads and
  misses, data-TLB loads and misses, branches, and branch misses
- TPCH, TPC-DS, and ClickBench run serially
- no JFR recording

The three broad suites completed successfully with all 165 expected results:
22 TPCH, 99 TPC-DS, and 44 ClickBench. Minimum available memory observed by
the suite monitors was 30,063,788 KiB.

## Broad result

Ratios are current divided by the July 23 baseline. Values above 1.0 are worse.

| Suite | Rows | Duration | Allocation | Instructions | Cycles |
|---|---:|---:|---:|---:|---:|
| TPCH | 22 | 1.014 | 0.996 | 1.017 | 1.021 |
| TPC-DS | 99 | 1.022 | 1.000 | 1.015 | 1.040 |
| ClickBench | 44 | 1.016 | 1.021 | 1.015 | 1.022 |
| All | 165 | 1.019 | 1.005 | 1.015 | 1.033 |

Before the regression fixes, the corresponding all-query geometric means were
1.028 duration, 1.033 allocation, 1.039 instructions, and 1.043 cycles.
Allocation is now effectively neutral and the instruction delta has fallen
from 3.9% to 1.5%.

## Focused outlier recapture

The broad sweep's largest newly visible single-fork outliers were recaptured
with three forks and the same counter policy:

| Query | Duration | Allocation | Instructions | Cycles |
|---|---:|---:|---:|---:|
| ClickBench q16 | 1.036 | 1.041 | 0.835 | 1.055 |
| TPC-DS q39 | 1.099 | 1.003 | 1.094 | 1.107 |
| TPC-DS q41 | 0.986 | 1.038 | 0.784 | 1.127 |
| TPC-DS q92 | 1.069 | 1.008 | 1.075 | 1.091 |

The q41 duration spike and ClickBench q16 instruction spike did not reproduce.
Q39 and q92 retained modest gaps in this first recapture.

## Stable q39/q92 confirmation and q39 repair

Q39 and q92 were then measured at both the frozen baseline and current
revision with five warmups, three measurements, and three forks. Q92's current
result was 5.8% faster while executing 5.9% more instructions, and its baseline
forks spanned 0.682–0.798 billion instructions. It is retained as compilation
and run-placement variation rather than a performance regression.

Q39 reproduced as a real regression: 521.2 ms and 11.469 billion instructions
versus the frozen baseline's 472.0 ms and 10.403 billion. History localized the
delta to removal of runtime count/average/stddev fusion. The repair does not
restore runtime recognition. Instead, the q39 physical plan explicitly selects
one immutable, multi-result integral count/average/stddev aggregation unit and
binds its result slots in logical output order. Aggregation operators remain
function-agnostic.

The identical three-fork protocol after that repair records:

| Metric | Pre-repair | Repaired | Frozen baseline | Repair vs pre | Repair vs baseline |
|---|---:|---:|---:|---:|---:|
| Duration | 521.225 ms | 479.311 ms | 471.962 ms | -8.04% | +1.56% |
| Allocation | 189.817 MB | 189.352 MB | 188.842 MB | -0.25% | +0.27% |
| Instructions | 11.469B | 10.626B | 10.403B | -7.35% | +2.14% |
| Cycles | 2.951B | 2.824B | 2.629B | -4.28% | +7.42% |

The cycle counter remains materially noisier than duration and instructions;
the recovered instruction and duration values are inside the normal
architecture-sweep envelope. The full JDK 26 suite passes 1,460 tests with
zero failures and 566 expected external-data skips. Real-SF10 repeated
execution and compiled q39 checks also pass. The two SQL-parity invocations
could not run because this checkout lacks the external Trino `q39b.sql`
resource.

The subsequent complete 99-query TPC-DS certification is in
`../20260728-post-q39-certification`. It records suite ratios of 1.0117
duration, 1.0013 allocation, 1.0146 instructions, and 1.0357 cycles versus the
frozen baseline. Combining that capture with the unchanged TPCH and ClickBench
results produces a final 165-query board of 1.0130/1.0056/1.0151/1.0301.

## Repaired-query validation

Separate three-fork validation covers the queries that drove the refactoring
regressions. Representative final values include:

- TPCH q16: 268.6 ms, 5.71 billion instructions, 183.7 MB allocated
- TPCH q21: 3.965 s and 60.34 billion instructions
- TPC-DS q02: 584.1 ms and 17.11 billion instructions
- TPC-DS q56/q60: 89.1/164.0 MB allocated
- TPC-DS q64: 2.048 s and 39.70 billion instructions
- TPC-DS q84: 73.7 ms and 1.471 billion instructions
- ClickBench q07: 84.7 ms and 1.393 billion instructions
- ClickBench q09/q10: 19.26/29.03 billion instructions

Q64 and q72 have known JVM compilation modes. Longer nine-measurement
validation records q64 at 38.47 billion instructions and q72 at 39.30 billion,
and q64's exact pre-regression Parquet decompression counts are restored.

## Native source-boundary allocation

A historical allocation A/B for ClickBench q07 localizes a remaining
approximately 7.3 MB/op increase to `c1324531` (`Expose Nitro Parquet as native
batch source`). Adjacent captures were:

| Revision | Duration | Allocation |
|---|---:|---:|
| `84a62f8d` | 185.4 ms | 72.34 MB |
| `e1717126` | 184.4 ms | 71.22 MB |
| `ce40af27` | 184.7 ms | 72.34 MB |
| `c1324531` | 187.9 ms | 79.63 MB |

This is per-batch SPI/source-generation and engine-ingress object cost at the
explicit native source boundary; vector payloads remain zero-copy. Eliminating
it would require bypassing that boundary or lifecycle-sensitive object pooling.
Neither is justified by the current architecture goals.

## Artifacts

- broad results and logs: `nitro-{tpch,tpcds,clickbench}.{json,log}`
- broad memory monitors: `nitro-{tpch,tpcds,clickbench}.memory.log`
- focused outliers: `focused-outliers.{json,log}`
- stable q39/q92 confirmation:
  `{baseline,current}-q39-q92-stable.{json,log}`
- repaired q39: `q39-planner-fusion-stable.{json,log}`
- q39 correctness and full-suite logs:
  `q39-planner-fusion-{focused-tests,real-tests,full-test}.log`
- launch state: `preflight-{processes,memory}.txt`
- repaired-query cohort and historical allocation A/B:
  `../20260728-post-regression-fixes`
