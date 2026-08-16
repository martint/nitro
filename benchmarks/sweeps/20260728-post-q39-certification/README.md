# Post-q39 TPC-DS certification

This directory certifies TPC-DS after `648447dc` (`Select fused statistical
aggregate plan`). The comparison baseline is the frozen July 23 capture in
`../20260723-pre-architecture-baseline`. TPCH and ClickBench were not rerun
because the committed physical-plan change is selected only by TPC-DS q39.

## Method

- JDK `/opt/java/openjdk`, version 26.0.1
- one unpinned JMH thread, `-Xmx12g`, transparent-huge-page request
- five one-second warmups, five one-second measurements, one fork
- normalized allocation, instructions, cycles, cache/TLB, and branch counters
- all 99 TPC-DS benchmarks run serially
- no JFR recording

All 99 expected results completed successfully. Minimum available memory was
37,050,424 KiB.

## Suite result

Ratios are current divided by the July 23 baseline. Values above 1.0 are
worse.

| Suite | Rows | Duration | Allocation | Instructions | Cycles |
|---|---:|---:|---:|---:|---:|
| TPC-DS | 99 | 1.0117 | 1.0013 | 1.0146 | 1.0357 |

The preceding post-regression-fix TPC-DS sweep measured
1.0221/1.0003/1.0149/1.0403. The certification therefore improves duration
about 1.0%, leaves allocation and instructions effectively unchanged, and
improves cycles about 0.45%.

Combining this TPC-DS capture with the unchanged final TPCH and ClickBench
captures gives a 165-query board of 1.0130 duration, 1.0056 allocation,
1.0151 instructions, and 1.0301 cycles. The preceding combined board was
1.0193/1.0050/1.0153/1.0329.

## Q39

The certification capture records q39 at 533.8 ms, 10.939 billion
instructions, 3.000 billion cycles, and 189.73 MB allocated. Instructions fall
5.5% from the preceding broad sweep's 11.572 billion. The noisier one-fork
duration is interpreted together with the post-fix three-fork cohort in
`../20260728-post-regression-fixes-full/q39-planner-fusion-stable.json`, which
records 479.3 ms and 10.626 billion instructions. Both instruction captures
confirm that the planner-authored shared traversal is active.

## Focused outliers

The broad capture's new q97 and q81 timing outliers were recaptured with five
warmups, three measurements, and three forks.

- q97 collapsed to 1.059x baseline duration and 0.994x instructions. The broad
  timing was noise.
- q81 initially retained 1.220x duration and 1.060x instructions. An exact
  parent/current A/B then measured parent `3d43366a` at 4.670 billion
  instructions and current at 4.700 billion. After restoring current modules,
  the reverse current capture improved to 4.596 billion instructions, below
  the parent. Allocation also improves about 3% from parent to current.

Q81 is therefore a pre-existing JVM compilation/run-placement mode, not a
regression from the q39 physical-plan change. Its stable instruction counter
is neutral in the exact parent/current comparison; the large wall-time
movement is not accompanied by corresponding engine work.

## Validation

The production/test slice was already validated before this certification:

- focused physical-unit lifecycle and result-slot test passed;
- real-SF10 repeated-execution and compiled q39 tests passed;
- full JDK 26 suite: 1,460 tests, zero failures/errors, 566 expected skips.

The two optional SQL-parity invocations could not start because this checkout
lacks the external Trino `q39b.sql` resource.

## Artifacts

- full certification: `nitro-tpcds.{json,log,memory.log}`
- focused q81/q97 recapture: `focused-q81-q97.{json,log}`
- exact parent q81: `parent-q81.{json,log}` and `parent-q81-build.log`
- reverse current q81: `current-q81-reverse.{json,log}`
- current-module restoration: `current-restore-build.log`

