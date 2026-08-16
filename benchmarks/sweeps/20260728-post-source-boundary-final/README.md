# Post-source-boundary Nitro performance sweep

This directory compares Nitro after the source-boundary correctness fixes with
the pre-architecture baseline in `../20260723-pre-architecture-baseline`.

Measured commits:

- `97716adc` — Preserve non-retained source batch lifetimes
- `eb7388c3` — Admit physical integer and date vector widths

## Method

- JDK `/opt/java/openjdk`, version 26.0.1
- one unpinned JMH thread, `-Xmx12g`, transparent-huge-page request
- five one-second warmups, five one-second measurements, one fork for the full
  165-query sweep
- normalized JMH allocation plus instructions, cycles, L1 data-cache loads and
  misses, data-TLB loads and misses, branches, and branch misses
- three forks with the same warmup, measurement, allocation, and counter policy
  for 16 focused outlier recaptures
- suites and focused legs run serially
- no JFR recording

The broad sweep ran from 2026-07-28T00:48:50Z through 01:50:27Z. Focused
recaptures completed at 02:09:27Z. Minimum available memory observed by the
suite monitors was 30,282,732 KiB.

## Correctness result

The current revision produced all 165 expected measurements:

- TPCH: 22/22
- TPC-DS: 99/99
- ClickBench: 44/44

Every suite has zero JMH failure markers and a successful Maven exit. This
closes all 22 failures in the post-module-split sweep: six physical vector-width
binding failures and sixteen non-retained source-batch lifetime failures.

The full Maven suite also passed before measurement:

    1,457 tests, 0 failures, 0 errors, 566 skipped

## Broad comparison with the July 23 baseline

Ratios are current divided by baseline. Values above 1.0 are worse for all four
metrics.

| Suite | Rows | Duration gmean | Allocation gmean | Instructions gmean | Cycles gmean |
|---|---:|---:|---:|---:|---:|
| TPCH | 22 | 1.027 | 1.068 | 1.039 | 1.032 |
| TPC-DS | 99 | 1.024 | 1.028 | 1.034 | 1.045 |
| ClickBench | 44 | 1.037 | 1.029 | 1.050 | 1.043 |
| All | 165 | 1.028 | 1.033 | 1.039 | 1.043 |

TPC-DS q02 and ClickBench q07 dominate the aggregate. Excluding both, the
remaining 163 rows have geometric-mean ratios of 1.015 duration, 1.032
allocation, 1.025 instructions, and 1.030 cycles.

The 143 rows that were valid in the preceding post-module-split sweep now have
ratios of 1.021 duration, 1.035 allocation, 1.030 instructions, and 1.036
cycles. The 22 recovered rows have ratios of 1.075, 1.021, 1.096, and 1.089,
respectively.

## Focused three-fork recaptures

| Query | Duration | Allocation | Instructions | Cycles | Fork duration range |
|---|---:|---:|---:|---:|---:|
| TPCH q01 | 1.080 | 1.238 | 1.072 | 1.078 | 1944.6–1996.4 ms |
| TPCH q04 | 1.024 | 1.701 | 1.081 | 1.027 | 968.6–983.9 ms |
| TPCH q16 | 1.194 | 2.539 | 1.324 | 1.197 | 315.1–317.0 ms |
| TPCH q21 | 1.117 | 1.032 | 1.212 | 1.121 | 4335.2–4545.7 ms |
| TPC-DS q02 | 3.626 | 1.053 | 4.023 | 3.581 | 2132.3–2143.7 ms |
| TPC-DS q56 | 1.067 | 2.191 | 1.072 | 1.086 | 245.3–249.3 ms |
| TPC-DS q58 | 1.088 | 1.101 | 1.097 | 1.116 | 492.5–507.6 ms |
| TPC-DS q60 | 1.112 | 2.550 | 1.122 | 1.134 | 299.2–311.3 ms |
| TPC-DS q64 | 1.187 | 1.014 | 1.191 | 1.182 | 2304.7–2375.9 ms |
| TPC-DS q72 | 1.142 | 1.008 | 1.059 | 1.205 | 1933.4–2117.5 ms |
| TPC-DS q84 | 1.138 | 1.025 | 1.105 | 1.153 | 78.0–80.0 ms |
| ClickBench q07 | 2.387 | 1.224 | 2.399 | 2.384 | 190.5–192.8 ms |
| ClickBench q09 | 1.200 | 1.057 | 1.332 | 1.214 | 2173.3–2288.6 ms |
| ClickBench q10 | 1.190 | 1.044 | 1.244 | 1.207 | 2596.0–2779.5 ms |
| ClickBench q38 | 1.057 | 1.196 | 1.038 | 1.095 | 163.9–173.0 ms |
| ClickBench q39 | 1.029 | 1.195 | 1.028 | 1.056 | 340.8–347.5 ms |

ClickBench q39's broad-sweep instruction ratio of 1.307 did not reproduce:
the three-fork ratio is 1.028. The other large entries above are stable enough
to treat as real.

## Assessment

Correctness is restored, but the architecture cleanup is not performance
neutral relative to July 23. The largest existing regressions remain TPC-DS
q02 and ClickBench q07. TPCH q16, TPCH q21, TPC-DS q64/q72/q84, and ClickBench
q09/q10 are also confirmed instruction/cycle regressions.

The newly visible allocation cost deserves a separate causal A/B before join
cleanup. TPCH q04/q16 and TPC-DS q56/q60 allocate roughly 1.7–2.55 times the
baseline. This pattern is consistent with the conservative removal of
constrained re-borrow admission at the pull `BatchSource` boundary forcing
eager payload materialization, but this sweep establishes correlation, not
causation. The next performance slice should reverse-control that admission
behind a correct non-advancing/open-batch contract and recapture these four
queries before changing join policy.

## Artifacts

- broad results: `nitro-{tpch,tpcds,clickbench}.json`
- broad console logs: `nitro-{tpch,tpcds,clickbench}.log`
- broad memory logs: `nitro-{tpch,tpcds,clickbench}.memory.log`
- focused results and logs: `focused-*-outliers.{json,log}`
- launch state: `preflight-processes.txt`, `preflight-memory.txt`
