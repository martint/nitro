# TPC-H q16 native join chain

Trino commit `76a1c188` composes the q16 source fragment as one native chain:

```text
Nitro Parquet scan -> hash join -> semi join/filter/project -> partial DISTINCT aggregation
```

The hash join and semi join retain Nitro batches in the scan allocator and stream into the
aggregation terminal. This removes the intermediate Nitro-to-Page-to-Nitro boundary that flattened
the join's dictionary-backed string outputs before composite grouping.

## Correctness

The SF10 Trino-versus-Nitro audit returned 27,840 rows with identical results and types. All 10 scan
attempts used native Nitro sources, with zero source rejections.

The local execution planner suite passed 37/37 tests, including a new join -> semi join -> grouped
aggregation coverage case. The normal Trino package, license, import, and style checks also passed.

## Duration and CPU

Nitro-only, one JVM, two warmups, five measurements:

| implementation | wall p50 (ms) | CPU p50 (ms) |
|---|---:|---:|
| Nitro before native composition | 474.498 | 948 |
| Nitro native chain | 429.956 | 863 |
| Trino recorded adjacent baseline | 485.203 | 710 |

Native composition reduced Nitro q16 wall time by 9.4% and CPU time by 9.0%. Nitro is now 11.4%
faster than the recorded Trino wall-time median, but still consumes 21.5% more CPU.

The corrected standalone SQL-topology harness measured Nitro at 701.748 ms versus Trino operators
at 731.556 ms (0.959x), with an instruction ratio of 0.937x. The integrated SQL engine's remaining
CPU ratio (1.215x) is therefore still materially inconsistent with the operator-level result. The
next investigation should attribute the residual CPU after confirming the partial composite
DISTINCT aggregation now receives dictionary-backed vectors.

Raw execution logs were intentionally left in `/tmp` rather than committed:

- `/tmp/tpch-q16-native-chain-audit.log`
- `/tmp/tpch-q16-native-chain-performance.log`
- `/tmp/native-chain-planner-suite.log`
- `/tmp/native-chain-package-check.log`

No JFR or heap-dump artifact was created.
