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
at 731.556 ms (0.959x), with an instruction ratio of 0.937x. Subsequent per-plan-node attribution
showed that the standalone Trino exchange model concealed the integrated engine's advantage in the
two downstream aggregation stages: the Nitro source-side native chain was at parity with Trino's
corresponding scan/join/filter/partial-aggregation operators, while final DISTINCT and partial count
accounted for essentially the complete residual CPU difference.

## Normalized composite width correction

The final DISTINCT consumes a full-width 128-bit normalized key. Its first batch is dictionary-backed,
but later flat binary batches pay query-stable interning without reducing the key footprint. A temporary
arity diagnostic established that retaining normalized keys for the compact 96-bit aggregate while using
the ordinary generated/hash path for the full-width aggregate removes this cost. The accepted implementation
expresses that decision as `FlatKeyTablePolicy.Layout.normalizedIntKeyMaxBits`; it contains no query, column, or
arity identity.

Synchronized two-warmup/twelve-measurement runs after the correction:

| implementation | wall p50 (ms) | CPU p50 (ms) |
|---|---:|---:|
| Nitro | 413.411 | 831 |
| Trino | 341.479 | 702 |

Nitro remains 1.211x on wall and 1.184x on CPU for q16, but its whole-process hardware-counter ratios
improved materially relative to the pre-correction capture:

| counter | before Nitro/Trino | after Nitro/Trino |
|---|---:|---:|
| instructions | 1.352 | 1.090 |
| cycles | 1.183 | 1.031 |
| branches | 1.382 | 1.108 |
| branch misses | 1.108 | 1.047 |
| cache misses | 1.072 | 1.014 |

The fresh 22-query TPC-H gate has a 0.726x CPU geomean and 0.754x total CPU ratio, improving on the
accepted board's 0.737x and 0.788x. q16 remains the principal TPC-H CPU regression. The complete Nitro
suite passed 1,615 tests with 566 skipped.

Raw execution logs were intentionally left in `/tmp` rather than committed:

- `/tmp/tpch-q16-native-chain-audit.log`
- `/tmp/tpch-q16-native-chain-performance.log`
- `/tmp/tpch-q16-nitro-width-counters.{log,csv}`
- `/tmp/tpch-q16-trino-width-counters.{log,csv}`
- `/tmp/tpch-full-normalized-width-{nitro,trino}.log`
- `/tmp/tpch-q18-q22-normalized-width-nitro.log`
- `/tmp/native-chain-planner-suite.log`
- `/tmp/native-chain-package-check.log`

No JFR or heap-dump artifact was created.
