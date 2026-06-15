# TPC-DS sf10 — Velox vs Nitro interpreted operators, single-thread

All numbers are **single-thread** wall, apples-to-apples:

- **Velox**: `--num_drivers=1`, single-thread CPU = `ScanWall + ComputeWall` summed (Velox runs
  the per-bucket sub-pipelines concurrently even at one driver, so the parallel "Execution time"
  understates single-thread cost; the operator-wall sum is the fair single-thread figure).
- **Nitro operators**: `BenchmarkQueries.query<NN>` (the interpreted operator tree, *not* the
  compiled engine), JMH single-thread.
  - `df_off` = default operators.
  - `df_on` = `-Dnitro.skipScan -Dnitro.dynamicFilter` (Velox-style dynamic filtering in the
    operator pipeline).

Per Martin's rules: interpreted operators are the parity vehicle (Velox is itself an operator
engine); the compiled engine is the over-drive that beats Velox. Every result here is
byte-identical between `df_off` and `df_on` (validated via DumpResults across all 99 queries).

## Headline (geomean gap = Nitro / Velox, 95 shared queries)

| config | geomean gap |
|--------|-------------|
| df_off (baseline operators)              | **2.26×** |
| df_on  (single deployable config)        | **1.43×** |
| best-of per query                        | **1.39×** |

Dynamic filtering nearly halves the single-thread gap to Velox. With the reader-asymmetry fix
(below), `df_on` as a *single always-on config* (1.43×) is now within 3 % of the per-query best-of
(1.39×) — i.e. DF can be left on globally, like Velox, with no meaningful regressions. Several
queries match or beat Velox single-thread (q16, q28, q41, q90, q91).

### Reader-asymmetry fix (eliminates the DF-on regressions)

The earlier DF-on regressions were *not* the filter logic — they were the `SkipDecodeScanOperator`
owned-reader being ~1.6× slower than the shared Trino `ParquetReader` on the no-prune path. Two
causes, both fixed:

1. `setPreserveDictionary` was on for **every** column (needed only for predicate-over-dictionary on
   *filter* columns), so payload columns came back as `DictionaryBlock`s decoded per-value. Now
   preserved only on filter columns; payload columns flatten to `LongArrayBlock` like the shared
   reader.
2. The no-filter path did a per-value `getLong` into a temp array, then a second copy in `emit`. Now
   a `bridgeFull` bulk-copies the block's backing `long[]`/null[] straight into the output vector
   (matching `TrinoParquetScanOperator`).

Result: DF-on regressions >10 % vs df_off dropped from **22 queries → 6** (all small absolute:
q76 731→1132, q84 146→208, q91, q58, q6, q93). Single-config geomean 1.48× → **1.43×**.

## What dynamic filtering added in this round

1. **Type coverage** — `SkipDecodeScanOperator` extended from BIGINT-only to INT32 (q21
   `inv_quantity_on_hand`) and short DECIMAL (q40 etc.; TPC-DS money columns are
   `FIXED_LEN_BYTE_ARRAY(4) DECIMAL(7,2)`), with `isEligible` schema gating + `scannedTable`
   routing.
2. **Selectivity guard** — the scan bulk-decodes the non-filter columns when survivors exceed
   ~8 % of the batch (skip-decode loses to bulk SIMD above the crossover), keeping the DF scan no
   worse than an ordinary scan on weakly-selective filters.
3. **DF forwarding through `SemiJoinOperator`, `FilterOperator`, `ProjectOperator`** — a
   downstream join's filter now propagates through these operators to the probe-side scan
   (Project remaps pass-through `Input` columns). This is what unblocked the biggest offenders.

## Biggest wins (best gap, before → after dynamic filtering)

| q  | velox 1thr | df_off | df_on | gap before → after | what unblocked it |
|----|-----------|--------|-------|--------------------|-------------------|
| 98 |   184 | 2806 |  294 | 15.2× → 1.6× | DF (INT/decimal) |
| 21 |   473 | 6465 | 1289 | 13.7× → 2.7× | DF + INT32 |
| 39 |   636 | 8304 | 1674 | 13.1× → 2.6× | DF |
| 80 |   870 | 5028 |  862 |  5.8× → **1.0×** | Project/Filter/SemiJoin DF forwarding |
| 60 |   378 | 2166 |  468 |  5.7× → 1.2× | SemiJoin DF forwarding |
| 56 |   342 | 1855 |  420 |  5.4× → 1.2× | SemiJoin DF forwarding |
| 33 |   378 | 1786 |  393 |  4.7× → **1.0×** | SemiJoin DF forwarding |
| 66 |   288 | 1870 |  584 |  6.5× → 2.0× | DF |
| 40 |   148 | 1181 |  257 |  8.0× → 1.7× | DF + short decimal |
| 13 |  1197 | 4575 | 2596 |  3.8× → 2.2× | Filter DF forwarding |
| 49 |   378 | 1238 |  694 |  3.3× → 1.8× | DF forwarding |
| 64 |  3057 | 8751 | 4212 |  2.9× → 1.4× | DF |

## Remaining offenders (DF doesn't help → genuine)

- **q24 (3.6×), q59 (3.5×), q74 (3.4×)** — high-cardinality grouping (customer-level), build
  side too large to be a selective DF; the gap is the vectorized-aggregation constant factor
  (Velox's hash table), a separate effort.
- The DF-on owned-reader overhead is **fixed** (see the reader-asymmetry section): the 22
  regressions collapsed to 6 small ones (q76 +55 %, q84, q91, q58, q6, q93 — all small absolute),
  so DF-on is a safe global default at 1.43×.
- **q24/q59/q74 still ~3.4–3.6×** is the next frontier — high-cardinality grouping, the
  vectorized-aggregation constant factor (a separate effort from the scan/DF work here).
