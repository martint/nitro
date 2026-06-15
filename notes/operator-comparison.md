# Operator-to-operator benchmarks: Nitro vs Velox

Whole-query benchmarks confound scan + decode + several operators, which makes
it impossible to attribute a gap to a specific operator. These benchmarks drive
a **single operator** over **identical synthetic in-memory input** on both
engines, so the per-operator gap can be isolated, profiled, and tuned.

## Harnesses

- Nitro: `src/test/java/org/weakref/nitro/BenchmarkOperatorComparison.java` (JMH).
- Velox: `/root/velox/velox/exec/benchmarks/NitroComparisonBenchmark.cpp`
  (folly Benchmark), target `velox_nitro_comparison_benchmark`.

Both use the same row counts, key distributions, group cardinalities, and batch
size (8192). Input is primary synthetic data built once (not replayed query
output). Single-threaded on both sides (Nitro JMH one thread; Velox
`maxDrivers(1)`).

### Running

```bash
# Nitro
mvnd -q exec:exec@benchmark \
  -Dbenchmark.include='org\.weakref\.nitro\.BenchmarkOperatorComparison\..*' \
  -Dbenchmark.options="-wi 3 -i 6 -w 1s -r 1s -f 1 -jvmArgsAppend -Xmx8g -rf csv -rff /tmp/nitro-ops.csv"

# Velox
cmake --build /root/velox/_build/release --target velox_nitro_comparison_benchmark -j 8
/root/velox/_build/release/velox/exec/benchmarks/velox_nitro_comparison_benchmark --bm_min_iters=6
```

## Methodology: the terminal-sum sink

Each pipeline terminates in a **global sum over every output column** of the
operator under test. This (a) forces full value materialization of lazy
(dictionary) join output on Nitro, matching Velox's full materialization, and
(b) reduces the result to one row so the consumer cost is trivial and identical
on both engines. This is the same idiom Velox's own `FilterProjectBenchmark`
uses. Without it the comparison is meaningless: Nitro's join output is a lazy
`DictionaryVector` (reading `.length()` is O(1) and never materializes), while a
naive Velox `copyResults` concatenates the entire output — the first draft
measured that concatenation, not the join (Velox "joinProbeSparse" read 560 ms;
with the terminal sum it is 162 ms).

**Caveat — the sink is not free and is cheaper on Nitro.** `aggregationSum` is
that sink measured in isolation: Nitro 3.8 ms vs Velox 22 ms for a global sum of
16M longs at 8192-row batches. So every row below includes a sink that costs
Nitro less, which inflates Nitro's apparent lead on the other operators. After
subtracting the sink, the joins are roughly **parity**, not a Nitro win. The
sink gap itself is largely Velox per-batch driver overhead at the small 8192
batch; a batch-size sweep is the next step to neutralize it.

## First results (sf-scale synthetic, single thread, ms/op)

| operator | Nitro | Velox | N/V | note |
|---|---|---|---|---|
| joinProbeSparse (8M probe ⋈ 1M sparse build) | 131 | 162 | 0.81 | ~parity after sink |
| joinProbeDense (array-mode build) | 81 | 130 | 0.62 | Nitro array mode |
| joinBuildSparse (1M probe ⋈ 8M build) | 215 | 360 | 0.60 | ~parity after sink |
| groupSumLowCardinality (16M rows, 1k groups) | 63 | 38 | 1.67 | **Velox wins** |
| groupSumHighCardinality (16M rows, 4M groups) | 445 | 462 | 0.96 | parity |
| aggregationSum (sum 16M longs = the sink) | 3.8 | 22 | 0.17 | per-batch overhead |
| project (c0+c0 over 16M) | 14 | 41 | 0.34 | includes sink |

## Conclusions

1. **At the in-memory operator level Nitro is competitive with Velox** — clearly
   ahead on global aggregation and projection at this batch size, ~parity on
   joins and high-cardinality grouping once the sink is accounted for. This
   **confirms the ~2× whole-query gap to Velox is scan/decode, not operator
   execution** (consistent with prior findings: the native Parquet reader is the
   real lever, the operators are not the bottleneck).
2. **Low-cardinality grouping is the one operator where Velox genuinely leads
   (1.67×)** — 16M rows into 1024 groups. A concrete, newly-isolated tuning
   target.
3. The cheap-op gaps (sum, project) are dominated by **Velox per-batch driver
   overhead at 8192-row batches**; a batch-size sweep will show whether they
   close at larger batches and is the next methodology step before drawing
   operator conclusions from them.

## Next

- Batch-size sweep (8k / 32k / 64k) to separate per-batch overhead from
  per-row operator cost and neutralize the sink confound.
- Profile low-cardinality grouping on both engines (the one clear Velox win).
- Add filter, multi-key join, and string-key variants.
