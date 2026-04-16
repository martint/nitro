# Tier 1 optimizations — results (2026-04-16)

## Applied changes

On top of the DictionaryVector join refactor (change `nmzzromw`):

- **1b** `HashJoinOperator` — replaced `currentMatches.isEmpty()` / `.size()` calls with a cached `int currentMatchCount` field (devirtualizes fastutil `LongList` polymorphic dispatch in the probe loop).
- **1c** `HashJoinOperator` — cached `outputCount` / `outerOutputCount` / `innerOutputCount` in final fields at construction; removed recursive chain of `outer.outputCount() + inner.outputCount()` on every call.
- **1d** `DictionaryVector.wrap(...)` — skip `validateIds` on the hot-path base case (callers supply bounds-valid ids). Composed-id path already skips.
- **1e** `Allocator.trackVector` — skip `IdentityHashMap.add` for vectors with `poolFamily() == null` (e.g. `DictionaryVector` wrapping borrowed data); retained-bytes tracking preserved.
- **1f** `HashJoinOperator.materializeInnerOutput` — hoist `outputSchema(...)` lookup out of the per-null-row loop; lazy-init once when the first NO_MATCH row is seen.
- **1a** `VectorAccess` + `RleVector.runIndexFromHint(position, hint)` — monotonic-cursor run lookup. `VectorAccess.longValues/booleanValues/doubleValues/binaryValues` lambdas thread a mutable `int[] hint` through consecutive calls; backward jumps fall back to binary search.

## Results — Q64/Q65/Q80/Q97

JMH `-wi 2 -i 2 -w 2s -r 2s -f 0 -foe true` on TPC-DS SF10 parquet.

| Query | Pre-Tier-1 ms/op | Post-Tier-1 ms/op | **Tier 1 speedup** | Trino ms/op | **Trino/Nitro (post)** |
|:-:|---:|---:|:-:|---:|:-:|
| Q64 | 29890.779 | 25421.370 | **1.18×** | 11309 | 0.45× (was 0.38×) |
| Q80 | 13724.505 | 12973.396 | **1.06×** | 5929 | 0.46× (was 0.43×) |
| Q97 | 6125.888 | 5969.383 | **1.03×** | 3891 | 0.65× (was 0.64×) |
| Q65 | 8427.772 | 8416.183 | 1.00× | 4680 | 0.56× (unchanged) |

Q64 gets the biggest win — its profile was dominated by `HashJoinOperator.prepareOuterProbeChunk` self-time, `AbstractCollection.isEmpty`, `outputCount` recursion, and `DictionaryVector.validateIds`, all addressed by Tier 1. Q80 gets a smaller Q64-style gain. Q65's hotspots are scalar arithmetic (`MultiplyI64.applyNulls`), not the join machinery — Tier 1 barely touches them, so ~flat is expected. The RLE cursor (1a) is in the codepath for `MultiplyI64.applyNulls` but the benefit there is modest at 2-iteration measurement noise.

## Correctness

All four target queries (Q64, Q65, Q80, Q97) pass `TestQueries#testQueryNN` vs. the Trino-SQL reference. Q66 has a pre-existing correctness failure on baseline (not introduced by this work).

`TestOperators` / `TestOperatorBatches`: identical pre-existing failure set, no new regressions.

## Full speedup vs Trino baseline after refactor + Tier 1

| Query | Baseline Nitro | After refactor | After Tier 1 | **Total speedup** | Trino | vs Trino |
|:-:|---:|---:|---:|:-:|---:|:-:|
| Q64 | 94055 | 29891 | 25421 | **3.70×** | 11309 | 0.45× |
| Q65 | 34202 | 8428 | 8416 | **4.06×** | 4680 | 0.56× |
| Q66 | 23547 | 3604 | — | ≥ 6.53× | 4092 | **>1.1× (Nitro faster)** |
| Q80 | 31865 | 13725 | 12973 | **2.46×** | 5929 | 0.46× |
| Q97 | 46139 | 6126 | 5969 | **7.73×** | 3891 | 0.65× |

Q64 remains the biggest absolute gap to Trino (0.45× = Nitro still 2.24× slower than Trino). The next biggest wins will come from Tier 2 — particularly **2a: vectorized composite probe**, which should directly attack Q64's remaining hash-join probe self-time.

## Next (Tier 2)

- 2a: vectorize multi-column probe (`LongPairJoinIndex`, `LongTripleJoinIndex`, `ObjectJoinIndex` fallbacks) — biggest expected Q64 gain
- 2b: hoist null-check dispatch out of `GroupingState` / `DistinctKeySet` loops
- 2c: scalar function null-merge refactor (dispatch once per encoding)
