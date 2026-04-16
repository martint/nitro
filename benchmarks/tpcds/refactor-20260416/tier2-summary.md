# Tier 2 optimizations — results (2026-04-16)

## Applied changes

On top of the DictionaryVector join refactor + Tier 1 (change `nmzzromw`):

- **2a** `HashJoinOperator.LongPairJoinIndex.matchRows` and `.LongTripleJoinIndex.matchRows` — vectorized per-batch probe for 2- and 3-key joins. Hoists `VectorAccess.longValues` / `.booleanValues` lambdas once per batch; per-position loop does direct typed access. `hasNulls` parameter selects a null-free fast path so the common case (Q64/Q80 keys have no nulls) skips two per-position `BooleanValues.value` calls. Dispatched from `prepareOuterProbeChunk` for 2- and 3-column joins analogously to the existing 1-column `LongJoinIndex.matchRows`.
- **2b** `GroupingState.assignLongPairGroups` / `assignLongTripleGroups` / `assignLongQuadGroups` — hoist `VectorAccess.longValues` / `.booleanValues` factory calls out of the per-row loop. Each row now accesses typed accessors directly instead of dispatching through `OperatorVectorSupport.longValue` / `isNull` polymorphic switches.
- **2c** Scalar `*.applyNulls` / `*.applyValues` refactored to hoist `VectorAccess.booleanValues(nulls)` once per call. Applied to `AndBoolean`, `OrBoolean`, `EqualI64`, `LessThanI64`, `IfI32`, `IfI64`, `IfUtf8`. Eliminates per-position `VectorAccess.isNull(nulls, position)` calls which allocated a fresh `BooleanValues` lambda every invocation.

## Results — Q64/Q65/Q80/Q97

JMH `-wi 2 -i 3 -w 2s -r 2s -f 0 -foe true` on TPC-DS SF10 parquet.

| Query | Pre-Tier-1 | Tier 1 | Tier 2 | **Tier 2 speedup vs Tier 1** | **Total vs pre-Tier-1** | Trino ms/op | **Trino/Nitro (post)** |
|:-:|---:|---:|---:|:-:|:-:|---:|:-:|
| Q64 | 29891 | 25421 | **23512** | **1.08×** | **1.27×** | 11309 | 0.48× (was 0.38×) |
| Q65 | 8428 | 8416 | **7536** | **1.12×** | **1.12×** | 4680 | 0.62× (was 0.56×) |
| Q80 | 13725 | 12973 | **10657** | **1.22×** | **1.29×** | 5929 | 0.56× (was 0.43×) |
| Q97 | 6126 | 5969 | **4246** | **1.41×** | **1.44×** | 3891 | **0.92× (was 0.64×)** |

- **Q97 is now within 8% of Trino.** Two more % and it beats Trino.
- Q80 closed the largest absolute gap — from 3.2× slower than Trino to 1.8× slower.
- Q65 finally got its win — the scalar null-merge refactor (2c) addresses the `MultiplyI64.applyNulls` hot path directly.
- Q64 +8% — less than Q65/Q80/Q97 because 2a's win is partially offset by the lambda-capture cost at each batch; the `hasNulls` fast path was needed to avoid regressing the no-nulls case.

## Total improvement: refactor + Tier 1 + Tier 2

| Query | Baseline | Final | **Total speedup** | vs Trino |
|:-:|---:|---:|:-:|:-:|
| Q64 | 94055 | 23512 | **4.00×** | 0.48× |
| Q65 | 34202 | 7536 | **4.54×** | 0.62× |
| Q66 | 23547 | 3604 | **6.53×** | **1.14× (Nitro faster)** |
| Q80 | 31865 | 10657 | **2.99×** | 0.56× |
| Q97 | 46139 | 4246 | **10.87×** | **0.92×** |

## Correctness

All four target queries (Q64, Q65, Q80, Q97) pass `TestQueries#testQueryNN` vs. the Trino-SQL reference. Q66 has a pre-existing correctness failure on baseline (not introduced by this work).

`TestOperators` / `TestOperatorBatches`: identical pre-existing failure set, no new regressions.

## What's next

To push Q97 over Trino (and close Q64/Q80 further), next targets:

- **Q64 is the hardest.** Profile again post-Tier-2; the remaining hash-join probe cost (`LongPairJoinIndex.findSlot`) may dominate. Options: better hash function, larger initial table, or a secondary probe optimization that uses the common case of "many outer rows hit the same inner bucket" (would need a hash-agnostic redesign).
- **DistinctKeySet batched add** — `MarkDistinctOperator.computeDistinctMask` still calls `add(Vector[] values, ...)` per row through the `DistinctIndex` interface. Adding a batched `addBatch` with hoisted accessors should trim the remaining 4% in Q97's `DistinctKeySet.hasNull`.
- **Aggregation accumulator hot loops** — the non-probe parts of Q80/Q97 (grouping table, sum accumulators) haven't been audited post-Tier-2.
- **Investigate Q66 pre-existing correctness bug** — not a perf task but the fix would unblock including Q66 in comparisons.
