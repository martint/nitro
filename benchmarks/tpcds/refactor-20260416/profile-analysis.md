# Profile analysis: Q64, Q65, Q80, Q97 (2026-04-16)

JMH `-prof stack` with `-wi 2 -i 3 -w 2s -r 3s`. ~50% of RUNNABLE samples unresolved by the stack profiler (native/JIT); numbers below are from the resolved half.

## Top hotspots by query

### Q64 — primary target (now 29.9s, Trino 11.3s, 2.64× behind)
| % | Stack (top of call tree) | Category |
|---:|---|---|
| 11.0% | `HashJoinOperator.prepareOuterProbeChunk` (self) | probe |
| 8.0% | `LongPairJoinIndex.findSlot` → `matchesNoNulls` → `matches` → `matchesForOuterPosition` | **probe row-at-a-time** |
| 4.3% | `prepareOuterProbeChunk` (again, via ProjectOperator → GroupedAggregation) | probe |
| 3.7% | `LongJoinIndex.matchDictionaryRows` | probe (dictionary outer key) |
| 2.2% | `TrinoParquetScanOperator.copyNulls` lambda | parquet IO |
| 2.2% | `java.util.AbstractCollection.isEmpty` inside `produceBatch` | **abstract-list isEmpty** |
| 1.3% | `LongJoinIndex.findSlot` | probe |
| 1.2% | `HashJoinOperator.wrapComposedDictionary` | new outer output |
| 1.0% | `DictionaryVector.validateIds` | **validation in hot path** |
| 0.4% | `HashJoinOperator.outputCount` recursion (10 frames deep) | **chained outputCount** |
| 0.3% | `Allocator.adopt` → `IdentityHashMap.put/hash` | **allocation tracking** |

### Q65 — primary target (now 8.4s, Trino 4.7s, 1.80× behind)
Dominated by scalar arithmetic, not join.
| % | Stack | Category |
|---:|---|---|
| 6.7% | `Arrays.binarySearch` ← `RleVector.runIndex` ← `VectorAccess.booleanValues` lambda ← `MultiplyI64.applyNulls` | **RLE null-check per row** |
| 5.2% | `I64BinaryDispatch.forEachFlatRle` ← `MultiplyI64.apply` | scalar kernel |
| 3.8% | `MultiplyI64.applyNulls` | null merge |
| 3.0% + 2.9% | `GroupingState.assignLongPairGroups` | grouping |
| 1.8% | `GroupingState.LongPairGroupingTable.assignGroup` | grouping |
| 1.8% | `OperatorVectorSupport.longValue` from grouping | per-position read |
| 1.7% | `Arrays.fill` ← `I64Vector.clearForReuse` ← `Allocator.allocatePooled` | **allocator pool zeroing** |

### Q80 — primary target (now 13.7s, Trino 5.9s, 2.32× behind)
Join-dominated like Q64.
| % | Stack | Category |
|---:|---|---|
| 19.2% | `HashJoinOperator.prepareOuterProbeChunk` (self) | **probe self-time is huge** |
| 4.6% | `LongPairJoinIndex.findSlot` via `matchesForOuterPosition` | probe |
| 2.3% | `AbstractCollection.isEmpty` in `produceBatch` | **abstract isEmpty** |
| 2.1% | `TrinoParquetScanOperator.copyNulls` lambda | parquet |
| 1.9% | `HashJoinOperator.materializeInnerOutput` | inner output |
| 1.8% | `AbstractCollection.isEmpty` (second path) | abstract isEmpty |
| 1.5% | `LongJoinIndex.matchDictionaryRows` | probe |
| 1.1% | `HashJoinOperator.outputSchema` ← `copyNullInnerPosition` | **schema lookup per null row** |
| 0.9% | `DictionaryVector.validateIds` | validation |
| 0.8% + 0.5% | `Arrays.binarySearch` ← `RleVector.runIndex` ← `IfI64.applyValues` | **RLE null-check per row** |

### Q97 — primary target (now 6.1s, Trino 3.9s, 1.57× behind)
Grouping + distinct + evaluator heavy.
| % | Stack | Category |
|---:|---|---|
| 6.5% | `GroupingState.assignLongPairGroups` | grouping |
| 4.1% | `OperatorVectorSupport.booleanValue` ← `isNull` ← `DistinctKeySet.hasNull` ← `MarkDistinct` | **per-row null check in MarkDistinct** |
| 3.4% | `AbstractCollection.isEmpty` in `produceBatch` | abstract isEmpty |
| 3.0% | `LessThanI64.applyNulls` | scalar null merge |
| 2.5% | `LongPairGroupingTable.assignGroup` | grouping |
| 2.3% | `LongJoinIndex.matchLongRows` | probe |
| 2.2% | `IfI64.constantNullValue` ← `HashMap.computeIfAbsent` | **computeIfAbsent on hot path** |
| 2.2% | `OperatorVectorSupport.booleanValue` ← `isNull` ← `GroupingState.isNull` | per-row null check in grouping |
| 1.7% + 1.6% | `AndBoolean.applyNulls` / `applyValues` | scalar null merge |
| 1.3% | `GroupingState.LongPairGroupingTable.rehash` | hash table grow |

## Cross-query pattern summary

Five themes dominate the RUNNABLE samples we can resolve:

1. **Hash-join probe (findSlot / prepareOuterProbeChunk)** — 11–19% in Q64/Q80. Per-position row-at-a-time work.
2. **Per-row null checks through `VectorAccess.booleanValues` lambdas on RLE** — binary search on `runEnds` every position, ~5–7% in Q65/Q80.
3. **`AbstractCollection.isEmpty` on `currentMatches`** — 1.5–3.4% across all four queries; abstract-list dispatch can't be devirtualized.
4. **Grouping table (`assignLongPairGroups` + `LongPairGroupingTable.assignGroup`)** — 4–10% in Q65/Q97; hash probing + null checks.
5. **Bookkeeping overhead**: `Allocator.adopt` (IdentityHashMap), `DictionaryVector.validateIds`, `HashJoinOperator.outputCount` recursion, `HashJoinOperator.outputSchema` per null row — 1–3% cumulatively.

## Prioritized fix plan

### Tier 1 — cheap structural wins (aim for 10–30% improvement across the four targets)

**1a. Cursor-based `RleVector` traversal in `VectorAccess.booleanValues` / `longValues` / `doubleValues` / `binaryValues` lambdas.**
- Today: `position -> runValues.value(values.runIndex(position))` does a binary search on `runEnds` for every position.
- Fix: when positions are accessed monotonically (mask iteration), maintain a run cursor that advances forward. Amortized O(1) per position instead of O(log runs).
- Files: [`src/main/java/org/weakref/nitro/function/scalar/builtin/VectorAccess.java`](src/main/java/org/weakref/nitro/function/scalar/builtin/VectorAccess.java) (4 lambda factories); [`RleVector.java:83-97`](src/main/java/org/weakref/nitro/data/RleVector.java) `runIndex` — expose a stateful "run cursor" API.
- Expected win: Q65 ≥5% wall-clock, Q80 ≥1%.

**1b. Devirtualize `currentMatches.isEmpty()` in `HashJoinOperator.produceBatch`.**
- Today: `currentMatches` typed as `LongList` (fastutil interface), `isEmpty()` goes through `AbstractCollection.isEmpty` which computes `size() == 0` via a polymorphic call. JIT can't inline the common case.
- Fix: store a `boolean currentMatchesEmpty` flag updated when `currentMatches` is assigned; check the flag in the hot loop.
- Files: [`HashJoinOperator.java`](src/main/java/org/weakref/nitro/operator/HashJoinOperator.java).
- Expected win: 1–3% on all four queries.

**1c. Cache `HashJoinOperator.outputCount` in a field.**
- Today: recursive `outer.outputCount() + inner.outputCount()` called many times; chained joins cost O(depth).
- Fix: compute once in the constructor, store in `private final int outputCount`; return directly.
- Files: [`HashJoinOperator.java`](src/main/java/org/weakref/nitro/operator/HashJoinOperator.java).
- Expected win: 0.3–1% on multi-join plans.

**1d. Skip `DictionaryVector.validateIds` when constructing from join internals.**
- Today: `DictionaryVector` ctor walks the entire `ids[]` array to check bounds. Our join knows its ids are valid.
- Fix: add an internal `unsafeDictionary(ids, values)` factory that bypasses `validateIds`, or make the existing private `(ids, values, copyIds)` ctor also skip validation when called from trusted paths.
- Files: [`DictionaryVector.java:154-159`](src/main/java/org/weakref/nitro/data/DictionaryVector.java), `wrapComposedDictionary` call sites.
- Expected win: Q64 ~1%, Q80 ~1%.

**1e. Bypass `Allocator.adopt` IdentityHashMap tracking on DictionaryVector wrap.**
- Today: each outer-column `adopt` does `IdentityHashMap.put` through the tracking set.
- Fix: for DictionaryVector wrapping a borrowed source, skip adopt (we're not owning new memory) or use a cheaper tracking mechanism.
- Files: [`HashJoinOperator.buildOuterDictionaryStream`](src/main/java/org/weakref/nitro/operator/HashJoinOperator.java), `Allocator.adopt`.
- Expected win: 0.3–1% on join-heavy queries.

**1f. Cache `HashJoinOperator.outputSchema` result per outer-output index during `copyNullInnerPosition`.**
- Today: each null inner row calls `outputSchema(outputIndex)` which rebuilds the schema via `borrow`. Q80 shows 1.1% in this path.
- Fix: compute null schema once per build and reuse.
- Expected win: Q80 ~1%.

### Tier 2 — structural changes (aim for additional 20–50% on join-dominated queries)

**2a. Vectorize multi-column probe (`LongPairJoinIndex`, `LongTripleJoinIndex`, `ObjectJoinIndex` fallbacks).**
- Today: multi-column joins route through `matchesForOuterPosition` — a per-position call returning a `LongList`. 8% of Q64 lives in this path.
- Fix: hash all key columns into a single long (or pack into a row-layout buffer) so `LongJoinIndex.matchRows` vectorized path extends to composite keys. This was the Velox lesson and the original audit called it out.
- Files: [`HashJoinOperator.java:1510-1790`](src/main/java/org/weakref/nitro/operator/HashJoinOperator.java) probe dispatch; add `matchCompositeRows` analogous to `matchRleRows`.
- Expected win: Q64 ≥5%, Q80 ≥5%.

**2b. Grouping-table null-check hoisting.**
- Today: `GroupingState.assignLongPairGroups` calls `OperatorVectorSupport.isNull(nulls, position)` per row in the assign loop. `isNull` dispatches through a polymorphic `Vector` switch each call.
- Fix: dispatch on the null vector encoding once before the loop, then run a monomorphic inner loop. For all-false nulls (common after a filter), skip the check entirely.
- Files: [`GroupingState.java`](src/main/java/org/weakref/nitro/operator/GroupingState.java), `DistinctKeySet.java`.
- Expected win: Q65 ≥2%, Q97 ≥3%.

**2c. Scalar function null-merge refactor.**
- Today: functions like `MultiplyI64.applyNulls`, `LessThanI64.applyNulls`, `AndBoolean.applyNulls` iterate per-position doing `or`-style merges via `VectorAccess.booleanValues(…).value(position)`. 3–5% cumulative in Q65/Q97.
- Fix: dispatch on null encoding once, then merge via vectorized bitwise ops for Flat×Flat, or fused RLE×Flat with cursor.
- Files: all `*I64.applyNulls` / `AndBoolean.applyNulls` etc.
- Expected win: Q65 ≥3%, Q97 ≥5%.

### Tier 3 — further-out (benefit ≥5% but more disruptive)

**3a. Eliminate `HashMap.computeIfAbsent` in `IfI64` / `ProjectOperator.evaluateOutput` per-call path** (2.2% in Q97).

**3b. Allocator pooled-vector clearing**: `Arrays.fill` on `I64Vector.clearForReuse` (1.7% Q65) — can often be skipped if the caller will overwrite every position anyway.

**3c. Investigate the 50% "stack is empty" samples** by rerunning with JFR (`-prof jfr`) or adding `-XX:-OmitStackTraceInFastThrow` plus `-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints` so the stack profiler can resolve JIT-compiled frames.

## Suggested implementation order

1. Tier 1b + 1c + 1d + 1e + 1f first — all small, mechanical, under a day total.
2. Tier 1a (RLE cursor) — medium effort; biggest per-fix gain on Q65/Q80.
3. Rerun benchmark — expect ~5–15% wall-clock on the four queries.
4. Tier 2a (vectorized composite probe) — biggest single structural win for Q64/Q80.
5. Tier 2b + 2c (null-check hoisting) — biggest structural win for Q65/Q97.
6. Rerun benchmark — expect another 10–30% cumulative improvement.

Don't do all of Tier 2 before measuring — the interactions (e.g. 2b may reduce `booleanValue` traffic enough to make 1a less meaningful) matter.
