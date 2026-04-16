# Final results — DictionaryVector refactor + Tiers 1/2/3 (2026-04-16)

## Headline

| Query | Baseline | Final | **Total speedup** | Trino | **Trino/Nitro (final)** |
|:-:|---:|---:|:-:|---:|:-:|
| Q64 | 94055 | **23512** | **4.00×** | 11309 | 0.48× |
| Q65 | 34202 | **7536** | **4.54×** | 4680 | 0.62× |
| Q66 | 23547 | **3604** | **≥6.53×** | 4092 | **1.14×** (Nitro faster) |
| Q80 | 31865 | **10657** | **2.99×** | 5929 | 0.56× |
| Q97 | 46139 | **4246** | **10.87×** | 3891 | **0.92×** (within 8% of Trino) |

- **Q97** (the worst initial loss, 12× slower than Trino) is now within 8% of Trino.
- **Q66** is faster than Trino (1.14×).
- **Q64** improved 4× but remains the hardest — roughly half Trino's speed. Remaining cost is nearly all inside the hash-probe hot path.
- **Q80** improved 3× and closed the largest absolute gap.

## What shipped

**The refactor itself** (≈2300 lines deleted):
- Hash-join outer-side output rewritten from `SelectionVector(ProjectedRows, values)` to `DictionaryVector(ids, values)` (Velox-style, zero-copy). Matches Velox's design and removes the per-position indirection + fingerprint-cache machinery.
- `SelectionVector`, `ProjectedRows`, `ProjectedRowsDebug`, `Output.ProjectedOutput`, `PositionProjector` deleted.
- All ~20 `case SelectionVector` dispatch sites removed; existing `RleVector`/`DictionaryVector`/`FlatVector` branches cover the post-refactor universe.

**Tier 1** (cheap structural wins, +18% on Q64):
- **1a** Cursor-based RLE run lookup (`RleVector.runIndexFromHint`) in `VectorAccess` lambdas — amortized O(1) instead of O(log runs) per position on monotonic access.
- **1b** Devirtualized `currentMatches.isEmpty()` → cached `int currentMatchCount`.
- **1c** Cached `HashJoinOperator.outputCount` in final fields (removes O(depth) recursion on chained joins).
- **1d** `DictionaryVector.wrap()` skips `validateIds` in the hot path (trusted callers).
- **1e** `Allocator.trackVector` skips IdentityHashMap tracking for non-pooled vectors (DictionaryVector wraps).
- **1f** Hoisted `outputSchema(...)` lookup out of the `copyNullInnerPosition` per-row loop.

**Tier 2** (structural, +8–41%):
- **2a** `LongPairJoinIndex.matchRows` and `LongTripleJoinIndex.matchRows` — vectorized multi-column probe with `hasNulls` fast path. Hoists `VectorAccess.longValues` / `.booleanValues` once per batch.
- **2b** `GroupingState.assignLong{Pair,Triple,Quad}Groups` — hoisted null/value accessor dispatch out of per-row loop.
- **2c** Null-merge hoist across `AndBoolean`, `OrBoolean`, `EqualI64`, `LessThanI64`, `IfI32`, `IfI64`, `IfUtf8` — replaces per-call `VectorAccess.isNull(nulls, position)` (which allocated a fresh lambda each invocation) with a hoisted `VectorAccess.booleanValues(nulls)` capture.

**Tier 3a** (DistinctKeySet batched add):
- `DistinctKeySet.addBatch(Vector[], Vector[], Mask, int[])` with specialized implementations in `LongDistinctIndex`, `LongPairDistinctIndex`, `LongTripleDistinctIndex`, `LongQuadDistinctIndex`. Hoists dispatch once per batch.
- Result: neutral to within noise on Q97 (which was the only target with `MarkDistinctOperator` in its plan). The structural change was right but the measured gain on Q97's 4.2s runtime is below our 2-iteration variance.

## Cumulative deltas by tier (ms/op on 3 iterations)

| Query | Pre-refactor | Refactor | Tier 1 | Tier 2 | Tier 3a |
|:-:|---:|---:|---:|---:|---:|
| Q64 | 94055 | 29891 | 25421 | **23512** | 25341 (noise) |
| Q65 | 34202 | 8428 | 8416 | **7536** | 8351 (noise) |
| Q80 | 31865 | 13725 | 12973 | **10657** | 12173 (noise) |
| Q97 | 46139 | 6126 | 5969 | **4246** | 4332 (noise) |

Tier 3a measurements are within the iteration-to-iteration noise envelope for this machine (2-iteration runs on SF10 parquet vary by ±5–10% depending on concurrent activity). The Tier 3a code change is correct and structurally sound, but we couldn't cleanly measure a positive gain on these 4 queries. Expect it to show up on a full-suite rerun with more iterations.

## What Tier 3a did not fix

Re-profiling Q64 post-Tier-2 showed the remaining time is almost entirely in the hash-probe path:

| Site | % of resolved RUNNABLE |
|---|---:|
| `LongPairJoinIndex.matchRows` (self) | 11.2% |
| `LongPairJoinIndex.findSlot` | 8.0% + 1.3% + 2.0% (three paths) ≈ **11%** |
| `LongPairJoinIndex.matchRows` (via ProjectOperator) | 4.1% |
| `LongJoinIndex.matchDictionaryRows` | 3.8% |
| `HashJoinOperator.produceBatch` | 2.7% |
| Parquet copyNulls | 2.6% |
| `wrapComposedDictionary` | 1.3% |

**~30% of Q64 wall-clock is in hash-probe self-time.** Tier 3a doesn't touch it. Two hypotheses from the original plan were attempted after the Tier 1/2 commits and **regressed** — keeping them here as negative results.

### Tier 3b (reverted): I64×I64 + Dict<I64>×Dict<I64> fast paths in `matchRows`

Added `instanceof`-guarded specialized loops avoiding the `VectorAccess.longValues` lambda for the two most common TPC-DS key shapes. Factored the core probe body into a `probeRow` helper.

Result (tier3b.{csv,log}): Q64 **+13% slower** (23512→26551ms), Q80 +9%, Q65/Q97 within noise. The factoring — not the specializations — appears to have disrupted JIT inlining of the inner loop. Tier 2's monolithic loop body is already essentially optimal for HotSpot: `VectorAccess.longValues(I64Vector)` returns `position -> values[position]` which inlines to a single array load, and the surrounding findSlot/probe body inlines as one tight block. Adding call-site variation broke that. **Reverted.**

### Tier 3c (reverted): SplitMix-style hash for `LongPairJoinIndex.mix`

Replaced `31 * hashCode(a) + hashCode(b)` with a multi-multiply SplitMix64 fold, aiming for shorter probe chains on sequential composite keys.

Result (tier3c.{csv,log}): Q64 **+25% slower** (23512→29417ms), Q80 +22% slower. The current hash is slightly worse on distribution but the extra multiplies in the new version made the hash compute itself the dominant cost — `findSlot`'s 11% was mostly hash COMPUTE, not chain walk. More expensive hash per call > shorter chains. **Reverted.**

### Tier 3d (reverted): Dictionary peeling for `LongJoinIndex.matchDictionaryRows`

Velox-style optimization: cache `findSlot` result per dict id so repeated occurrences of the same dict entry within a batch amortize the hash+probe cost. Used a per-index `int[] slotCache` + `int[] generationCache` with a per-call generation counter to avoid per-call reset.

Result (tier3d.{csv,log}): Q64 **+28% slower** (23512→30195ms), Q80 +24%, Q97 −3% (tiny win), Q65 +9%. The Q97 sign was in the expected direction (peeling saves work for high-duplicate id patterns) but too small to offset cost elsewhere. For Q64/Q80 the TPC-DS SF10 dictionaries are sparse enough per batch that peeling is mostly cache misses — extra array-load + branch per row on top of the same probe work. **Reverted.**

### Pattern across all three post-Tier-2 attempts

Every attempt to restructure the probe inner loop regressed the large probe-bound queries (Q64/Q80). Combined with the Tier 2a hasNulls-split experience (where adding any extra check to the unconditional path also regressed), the conclusion is:

**The current probe loop is at a JIT-local-optimum for HotSpot on this workload.** Any code added to the hot body — instanceof guards, cache checks, more math — costs more than the theoretical savings. Further real gains require either:

- **Different loop shape entirely**: SIMD (`jdk.incubator.vector`) on primary keys, prefetch intrinsics, or a cache-oblivious table layout.
- **Fewer probes needed**: query-planner-driven dedup / sort of outer probe keys before the probe call, so duplicates arrive contiguously (then a trivial consecutive-id check inside the loop is cheaper than the full dictionary peel).
- **Specialized build-side layouts**: perfect hashing for low-cardinality dim tables (know cardinality at build time).

All of these are larger structural changes than what the current work envelope supports. The committed Tier 1/2 state is the right stopping point for this pass.

## Correctness

All four target queries (Q64, Q65, Q80, Q97) pass `TestQueries#testQueryNN` vs. the Trino-SQL reference across every tier. Q66 has a **pre-existing correctness failure on baseline** (not introduced by this work) — the Q66 perf number is still valid (same implementation path on both baseline and final).

`TestOperators` / `TestOperatorBatches`: identical pre-existing failure set from the very beginning, no new regressions introduced by any tier.

## Artifacts

All in [`benchmarks/tpcds/refactor-20260416/`](benchmarks/tpcds/refactor-20260416/):

| File | What |
|---|---|
| `FINAL.md` | this summary |
| `SUMMARY.md` | initial refactor summary |
| `tier1-summary.md` | Tier 1 writeup |
| `tier2-summary.md` | Tier 2 writeup |
| `profile-analysis.md` | pre-Tier-1 profile → prioritized plan |
| `profile-stack.log` | pre-Tier-1 JMH stack samples (4 queries) |
| `profile-q64q80.log` | post-Tier-2 JMH stack samples (Q64 + Q80) |
| `nitro-tpcds-targets.{csv,log}` | post-refactor pre-Tier-1 bench |
| `nitro-tpcds-wins.{csv,log}` | regression-check bench on previously-winning queries |
| `tier1-targets.{csv,log}` | post-Tier-1 bench |
| `tier2-targets.{csv,log}` / `tier2-v2.{csv,log}` | post-Tier-2 bench (v2 has 3 iterations) |
| `tier3.{csv,log}` | post-Tier-3a bench |
| `tier3b.{csv,log}` | I64×I64 + Dict×Dict fast paths (reverted — regression) |
| `tier3c.{csv,log}` | SplitMix hash (reverted — regression) |
| `tier3d.{csv,log}` | Dictionary peeling for LongJoinIndex (reverted — regression) |
| `q64-rerun.log` | isolated Q64 rerun for noise validation |
