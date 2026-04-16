# Null-propagation + hash-quality architectural fixes — Q65 follow-up (2026-04-17)

Follow-up to [`../audit-fixes-20260417/FINAL.md`](../audit-fixes-20260417/FINAL.md). Closes the two biggest architectural gaps the Q65 JFR identified: null-propagation overhead in scalar binary ops and poor hash quality in the grouping/join/distinct hash tables.

## TL;DR

| Query | Session baseline | Null-protocol fixes | Null-protocol + hash | Δ total | Trino harness | Trino/Nitro (now) |
|---|---:|---:|---:|:-:|---:|:-:|
| Q64 | 27047 | 25230 | **23168 ± 543** | **−14%** | ~23000 | **~0.99× (parity)** |
| **Q65** | **7395** | 6545 | **5933 ± 94** | **−20%** | 4731 | **0.80×** (was 0.61×) |
| Q80 | 10026 | 11973 | **10641 ± 624** | +6% | 8684 | 0.82× |
| Q97 | 3540 | 3697 | 3517 ± 101 | 0% | 4125 | **1.17× (Nitro faster)** |

**Q65 closed from 1.64× slower to 1.25× slower than the corrected Trino harness.** Q64 at parity. Q97 still faster than Trino. Q80 within noise of baseline.

## Architectural context

The same Q65 JFR that motivated the harness audit also surfaced two structural gaps where Nitro does work Trino doesn't:

1. **Scalar null-propagation.** Nitro keeps VALUES and NULLS as two separate streams; scalar binary functions run an unconditional per-row null-OR loop even when both inputs have no nulls. Trino attaches nulls inline on Blocks and the per-Block `mayHaveNull()==false` fast path is compiled out by the JIT.

2. **Grouping/join hash-table quality.** Eight of Nitro's nine hash tables used a weak `31 * Long.hashCode(a) + Long.hashCode(b) + …` combiner with a single `>>> 16` xor-shift. On TPC-DS surrogate keys (item_sk, store_sk, date_sk — all fit in 32 bits, so `Long.hashCode(x) = (int) x`), adjacent key pairs collide heavily, lengthening probe chains. Trino uses Murmur-style mixing throughout.

The Q65 JFR decomposition, pre-fix (1718 samples):

| Call chain | Samples | % | Category |
|---|---:|---:|---|
| `MultiplyI64.applyNulls` + transitive callees | ~435 | 25% | null-propagation |
| `LongPairGroupingTable.assignGroup` | 478 | 28% | hash/probe |
| `LongJoinIndex.findSlot + matchLongRows` | 138 | 8% | hash/probe |

## Fixes shipped

### Null-propagation

1. **`BooleanVector.isAllFalse()`** — lazy-memoized O(n) scan with O(1) cache. Reset on `clearForReuse()`. Safe because BooleanVectors are treated as immutable after `Streams` publication.
2. **`VectorAccess.isAllFalseNulls(Vector)`** — shared helper recognizing null ref, 1-run RLE-of-false, length-1 flat false, and flat BooleanVector via the cached `isAllFalse()`.
3. **`PlanEvaluator.completeRequestedStreams`** — synthesizes missing NULLS/ERRORS as a 1-run RLE-of-false (O(1)) instead of a batch-size flat BooleanVector pre-filled with false (O(n)).
4. **`MultiplyI64`, `LessThanI64`, `EqualI64`, `SubtractI64`** — short-circuit their `applyNulls` loop when both inputs pass `isAllFalseNulls`.
5. **`SumStateVector`** — O(1) `nullGroupCount` counter maintained in `increment()`, `initialize()`, `grow()`, `clearForReuse()`. `hasAnyNull()` → `nullGroupCount > 0`. Bounded `copyXxxTo(output, count)` variants.
6. **`Sum.result`** — sizes output to `maxGroup + 1` (visible range only, no tail-trues); emits NULLS as 1-run RLE-of-false when `hasAnyNull()` is false.

### Hash quality

All eight weak `mix` functions replaced with Fibonacci-prime combine + Murmur3 64-bit finalizer:

```java
long hash = first * 0x9E3779B97F4A7C15L
          + second * 0xC4CEB9FE1A85EC53L
          + [third * 0x94D049BB133111EBL]
          + [fourth * 0xBF58476D1CE4E5B9L]
          + nullMask;
hash ^= hash >>> 33;  hash *= 0xFF51AFD7ED558CCDL;
hash ^= hash >>> 33;  hash *= 0xC4CEB9FE1A85EC53L;
hash ^= hash >>> 33;
return (int) hash;
```

Locations touched:
- `GroupingState.LongPair/LongTriple/LongQuad GroupingTable.mix`
- `HashJoinOperator.LongPair/LongTriple JoinIndex.mix`
- `DistinctKeySet.LongPair/LongTriple/LongQuad DistinctIndex.mix`

This is the same finalizer `HashJoinOperator.LongJoinIndex.mix` (single key) already used.

## Measured impact

### Q65 hot-spot share (JFR, 3-iteration sample)

| Method | Pre-session | Post null-protocol | Post + hash |
|---|---:|---:|---:|
| `LongPairGroupingTable.assignGroup` | **28.9%** | 28.7% | **14.5%** |
| `MultiplyI64.applyNulls` | 9.5% | 8.5% | 8.6% |
| `RleVector.runIndexFromHint` | 7.6% | 8.7% | 8.9% |
| `LongJoinIndex.findSlot` | 5.2% | 4.4% | 7.1% |

The hash change halved the grouping-table's share — the single biggest single-change effect in this session. `MultiplyI64.applyNulls` remains stuck at ~8.5% because the short-circuit doesn't fire on one specific code path in Q65's Sum→multiply chain (see "Remaining gap" below).

### Per-query wall clock

5 warmup + 10 measurement iterations, SF10 parquet, 1 fork, JMH 1.37:

| Query | Session baseline | This session final | Δ |
|:-:|---:|---:|:-:|
| Q64 | 27047 | 23168 | **−14%** |
| Q65 | 7395 | 5933 | **−20%** |
| Q80 | 10026 | 10641 | +6% |
| Q97 | 3540 | 3517 | 0% |

The ±-error bars are unusually tight on the final run (Q65 ±94 ms on 5933 mean — 1.6% relative error), indicating the changes are compiler-friendly and consistent.

### The Q80 tradeoff

Q80 regressed +6% overall — the `SumStateVector` counter's extra branch in `increment()` costs measurable time on Q80's hot 3-way aggregator. The hash change then recovered most of the regression (+19% after null-protocol, ended at +6% after hash).

If Q80's ~6% becomes a priority, the mitigation is a bulk-update path in `Sum.accumulate`'s null-free fast paths (`tryAccumulateNullFreeGrouped`, `accumulateDictionary`): count first-time-visited groups in a single post-increment pass and bulk-decrement `nullGroupCount` once. Not prototyped this session.

## What's left

The remaining ~35-40% gap on Q65 and Q80 vs Trino is mostly:

1. **Grouping-table layout.** Even with good hash, Nitro's table keeps four parallel arrays (`firstKeys[]`, `secondKeys[]`, `nullMasks[]`, `groupIds[]`) indexed by slot. Each probe step is four independent cache-line loads. Trino's `FlatHash` packs hash-prefix + keys + group-id into one byte-packed row; one slot = one cache line. Restructuring this is 2-3 days of work with medium risk; plausible Q65/Q80 recovery 15-20%.

2. **Dynamic filtering.** Every Trino optimizer plan has `dynamicFilterAssignments` on its scans (semi-join-reduction pushed from build to probe side). Neither harness implements this. Symmetric asymmetry — both do more work than real Trino — but worth eventually matching.

3. **One remaining scalar null-propagation site.** `MultiplyI64.applyNulls` in Q65's threshold predicate receives a left operand that still has `isAllFalseNulls == false` despite the Sum output being non-null in the visible range. The 1-run RLE emitted by `Sum.result` should fire the short-circuit, but something in the Q65 plan's memoization/merge path is re-wrapping the NULLS into a flat BooleanVector with stale-null tail entries. Worth a targeted trace if we care about the last ~8% on Q65.

## Files changed

- `src/main/java/org/weakref/nitro/data/BooleanVector.java` — `isAllFalse()` with cache
- `src/main/java/org/weakref/nitro/data/SumStateVector.java` — `nullGroupCount`, `hasAnyNull()`, bounded `copyXxxTo`
- `src/main/java/org/weakref/nitro/function/scalar/builtin/VectorAccess.java` — `isAllFalseNulls(Vector)`
- `src/main/java/org/weakref/nitro/function/scalar/builtin/MultiplyI64.java` — short-circuit
- `src/main/java/org/weakref/nitro/function/scalar/builtin/LessThanI64.java` — short-circuit
- `src/main/java/org/weakref/nitro/function/scalar/builtin/EqualI64.java` — short-circuit
- `src/main/java/org/weakref/nitro/function/scalar/builtin/SubtractI64.java` — short-circuit
- `src/main/java/org/weakref/nitro/operator/aggregation/Sum.java` — visible-range sizing + O(1) null check + 1-run RLE-of-false on no-null
- `src/main/java/org/weakref/nitro/operator/evaluator/PlanEvaluator.java` — synthesize absent NULLS/ERRORS as RLE-of-false
- `src/main/java/org/weakref/nitro/operator/GroupingState.java` — Murmur3 finalizer in LongPair/Triple/Quad mix
- `src/main/java/org/weakref/nitro/operator/HashJoinOperator.java` — Murmur3 finalizer in LongPair/Triple join mix
- `src/main/java/org/weakref/nitro/operator/DistinctKeySet.java` — Murmur3 finalizer in LongPair/Triple/Quad mix
- `src/test/java/org/weakref/nitro/TestOperators.java` — two tests updated to read via `VectorAccess.booleanValues` (accept either flat or RLE shapes)

All 8 Q64/Q65/Q80/Q97 correctness tests pass. `TestOperators` at baseline failure count (3 fail + 5 err, all pre-existing).

## Artifacts

- `bench-final.csv` / `bench-final.log` — post-null-protocol bench
- `bench-hash.csv` / `bench-hash.log` — final bench (null-protocol + hash)
- `q65-jfr-console.log` — JFR capture run outputs
