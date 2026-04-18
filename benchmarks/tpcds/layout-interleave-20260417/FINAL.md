# Hash-table layout interleaving — session 3 (2026-04-17)

Follow-up to [`../q65-null-protocol-20260417/FINAL.md`](../q65-null-protocol-20260417/FINAL.md). Attacks the remaining biggest architectural gap: cache behavior of the two-key hash tables.

## TL;DR

| Query | Pre-layout | Post-layout | Δ | Trino harness | Trino/Nitro (now) |
|---|---:|---:|:-:|---:|:-:|
| Q64 | 23168 | 23070 ± 276 | ~0% (noise) | ~23000 | ~1.00× (parity) |
| **Q65** | 5933 | **5649 ± 36** | **−4.8%** | 4731 | **0.84×** (was 0.80×) |
| **Q80** | 10641 | **10404 ± 476** | **−2.2%** | 8684 | 0.83× |
| Q97 | 3517 | 3450 ± 54 | −1.9% | 4125 | **1.20× (Nitro faster)** |

Clean wins on Q65, Q80, Q97; Q64 unchanged. Error bars unusually tight (Q65 ±0.6% relative) — indicates the change is JIT-friendly at steady state.

## Diagnosis

Post-hash-fix Q80 JFR (2873 samples) showed an enormous single hot spot:

| Method | % of wall clock |
|---|---:|
| `LongPairJoinIndex.findSlot` | **62.3%** (1791 samples) |
| `Allocator.borrowMask` | 3.3% |
| `LongPairJoinIndex.matchRows` (self, post-probe) | 2.0% |
| others | <2% each |

62% in one function is extreme. Root cause was the layout:

```java
private long[] firstKeys;
private long[] secondKeys;
private long[] singleRows;
private LongArrayList[] rowsBySlot;
```

Each `findSlot` probe step reads three separate arrays at slot index. On Q80's `store_sales ⨝ store_returns` table (capacity ~4M slots, ~128 MB across the three long[] arrays), every probe step was three independent cache-line loads — nowhere near resident.

Q65's analogous `LongPairGroupingTable` is on the same pattern but smaller (14.5% of Q65 wall clock after the prior commits), so its share was masked until the grouping-table share fell from 28.9% with the hash-quality fix.

## Fix

Replace three parallel `long[]` arrays with a single interleaved `long[capacity * 3]` holding `{firstKey, secondKey, singleRow}` per slot. Each probe step now reads 24 bytes from one cache line (linear probing continues walking the same or adjacent cache lines). The multi-match `LongArrayList[] rowsBySlot` stays separate because it's the cold path (accessed only on key-match with multi-row groups).

Applied to:
- `HashJoinOperator.LongPairJoinIndex` — stores `{firstKey, secondKey, singleRow}`
- `GroupingState.LongPairGroupingTable` — stores `{firstKey, secondKey, groupId}`; `byte[] nullMasks` stays parallel (consulted only on key match, rarely 1)

Per-slot access helpers use `base = slot * 3` then `entries[base + FIELD_OFFSET]`. The `findSlot` hot path:

```java
private int findSlot(long first, long second)
{
    long[] table = entries;
    int slot = mix(first, second) & mask;
    while (true) {
        int base = slot * ENTRY_STRIDE;
        long singleRow = table[base + SINGLE_ROW_OFFSET];
        if (singleRow == NO_MATCH_ROW_REFERENCE) return slot;
        if (table[base + FIRST_KEY_OFFSET] == first && table[base + SECOND_KEY_OFFSET] == second) return slot;
        slot = (slot + 1) & mask;
    }
}
```

Three consecutive `table[base+i]` reads from the same cache line. Linear probing continues on the same or adjacent cache lines.

## JIT warmup note

Q64's 15-join plan needs more warmup than Q80/Q65/Q97 after the layout change — the interleaved access pattern requires more C2 compile work than the old straight-field-load pattern. With 5 warmup iterations Q64 showed extreme variance (iter 1-7: 40-72s; iter 8-10: 22-23s). With 15 warmup iterations Q64 stabilizes at 23070 ± 276 ms (parity with pre-layout 23168 ± 543).

The benchmark harness (`BenchmarkQueries`) should probably increase `@Warmup(iterations = 3)` to `5` or more so variance is reliable on Q64-shaped plans. Not addressed in this commit.

## Files changed

- `src/main/java/org/weakref/nitro/operator/HashJoinOperator.java` — `LongPairJoinIndex` interleaved layout
- `src/main/java/org/weakref/nitro/operator/GroupingState.java` — `LongPairGroupingTable` interleaved layout

All 8 Q64/Q65/Q80/Q97 correctness tests pass. `TestOperators` at baseline failure count (3 fail + 5 err, all pre-existing).

## What's still left

The triple / quad variants (`LongTripleJoinIndex`, `LongTripleGroupingTable`, `LongQuadGroupingTable`) and the `DistinctKeySet` triple/quad tables remain in the parallel-arrays layout. Without a profile showing them as a hot spot, it's not obvious they'd benefit. Q64's cross_sales group-by uses `LongQuadGroupingTable`-sized keys (15 columns), but it falls through to a generic path since we don't have a `LongFifteen` table — this is either RowBasedGroupingTable or similar, which is a different architecture.

Other still-open architectural gaps from the prior diagnosis:
- Dynamic filtering (Trino's semi-join-reduction pushdown from build to probe scans). Neither harness implements this.
- Remaining `MultiplyI64.applyNulls` still ~8.5% on Q65 (one specific call site where the short-circuit doesn't fire due to how the NULLS stream is re-wrapped). Targeted trace needed.
- Q80 counter-overhead on `SumStateVector.increment` (bulk-update in Sum's null-free fast paths would recover the small residual).

## Artifacts

- `bench-layout.csv` / `bench-layout.log` — full 5w+10i bench across Q64/Q65/Q80/Q97
- `bench-q64-longwarmup.log` — Q64 15w+10i stabilization run (confirms layout is fine with enough warmup)
- `bench-q80-interleave.log` — Q80 isolated stabilization run
