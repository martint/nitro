# Scope: closing the Nitro skip-decode gap on nullable, fragmented DF scans

## Motivation
The 2026-06-22 three-engine sweep + per-query profiling found that after the committed reader wins
(decimal word-load, SIMD dict gather) the remaining Nitro-vs-Trino-reader scan gap concentrates on
**nullable, fragmented dynamic-filter scans**. Worst case: **q76 = 1.32–1.46×** (nitroSkip vs Trino's
`SkipDecodeScanOperator`). q76 is 79% scan (three unioned fact tables with null group keys); its DF
produces many small *gapped* survivor runs, so the per-run skip machinery dominates.

JFR (q76, scan subtree = nitro 4637 vs trino 2408 samples, ~1.9×):
- `RleReader.skip` 383 + `skipCountingOnes` 304 = **687** (def + id RLE walk, per survivor run)
- `RleReader.read` 400, `readSelectedLongs` 371, `filterDictLongs` 548 (lead DF, equal-work to Trino),
  `gatherDictionary`/inline gather 171+.

## KEY FINDING: this is NOT a from-scratch rewrite
Nitro's `ColumnReader.readSelectedLongs` **already implements Trino `SkipFlatColumnReader`'s algorithm**
— coalesce consecutive survivors into a run, `skip(gap)` then `read(run)`, co-advancing a separate
definition-level decoder and value/id decoder. Operation-for-operation:

| step | Nitro (readSelectedLongs, nullable) | Trino (readSelectedNullable) |
|------|--------------------------------------|------------------------------|
| gap  | `rle.skip(defRle.skipCountingOnes(gap))` | `valueDecoder.skip(defLevelDecoder.skip(gap))` |
| run  | `defRle.readRunCountingOnes(runDef,run)` | `defLevelDecoder.readNext(isNull,off,run)` |
| read | `rle.read(ids)` + gather | `valueDecoder.read` / `readDictionaryIds` |

So the structure is the same; the gap is **per-operation constant factor** + a few **un-SIMD'd gathers**,
not a missing algorithm. That reframes the work from "rewrite" to "three targeted optimizations."

## Where the gap actually is, and the proposed work

### Stage 1 — SIMD the remaining nullable-path gathers  (LOW effort, LOW risk)
The committed SIMD `gatherLongs`/`gatherInts` were only wired into the *non-null* paths. Four scalar
gather sites remain in `ColumnReader`, all on the nullable streaming path q76 hits:
- L478 (long, full-non-null run in nullable page) and L686 (int) — **direct** `gatherLongs/Ints` calls.
- L578 / L808 (`materializeStreamingRunLong/Int`, mixed null+non-null run) — a *conditional* gather
  (only non-null positions consume an id); needs a masked/compress gather or leave scalar.
- Effort: ~1–2h. Expected: small (q76 gather is ~3–4% of total); helps every nullable dict scan.
- Risk: nil — same byte-identical pattern already shipped; verify via QueryDigest digests.

### Stage 2 — Vectorized bitWidth-1 definition-level (null-mask) decoder  (MEDIUM effort, the real lever)
`skipCountingOnes` (304) + the def half of `read`/`readRunCountingOnes` are the dominant per-run cost on
nullable scans. Today the def levels go through the **generic** `RleReader` (shared with dict ids). Trino
uses a **specialized** `FlatDefinitionLevelDecoder` / `NullsDecoders` for the bitWidth-1 null mask:
- popcount-based `skip(n)->nonNullCount` and a vectorized `readNext(isNull[],off,n)` that writes the
  boolean null mask directly, both operating a machine-word at a time over the packed bits.
- Nitro's `skipCountingOnes` already popcounts per 64-bit word (good); the gap is `readRunCountingOnes`
  materializing `int[] runDef` then summing, and the generic RLE bookkeeping per call.
- Proposal: add a dedicated `DefinitionLevelDecoder` (bitWidth fixed = 1) that (a) returns non-null count
  for a skip in O(words), (b) fills a `boolean[] isNull` for a run in O(words) via word-at-a-time
  expansion, replacing `defRle` in the streaming paths. Mirrors Trino but in Nitro's MemorySegment world.
- Effort: ~1–2 days. Expected: the bulk of the 304 + part of the 400/371 on nullable scans → q76 toward
  ~1.0–1.1×, and helps the ~10–15 nullable-skip-bound DS queries.
- Risk: MEDIUM — new decoder in a hot path; must stay byte-exact. Gate behind a flag during bring-up;
  fall back to `defRle` if not enabled. Full QueryDigest digest sweep (all 99) before default-on.

### Stage 3 — Reduce per-run RleReader call overhead / fuse def+id advance  (HIGHER effort, UNCERTAIN)
Each survivor run still pays `loadNextRun` (`readUleb128` + value read) whenever it crosses an RLE/
bit-packed boundary, on BOTH the def and id readers. Options: (a) coalesce the def-skip and id-skip into
one pass; (b) cache the current-run state more aggressively; (c) batch multiple survivor runs per page
before touching the decoders. Effort ~2–3 days, payoff uncertain (the algorithm is already O(runs)); do
only if Stage 1+2 leave a measured residual worth chasing.

## Success criteria & measurement
- Correctness: byte-identical QueryDigest digests, nitroSkip vs Trino reader, ALL 99 TPC-DS (+ the
  VerifySkip 3.6M-survivor check) — non-negotiable, every stage.
- Perf gate (idle box, taskset -c 0, min-of-3 ≥15-iter, the high-variance lesson): q76 ≤ ~1.05× vs Trino
  reader; no regression on the tuned set (q21/q24/q50/q82) or controls (q39/q13/q88). Re-run the full
  nitroSkip-vs-skipdecode board.

## Recommendation
Stage 1 is a cheap, safe follow-on to the committed gather work — do it. Stage 2 is the actual lever for
nullable skip-heavy queries and is the bulk of the remaining gap; medium effort, gated + digest-verified.
Stage 3 is speculative — defer until 1+2 are measured. This is **incremental**, not a rewrite: Nitro
already has Trino's skip algorithm, so the risk is contained to the def-level decoder swap (Stage 2).

## Out of scope / non-levers (already established)
- Decompression volume / PageIndex page pruning: Trino's baseline (`SkipDecodeScanOperator`) passes
  empty column/offset indexes, so both decompress the same bytes — not a fair lever.
- Predicate-over-dictionary: Nitro already has it (`filterDictLongs` + `acceptById`).
- Bulk bit-unpack: already SIMD (`VectorBitUnpacker`).
- Whole-vs-skip heuristic tuning for q76: only ~2% and risks the tuned set.
