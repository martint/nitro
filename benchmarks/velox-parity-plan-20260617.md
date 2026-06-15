# Closing the Velox gap — diagnosis and plan (2026-06-17)

> **Before profiling any laggard, follow `benchmarks/laggard-parity-methodology.md`.** Step 1 there
> (verify per-stage row counts are equal between the two engines) must pass first. The q59 case showed an
> apparent "constant factor" gap was really a harness that pushed 5× more rows through projection +
> aggregation than Velox — fixed by aligning the plan shape, not by engine tuning. The "broad ~1.3× constant
> factor" framing below is only valid once row-count parity is confirmed.

Baseline: `benchmarks/four-engine-sweep-linux-20260617.md`. The Nitro **compiled** engine is the parity
vehicle (geomean vs Velox: TPC-H 1.30×, TPC-DS 1.35×, ClickBench-subset 0.71× — already faster on CB).
Interpreted is 1.9–2.9× and is not the focus.

## The gap is two things, not one

**(a) A handful of pathological outliers (4–32×).** On *every* one of these, **Trino is also far slower than
Velox** (e.g. TPC-DS q88: Velox 194ms, compiled 1910ms, Trino 5728ms; q64: 1280 / 5730 / 13117; ClickBench
q29: 1000 / 31974 / 8837). Nitro compiled is consistently *faster than Trino* here. So these are **not Nitro
defects** — they are queries where Velox has a structural advantage both JVM engines lack.

**(b) A broad ~1.3× constant-factor tax** on the bulk of queries — the JVM-vs-C++ scan/decode/access overhead.

## Profile evidence (JFR, compiled engine, single query)

| Query | What dominates | Share |
|---|---|---|
| TPC-DS q88 (9.84×) | **Parquet decode of `store_sales`** (RLE/bit-pack/null decoders) | ~83% scan, ~17% compute |
| ClickBench q29 (32×) | **RE2J regexp** (`NFAMachine.add/step` — NFA fallback for capturing-group `REGEXP_REPLACE`) | ~90% regexp |
| TPC-H q1 (1.61×) | **FFM segment access** (`SegmentVarHandle.checkSegment`→`isAlignedForElement`) then JIT kernel + column bridge; decode is a smaller share | alignment-check ≈29% |

## The three levers

### Lever 1 — Dynamic filtering (runtime filters into scans).  *Biggest. Broadest. Hardest.*
Velox's HashJoin auto-pushes the build side's key range / membership into the probe-side `TableScan` at
runtime, so a selective dimension (e.g. q88 `s_store_name='ese'`, 1 of ~200 stores) prunes ~99% of the fact
scan *before decode*. The Nitro/Trino operator harnesses scan and decode the **entire** fact table, then
discard via the join. This single mechanism explains the scan-bound outlier cluster:
**q88, q64, q23, q21, q49, q40, q80** (and partly q9), plus TPC-H **q21**. q88 is 83% decode → a selective
runtime filter could take it from ~1910ms toward ~400ms.

Today Nitro's scan has `constrain(Mask)` (post-decode lazy masking) and row-group metadata
(`RowGroupInfo`/`PrunedBlockMetadata`) but **no runtime filter** (nothing pushes build-side keys to the probe
scan). Work: build-side min/max + IN/bloom summary → push to `(Trino|Hardwood)ParquetScanOperator` → prune row
groups by statistics and skip/filter pages at decode. The metadata plumbing already exists; the filter
construction + decode-time application is new. To keep the comparison apples-to-apples, add the same to the
Trino operator harness.

### Lever 2 — Constant-factor scan/access path.  *Medium win, broad, cheaper.*
- **FFM alignment checks** (`isAlignedForElement`, ~29% of q1): off-heap `Slice`/MemorySegment reads in the
  parquet→kernel path validate layout/alignment on every access. Switch hot reads to **unaligned** access
  (byte-aligned layouts / `JAVA_LONG_UNALIGNED`) or bulk-copy a column to a primitive array once per batch.
  Nitro's own hot loops already use heap `byteArrayViewVarHandle` (no check) — the cost is in the segment-backed
  reads. Likely 10–20% broadly, low risk.
- **Column-access bridge** (`CompiledQuerySupport.convertColumn/stringColumn/advance`): per-batch glue between
  the parquet reader and the JIT kernel. Tighten / widen batches.
- **Decode throughput** itself (RLE/bit-pack): hardest; would need wider SIMD decode. Defer.

### Lever 3 — Regexp engine (ClickBench q29 only).  *Narrow.*
~90% of q29 is RE2J in NFA mode (capturing-group `REGEXP_REPLACE`). Velox uses native RE2; even Trino is 8.8×.
Options: detect simple anchored patterns and hand-roll, precompile/cache DFA, or accept it. One query.

## Recommended sequence
1. **Lever 2 FFM-alignment fix** — cheap, broad, de-risks the constant factor first (days).
2. **Lever 1 dynamic filtering** — the real prize; collapses the outlier cluster and most of the absolute
   TPC-DS gap (the big multi-week item).
3. **Lever 3** — opportunistic, last.

Interpreted-engine parity is out of scope (compiled is the vehicle); revisit only if interpreted-only shapes matter.
