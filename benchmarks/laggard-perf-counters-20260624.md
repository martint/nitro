# Laggard microarchitectural profile (2026-06-24)

`sudo perf stat` HW counters via delta method (PerfDriver, iters lo=4/hi=12, warmup=5, taskset -c 0).
Nitro (new reader) vs Trino harness, single-thread. instr/cyc per iteration; IPC=instr/cyc.

| query | gap | engine | instr/it | IPC | L1miss/it | miss% | instr N/T | miss N/T | class |
|-------|-----|--------|----------|-----|-----------|-------|-----------|----------|-------|
| cb q02 | 1.66x | nitro | 15.51B | 4.99 | 58M | 0.7% | **2.75x** | 0.75x | INSTRUCTION |
| cb q02 |      | trino | 5.63B  | 3.21 | 78M | 2.9% | | | |
| cb q08 | 1.67x | nitro | 15.74B | 4.85 | 62M | 0.7% | **2.75x** | 0.78x | INSTRUCTION |
| cb q08 |      | trino | 5.73B  | 3.27 | 79M | 3.0% | | | |
| cb q20 | 1.23x | nitro | 13.33B | 4.24 | 142M | 2.2% | **1.95x** | 0.93x | INSTRUCTION |
| cb q20 |      | trino | 6.82B  | 2.78 | 152M | 5.1% | | | |
| tpch q06 | 1.58x | nitro | 31.60B | 4.01 | 332M | 2.2% | **1.92x** | 1.58x | INSTRUCTION |
| tpch q06 |      | trino | 16.50B | 3.35 | 210M | 3.1% | | | |
| tpch q14 | 1.13x | nitro | 29.89B | 3.12 | 549M | 4.2% | 1.06x | **1.85x** | LATENCY |
| tpch q14 |      | trino | 28.16B | 3.19 | 298M | 2.5% | | | |
| tpch q19 | 1.21x | nitro | 38.21B | 2.65 | 510M | 3.0% | 1.11x | 1.22x | mixed |
| tpch q19 |      | trino | 34.38B | 2.78 | 417M | 2.8% | | | |
| tpch q15 | 1.16x | nitro | 34.42B | 3.23 | 739M | 4.9% | 1.12x | 1.32x | mixed |
| tpch q15 |      | trino | 30.70B | 3.37 | 561M | 4.4% | | | |
| tpcds q82 | 1.11x | nitro | 71.83B | 3.16 | 989M | 2.3% | 1.20x | 1.17x | mixed |
| tpcds q82 |      | trino | 59.67B | 3.04 | 849M | 3.7% | | | |
| tpcds q37 | 1.10x | nitro | 61.32B | 2.84 | 971M | 2.8% | 1.04x | 1.23x | latency-ish |
| tpcds q37 |      | trino | 58.97B | 3.07 | 792M | 3.4% | | | |

## Classification & levers
- **INSTRUCTION-BOUND (cb q02/q08/q20, tpch q06):** Nitro issues 1.9-2.75x MORE instructions at HIGHER
  IPC (4-5) and FEWER cache misses. Wall time tracks instruction count. The gap is the per-position
  interpreted filter+aggregate machinery (VectorAccess value/null lambdas, LongComparisonMaskSupport
  per-row null/error, Mask.retainIf) vs Trino's columnar/SIMD filter (DictionaryAwareColumnarFilter does
  ~1 op per many rows). LEVER = cut per-row instructions: vectorize the filter-mask eval + aggregate
  accumulate, or eliminate lambda indirection on the hot path (the compiled engine does this structurally).
  This is the OPPOSITE of an unfixable constant factor — there is 2-2.75x instruction headroom. (NB: the
  earlier RLE-compare attempt failed because it did NOT cut instruction count; it added a runIds alloc.)
- **LATENCY-BOUND (tpch q14):** ~equal instructions, 1.85x the L1 misses. Scattered access (join
  single-position output copy / scan). LEVER = layout/prefetch / bulk (vectorized) join-output copy.
- **MIXED/SMALL (tpch q19/q15, tpcds q82/q37):** <=1.2x on both axes — genuinely near parity, low headroom.

Tooling: PerfDriver.java (reflective BenchmarkQueries runner), /tmp/perfdelta.sh.
