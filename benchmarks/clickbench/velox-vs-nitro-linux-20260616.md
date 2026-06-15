# ClickBench whole-query: Nitro vs Velox — Linux, full hits parquet, single-thread, 2026-06-16

Velox harness: velox/benchmarks/clickbench/ClickBenchBenchmark.cpp (mirrors the Nitro query NUMBERING +
definitions; canonical copy in ~/notes/velox/harness/). Single-thread: num_drivers=1 num_io_threads=1
num_splits_per_file=1. Nitro from the three-engine sweep (JMH -f 1). Both read /root/data/clickbench.

## Result (Nitro/Velox; >1 = Velox faster)
geomean = 0.93x  (32 queries; median 0.97x).  Nitro faster on 18/32, Velox on 14/32.

So on ClickBench (single-table scan + group-by/agg) Nitro's interpreted engine is ~even with Velox,
slightly ahead overall -- a sharp contrast with TPC-H (Velox ~2x ahead). Consistent with the session
finding that Nitro's operators (esp. grouping + the string kernels improved this session) are competitive;
the gap is join + scan/decode (TPC-H), not grouping-heavy single-table work (ClickBench).

## Gaps / caveats
- q0 (Nitro query00 = scan ALL ~100 columns over 14GB) not timed: very slow, >200s; excluded.
- q37-43 (CounterID=62 + July-EventDate-window "PageViews" family): SIGSEGV deep in Velox Driver::enqueue
  (Velox-internal crash with this plan/data shape; needs gdb). Excluded.
- q29/33/34/35 absent from the Nitro sweep CSV (not in the 40-row nitro result), so not in the 32-query geomean.
- Velox times are single verbose runs (not JMH-averaged) -> noisier than the Nitro numbers; directional.
- Velox pinned single-thread (no parallel scan); with default parallelism Velox's scans would be faster.
