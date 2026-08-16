# ClickBench q24 operator/SQL shape alignment

This directory isolates why ClickBench q24's full SQL CPU ratio did not reproduce the older operator-chain ratio.
All JMH results use JDK 26, one fork, a 12 GiB heap, three warmup iterations, five measurement iterations, the real
100-million-row split Parquet data, and the GC profiler.

The original operator harness read all 165 files through one scan/filter/TopN pipeline and ordered only by
`EventTime`. The SQL harness deterministically orders by `EventTime, WatchID, ClientIP, URL, SearchPhrase`, and the
Trino plan runs one scan/filter/local-TopN pipeline per split before exchange and final TopN. The updated operator
harnesses now reproduce those keys and that topology. Nitro also uses the integration's shared Parquet resources and
adaptive host-boundary batch policy.

| Shape | Nitro ms/op | Trino ms/op | Ratio | Nitro bytes/op | Trino bytes/op | Allocation ratio |
|---|---:|---:|---:|---:|---:|---:|
| one artificial multi-file pipeline | 8,139.39 | 38,173.46 | 0.213x | 2,135,416,874 | 168,385,455,640 | 0.013x |
| 165 split-local pipelines | 18,578.71 | 39,491.14 | 0.470x | 29,556,191,276 | 169,044,080,112 | 0.175x |
| split-local, integration-equivalent Nitro resources | 17,019.05 | 39,491.14 | 0.431x | 27,264,658,835 | 169,044,080,112 | 0.161x |

The post-bound-LIKE SQL result is 21,348.67 mean Nitro CPU-ms versus 34,056.67 for Trino (0.627x), with
36,516 MiB versus 82,347 MiB of measured thread allocation. JMH average time is not aggregate process CPU, so its
absolute value must not be substituted for that SQL metric. The structural result is nevertheless decisive: creating
the real split-local Nitro topology adds roughly 9--10 seconds and 25 GiB per operation relative to the old harness,
while the equivalent Trino control barely moves. The old 0.213x comparison was therefore not a SQL-shaped baseline.

The remaining SQL/operator difference is bounded: SQL Nitro CPU is about 25% above the exact-shaped JMH wall score,
while concurrent SQL Trino CPU is about 14% below its sequential JMH score. Further attribution must compare aggregate
CPU/hardware counters and lifecycle metrics for the shaped pipelines; it must not change a kernel based on the stale
single-source result.

A matched Linux `perf` probe over one warmup plus one measured exact-shaped operation gives Nitro 616.7 billion
instructions / 326.0 billion cycles versus Trino's 1,097.5 / 466.9 billion: 0.562x instructions and 0.698x cycles.
Nitro also uses 0.567x branches, 0.508x branch misses, 0.666x cache misses, 0.581x L1-data misses, and 0.514x dTLB
misses. Its lower 1.89 versus 2.35 instructions/cycle and much higher system time (19.86 versus 7.02 seconds over the
two operations) keep the instruction reduction from translating directly into elapsed time. This corroborates the
SQL CPU ratio: no extra TopN or function work is hidden outside the shaped operator graph. The remaining actionable
q24 target is the 27--36 GB split-local allocation/system-time lifecycle, not a rewrite of the already efficient
filter or TopN kernels. Counters were multiplexed at roughly 33%; use their ratios as attribution, not absolute event
totals.

Files:

- `nitro.json`, `trino.json`: corrected five-key ordering with the old one-pipeline topology.
- `nitro-split.json`, `trino-split.json`: 165 split-local pipelines, exchange, and final TopN.
- `nitro-split-exact.json`: split-local Nitro with the integration's shared resources and adaptive boundary batches.
- `nitro-perf-probe.json`, `trino-perf-probe.json`: matched one-warmup/one-measurement hardware-counter probe metadata.

No JFR, heap dump, or Kata artifact was created.

## Allocator-topology correction (2026-08-09)

The split-local Nitro harness still constructed all 165 partial pipelines with
one allocator, unlike Trino's independent source-operator allocators. The
corrected harness constructs one allocator per split from the same explicit
`EngineResources` owner and closes it with that split pipeline.

With three warmups, five measurements, a 12 GiB heap, and the GC profiler, the
corrected result is 16,739.133 ms/op and 29,613,139,716.8 bytes/op. The previous
integration-resource result was 17,019.05 ms/op and 27,264,658,835 bytes/op.
Thus the missing allocator topology understated allocation by about 2.35 GB but
did not explain the timing ratio. Against the adjacent SQL control
(38,085,641,371.6 bytes at 36,321.298 MiB), roughly 8.47 GB of SQL-only allocation
remains to be attributed. Raw output is in
`../20260809-close-transfer/q24-allocator-shaped-jmh2.log`.
