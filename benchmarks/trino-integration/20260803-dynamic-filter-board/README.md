# Completed dynamic-filter benchmark board

This board measures the Nitro and Trino backends over the same local Parquet SF10 data after Nitro commit
`cd66dbec` and Trino integration commits through `d63e53f6`, including `08d9ee21`, `763828a1`, and `f04b9942`.
No JFR or heap-dump artifact is retained.

`board-controlled.csv` is the primary result. It uses the warmed TPC-H and TPC-DS board and replaces ClickBench with
the 12 GiB-heap sweep in `clickbench-12g-controlled-board.csv`. ClickBench q19, q29, and q30 use controlled
three-measurement means: q19 removes ordinary single-sample noise, q29 uses the corrected ClickHouse-to-Trino regexp
replacement syntax, and q30 uses fresh engine processes to remove the severe q29-to-q30 heap/run-order interference.
The q30 source rows are preserved in `clickbench-q30-12g-controlled.csv`; their full Maven daemon logs were
`daemon-d445184b.log` and `daemon-dd8b4f03.log`.

`board-warm.csv` and its six `*-warm.log` sources preserve the earlier one-warmup board. `board.csv` and the
unsuffixed logs preserve the zero-warmup diagnostic sweep. Do not use either older board for final engine comparisons:
cold generated-code costs distort several Nitro queries, and the 3 GiB ClickBench JVM amplifies allocation-heavy
run-order interference. The three-measurement `tpcds-q03-warm-interleaved.log` control demonstrates the cold-code
effect: Nitro/Trino mean CPU is 0.136, corroborating the SQL-shaped operator ratio of 0.141, whereas the cold sweep
reported 1.092.

| suite | queries | wall geomean | CPU geomean | wall wins | CPU wins | total wall | total CPU |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-DS | 103 | 0.673 | 0.442 | 79 | 92 | 0.824 | 0.634 |
| TPC-H | 22 | 0.800 | 0.737 | 19 | 18 | 0.812 | 0.788 |
| ClickBench | 43 | 0.886 | 0.684 | 29 | 38 | 0.789 | 0.724 |
| all | 168 | 0.738 | 0.528 | 127 | 148 | 0.807 | 0.689 |

The controlled CPU ratios corroborate the SQL-shaped operator board (TPC-H 0.752, TPC-DS 0.561, ClickBench 0.790);
TPC-DS and ClickBench are somewhat better here. ClickBench now uses 0.684x CPU and 0.886x wall geometrically. Its
remaining aggregate gap is wall utilization rather than hidden CPU.

ClickBench q29 now reproduces the operator-level result: Nitro is 2.995 s / 17.925 CPU-s versus Trino at 8.492 s /
53.343 CPU-s, or 0.353x wall and 0.336x CPU. The old parity result measured a different expression because Trino
interprets ClickHouse's `\\1` replacement as a literal `1`; the integration harness now translates it to Trino's
`$1` capture syntax for both engines. Fresh-process q30 is likewise not an operator regression: Nitro is 1.336 s /
8.193 CPU-s versus 3.151 s / 21.081 CPU-s, or 0.424x wall and 0.389x CPU. Controlled q19 is CPU-neutral (1.009x)
but faster on wall (0.829x), so no implementation change is justified there.

ClickBench q05/q06 exposed a plan-shape error rather than an operator regression. The Nitro-enabled optimizer had
disabled Trino's single-DISTINCT-to-group-by rewrite and serialized all 100 million rows through one Nitro
aggregation. Restoring Trino's ordinary strategy changes q05 from 1.997x to 1.041x p50 wall and q06 from 3.450x to
1.148x, while CPU is 0.984x/0.946x. The `clickbench-q05-12g-controlled.log` and
`clickbench-q06-12g-controlled.log` files preserve the serialized-plan controls; the `*-trino-shape-controlled.log`
files are the accepted overlays. The q05/q06 operator harnesses now reproduce split-local distinct grouping, the
exchange boundary, final distinct grouping, and global count; the separate compiled lowerings remain explicitly
kernel-only and are not SQL expectations.

Q10 demonstrates why plan capture must use the same statistics regime as the benchmark. The no-statistics correctness
session selects the PRE_AGGREGATE `GroupId` rewrite, while the performance setup runs `ANALYZE` and selects the
`MarkDistinct` shape already modeled by the ordinary operator harness. The controlled analyzed capture in
`clickbench-q10-trino-shape-operator-cpu.log` is Nitro 2.613 s / 5.716 CPU-s versus Trino 3.596 s / 8.170 CPU-s
(0.727x wall, 0.700x CPU). The dominant aggregate is 3.440 CPU-s for Nitro versus 5.800 CPU-s for Trino; scan is
1.127 versus 1.276 CPU-s and partitioned output is 0.929 versus 0.880 CPU-s. The compiled q10 lowering remains a
fused-kernel benchmark and is labeled accordingly.

The post-change correctness/admission gate preserves Trino's normal optimizer choices. All 43 ClickBench queries,
all 22 TPC-H queries, and all 99 TPC-DS queries pass with zero Nitro expression, aggregation, aggregation-source,
or native-scan rejections. TPC-DS q31 and q58 exposed scalar adapters that assumed structural long decimals were
always flat `StructVector` instances; Nitro legitimately preserved outer dictionary/RLE encodings through joins and
branches. Commit `763828a1` makes structural field access encoding-neutral without decoding the vectors. The
`clickbench-correctness-trino-shape-final.log` file is the accepted ClickBench gate;
`clickbench-correctness-trino-shape.log` preserves the earlier q10 diagnostic before generic `any_value` bindings
were added.

The largest remaining material wall-only gap is TPC-DS q09. The largest remaining CPU excesses are TPC-DS q22
(+4.128 s), q67 (+3.596 s), q04 (+3.298 s), and q47 (+1.150 s); the ClickBench wall ratios above 1.3 are subsecond
queries and need controlled repeats before implementation work. Q72 is no longer a regression after completed
dynamic-filter admission:
the warmed run is 1.542 s / 4.631 CPU-s for Nitro versus 5.908 s / 23.025 CPU-s for Trino.

The unsuffixed ClickBench Nitro log ends with a failure in an unrelated smoke test that assumes a baseline runner is
present. All 43 requested measurements completed before that failure. Both warmed ClickBench sweeps select only the
performance test and pass.
