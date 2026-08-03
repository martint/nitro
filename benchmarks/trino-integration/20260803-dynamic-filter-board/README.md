# Completed dynamic-filter benchmark board

This board measures the Nitro and Trino backends over the same local Parquet SF10 data after Nitro commit
`cd66dbec` and Trino integration commits `ecf7c653` and `08d9ee21`. No JFR or heap-dump artifact is retained.

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
| ClickBench | 43 | 0.925 | 0.679 | 29 | 38 | 0.831 | 0.722 |
| all | 168 | 0.746 | 0.528 | 127 | 148 | 0.826 | 0.688 |

The controlled CPU ratios corroborate the SQL-shaped operator board (TPC-H 0.752, TPC-DS 0.561, ClickBench 0.790);
TPC-DS and ClickBench are somewhat better here. ClickBench now uses 0.679x CPU and 0.925x wall geometrically. Its
remaining aggregate gap is wall utilization rather than hidden CPU.

ClickBench q29 now reproduces the operator-level result: Nitro is 2.995 s / 17.925 CPU-s versus Trino at 8.492 s /
53.343 CPU-s, or 0.353x wall and 0.336x CPU. The old parity result measured a different expression because Trino
interprets ClickHouse's `\\1` replacement as a literal `1`; the integration harness now translates it to Trino's
`$1` capture syntax for both engines. Fresh-process q30 is likewise not an operator regression: Nitro is 1.336 s /
8.193 CPU-s versus 3.151 s / 21.081 CPU-s, or 0.424x wall and 0.389x CPU. Controlled q19 is CPU-neutral (1.009x)
but faster on wall (0.829x), so no implementation change is justified there.

The largest remaining wall-only gaps are ClickBench q05 (2.164x wall with 0.764x CPU) and q06 (3.361x wall with
0.930x CPU), followed by q11 and TPC-DS q09. These point to pipeline utilization/scheduling around distinct
aggregation, not slower Nitro kernels. The largest remaining CPU excesses are TPC-DS q22 (+4.128 s), q67 (+3.596
s), q04 (+3.298 s), and q47 (+1.150 s). Q72 is no longer a regression after completed dynamic-filter admission:
the warmed run is 1.542 s / 4.631 CPU-s for Nitro versus 5.908 s / 23.025 CPU-s for Trino.

The unsuffixed ClickBench Nitro log ends with a failure in an unrelated smoke test that assumes a baseline runner is
present. All 43 requested measurements completed before that failure. Both warmed ClickBench sweeps select only the
performance test and pass.
