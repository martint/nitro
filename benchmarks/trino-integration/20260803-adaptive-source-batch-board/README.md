# Adaptive source-batch SQL board

This refresh uses JDK 26, the native Parquet datasets, a 12 GiB test heap, one warmup, three measured iterations,
and the committed adaptive 10K-to-40K Nitro Parquet host-boundary policy. The retained Trino rows from the controlled
current board are the denominator. TPC-DS and ClickBench ran Nitro-only in fresh JVMs so their long sequential sweeps
did not also retain a baseline engine in the same heap.

| Suite | Queries | Wall geomean Nitro/Trino | CPU geomean Nitro/Trino | CPU wins | Total CPU Nitro/Trino |
|---|---:|---:|---:|---:|---:|
| TPC-DS, controlled | 103 | 0.496 | 0.365 | 103 | 0.526 |
| ClickBench, controlled | 43 | 0.700 | 0.567 | 43 | 0.699 |
| TPC-H, controlled | 22 | 0.742 | 0.700 | 20 | 0.740 |
| All suites | 168 | 0.571 | 0.445 | 166 | 0.621 |

The TPC-DS sweep initially exposed a correctness defect at the interaction between adaptive batch sizing and deferred
filtered payload. Commit `42123803` confines deferred payload to the current public batch rather than the policy's
eventual capacity. The accepted sweep is entirely after that fix and passes all 103 query variants.

The ratio CSVs apply only established run-order controls. TPC-DS q04 and q22 use the accepted paired controls in the
dynamic-filter board; q67 uses the paired 20,680/21,051 CPU-ms control. The raw sequential measurements for those
queries remain in `tpcds-nitro.log`. ClickBench q30 again suffered its known late-sweep heap-history failure
(45,991 CPU-ms); a fresh two-warmup/five-measurement process reports 8,098 CPU-ms and 1,346 ms median wall, which is
the value used in `clickbench-controlled-ratios.csv`.

Fresh paired controls clear the material residual screen values too. TPC-DS q15 is 0.166x CPU and q78 is 0.738x;
ClickBench q19 is 0.974x CPU and 0.773x median wall. Their logs are retained beside the board. The only remaining
screen rows also clear: TPC-DS q87 is 0.674x CPU/0.803x wall and ClickBench q14 is 0.958x CPU/0.838x wall. Thus every
controlled TPC-DS and ClickBench query is a CPU win. Only the two small TPC-H CPU differences below remain, both with
material wall wins. No JFR, heap dump, or other profiling artifact was created.

Fresh paired two-warmup/five-measurement controls also clear the larger TPC-H screen values. Q03 is 3,148 versus
3,029 mean CPU-ms (1.039x) while Nitro is faster on median wall (650 versus 719 ms). Q16 is 740 versus 707 mean
CPU-ms (1.047x) while Nitro is again faster on median wall (346 versus 457 ms). Both are small CPU differences and
strong wall wins; neither justifies changing the already-faster standalone operators. The paired logs are retained
as `tpch-q03-paired.log` and `tpch-q16-paired.log`.
