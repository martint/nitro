# Post-TopN full benchmark sweep (2026-08-04)

This directory records the first complete three-suite sweep after applying the
columnar ordering-buffer representation to both host-driven and pull TopN.
Every suite uses the native Nitro Parquet reader, JDK 26, task concurrency one,
a 12 GiB test JVM, one warmup, one measurement, allocation tracking, and no JFR
or heap-dump output. Ratios are Nitro / Trino; lower is better.

| Suite | Queries | Wall geomean | CPU geomean | Total wall | Total CPU | CPU wins | Allocation total |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.766 | 0.751 | 0.751 | 0.790 | 17 | 0.288 |
| TPC-DS | 103 | 0.605 | 0.419 | 0.700 | 0.590 | 98 | 0.294* |
| ClickBench | 43 | 0.898 | 0.706 | 0.796 | 0.809 | 38 | 0.379* |
| Overall | 168 | 0.690 | 0.517 | 0.748 | 0.709 | 153 | 0.327* |

`*` Allocation totals exclude five rows whose long-process thread-allocation
delta became negative when worker threads retired. Those counters are not valid
allocation measurements; all nonnegative rows are retained.

Relative to the preceding compact-dictionary board, the overall wall geomean
improves from 0.695 to 0.690, CPU geomean from 0.520 to 0.517, total wall from
0.758 to 0.748, total CPU from 0.716 to 0.709, and CPU wins from 151 to 153.
TPC-DS improves to 98/103 CPU wins. ClickBench q42 retains the intended TopN
result in the long sweep at 0.575x CPU; its 1.159x wall row is another case in
which elapsed time does not track compute.

## Regression screening

Most raw losses are already covered by fresh-process controls:

- TPC-DS q15, q22, q38, and q67 and ClickBench q30 are known long-process
  sequencing/GC artifacts from earlier controls.
- TPC-H q21 again wins wall while losing 6.6% CPU in the long process; its
  multi-measurement fresh control is practical CPU parity at 1.012x.
- TPC-H q16 reproduces at 1.057x CPU but wins wall at 0.978x and allocates
  0.577x as much. Its exact standalone profile does not expose an adapter tax:
  it is itself dominated by the four distributed aggregation/sort stages, and
  its scan/join/anti-join path is not faster than the integrated pipeline.
- A fresh two-warmup, three-measurement ClickBench q40 control clears the raw
  1.140x CPU row to 1.014x mean CPU (1.013x median), while the wall ratio remains
  1.281x and allocation is 0.585x. This is a scheduling/GC-overlap target, not
  justification for changing a Nitro operator hot path.

The attempted q16 join-payload liveness attribution was also neutral within
large run-to-run variance. The SQL pipeline already uses Nitro's prepared
single-long build representation and native Parquet source. Adding cross-
operator projection/liveness machinery is not justified by the remaining 5.7%
CPU difference.

Artifacts:

- `tpch.log`, `tpcds.log`, and `clickbench.log`: complete raw sweeps.
- `clickbench-q40-control.log`: fresh multi-measurement q40 control.
- `tpch-q16-standalone-profile.log`: exact SQL-shaped standalone operator CPU
  and allocation attribution.
