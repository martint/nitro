# Value-encoding admission full SQL board

This is the complete publication sweep after Nitro commit `2a38b423` and Trino integration commit `8d6085f6`.
All 168 query variants passed exact result comparison: 22 TPC-H queries, 103 TPC-DS variants (99 statements), and
43 ClickBench queries. Each suite ran in a fresh 12 GiB JVM against the native Nitro Parquet reader and the same
SF10/ClickBench Parquet data, with five warmups, three alternating measurements, an 8 GiB query limit, thread
allocation accounting, sampled query/heap peaks, boundary counters, and per-plan-node operator CPU metrics. No JFR,
heap dump, or Kata artifact was created.

| suite | queries | wall geomean | CPU geomean | allocation geomean | wall wins | CPU wins | allocation wins |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.725x | 0.686x | 0.181x | 21 | 22 | 22/22 |
| TPC-DS | 103 | 0.575x | 0.393x | 0.185x | 95 | 100 | 100/101 valid |
| ClickBench | 43 | 0.794x | 0.634x | 0.293x | 33 | 40 | 42/43 |

Weighted wall/CPU ratios are 0.708x/0.739x for TPC-H, 0.656x/0.539x for TPC-DS, and 0.687x/0.666x for
ClickBench. Maximum p50 sampled query peaks are 899.2 MiB Nitro versus 1,093.4 MiB Trino on TPC-H q09,
1,971.5 MiB Nitro on TPC-DS q23a versus Trino's suite maximum of 1,687.7 MiB on q23b, and 4,205.4 MiB Nitro
versus 4,715.3 MiB Trino on ClickBench q33. Allocation deltas wrapped for TPC-DS q19 and q39a; those rows remain
in the raw reports but are excluded from the allocation geomean.

The plan-node board accounts for 99.90%, 99.85%, and 99.86% of Nitro query CPU in TPC-H, TPC-DS, and ClickBench.
Its aggregation/join/scan-project family ratios are 0.251x/0.427x/0.668x for TPC-H and
0.558x/0.352x/0.409x for TPC-DS. ClickBench aggregation and scan-project are 0.446x and 0.214x. The end-to-end
ratios therefore remain explained by measured execution operators rather than unaccounted adaptation or harness
work.

The remote-output change activates in the complete board. TPC-DS q67 measures 4,432.8 ms wall and 15,642 CPU-ms
for Nitro versus 5,320.5 ms and 22,020 CPU-ms for Trino. This agrees with the fresh guard and confirms that the
value-encoding admission rule is not specific to q22. The sequential q22 row itself is 5,096.9 ms / 19,200 CPU-ms
versus Trino's 5,309.4 ms / 16,418 CPU-ms, while its fresh paired acceptance is 4,734.8 ms / 11,180 CPU-ms
(0.787x wall / 0.695x CPU). The disagreement is a reproducible long-lived-suite state effect, not a failure of the
new partition admission: the fresh value-only screen puts the hot partition node near 0.19 CPU-seconds.

The other raw CPU candidates have the same treatment. TPC-DS q20 and q23a retain their previously recorded fresh
controls (q20 0.102x CPU; q23a approximately parity) and ClickBench q19 remains approximately parity. ClickBench q40
retains its small reproducible CPU edge. A new raw ClickBench q07 anomaly attributed 1,889 ms to the fused source and
reported 4.45x Trino CPU. A fresh five-warmup/five-measurement run clears it: Nitro is 146.4 ms wall, 225 CPU-ms,
and 130.0 MiB allocated versus Trino's 166.9 ms, 430 CPU-ms, and 628.8 MiB (0.877x/0.523x/0.207x). Substituting
only that confirmed q07 control moves the ClickBench geomeans to 0.768x wall / 0.603x CPU / 0.272x allocation,
essentially the preceding publication board. The raw sequential board remains the source of record rather than being
silently rewritten.

Files:

- `tpch.xml`, `tpcds.xml`, and `clickbench.xml`: complete structured reports.
- `tpch.log`, `tpcds.log`, and `clickbench.log`: raw paired suite output.
- `sql-shape-operator-board.csv` and `sql-shape-operator-board.md`: current per-plan-node CPU reconciliation.
- `clickbench-q07-isolated.log`: fresh-JVM control for the new raw outlier.

## Q40 steady-state closeout

The remaining reproducible CPU edge does not identify a degraded Nitro kernel. Separate engine-only runs used ten
warmups and 500 measurements, with hardware counters attached after the benchmark header and warmups. Nitro measures
181.3 ms wall, 436 CPU-ms, and 1,269.1 MiB allocated per query; Trino measures 160.2 ms, 422 CPU-ms, and
1,807.6 MiB. The ratios are 1.132x wall, 1.035x CPU, and 0.702x allocation.

Nitro and Trino execute at 2.25 and 2.23 instructions per cycle, with 1.57% and 1.71% branch-miss rates. Nitro's
average parallel utilization is lower (4.02 versus 4.23 CPUs), which amplifies the small CPU difference into the wall
gap. This reproduces the earlier q40 counter and allocator-pool closeout: there is no branch, cache, or instruction-
efficiency collapse, and reducing useful retained storage previously worsened CPU. No production change is justified.
The valid artifacts are `clickbench-q40-{nitro-measurement,trino-steady}.perf-stat` and their corresponding 500-run
logs; setup-contaminated attempts were removed.
