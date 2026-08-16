# Post-affine full SQL board

This is the complete 168-variant board after Nitro commit `76e9ab5b` and Trino integration commit `b91d5a45`.
Every query passed exact result comparison. Each suite ran in a fresh 12 GiB JVM against the native Nitro Parquet
reader and the same SF10/ClickBench Parquet data, with five warmups, three alternating measurements, an 8 GiB query
limit, thread-allocation accounting, sampled query/heap peaks, boundary counters, and per-plan-node operator CPU
metrics. No JFR, heap dump, or Kata artifact was created.

| suite | queries | wall geomean | CPU geomean | allocation geomean | wall wins | CPU wins | allocation wins |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.711x | 0.685x | 0.182x | 22 | 22 | 22/22 |
| TPC-DS | 103 | 0.575x | 0.403x | 0.187x | 93 | 101 | 100/101 valid |
| ClickBench, raw | 43 | 0.789x | 0.622x | 0.330x | 34 | 40 | 39/41 valid |
| ClickBench, q07-reconciled | 43 | 0.768x | 0.591x | 0.306x | — | — | 40/41 valid |

Weighted wall/CPU ratios are 0.695x/0.729x for TPC-H, 0.657x/0.545x for TPC-DS, and 0.704x/0.675x for raw
ClickBench. Replacing only ClickBench q07 with its fresh five-warmup/five-measurement control changes the weighted
ratios to 0.701x/0.671x. Two TPC-DS Trino allocation deltas (`q39a`, `q80`) and three ClickBench deltas (`q14` on
both engines and Trino `q33`) wrapped across thread retirement and are retained but excluded from allocation
geomeans.

Current-build five-warmup/five-measurement controls also clear the two high-absolute-CPU TPC-DS history rows. Q22 is
0.756x wall / 0.659x CPU / 0.342x allocation; q23a is 0.924x / 0.969x / 0.664x. Replacing only those two rows gives
TPC-DS 0.573x wall / 0.401x CPU / 0.186x allocation geomeans and 0.646x/0.538x weighted wall/CPU. Q23a retains the
known higher peak-query-memory shape (2,524.7 versus 2,396.7 MiB p50), but its raw CPU loss does not reproduce.

The full-history ClickBench q30 result is the decisive change. Nitro measures 102.690 ms p50 wall / 264.0 mean
CPU-ms / 235.697 MiB p50 allocation versus Trino's 3,078.510 / 20,845.3 / 70,386.315: 0.033x wall, 0.0127x CPU,
and 0.0033x allocation. All 165 Nitro splits use the selected affine-INTEGER physical aggregation. The old
long-lived-JVM degradation is gone; the integrated result now reproduces the SQL-shaped operator result even after
q01--q29.

The raw ClickBench q07 row was not a kernel regression. It reported 1,916.7 CPU-ms and 3,149.9 MiB allocation, while
the preceding complete boards were near 380 CPU-ms and 138 MiB. A fresh paired control gives Nitro 143.842 ms /
224.2 CPU-ms / 132.160 MiB versus Trino 121.604 / 427.8 / 626.904: 1.183x wall, 0.524x CPU, and 0.211x allocation.
The reconciled line above substitutes only this proven process-state outlier.

The remaining reproducible ClickBench CPU edge is q40. The paired five-measurement control is 448.6 versus 428.2
mean CPU-ms (1.048x), while Nitro wins wall at 217.354 versus 284.856 ms p50 and allocation at 1,106.5 versus
1,813.8 MiB. A synchronized 50-measurement query-phase perf capture reports Nitro/Trino at 1.135x task-clock,
1.138x cycles, 1.149x instructions, 1.177x branches, 1.094x branch misses, 1.059x cache misses, 1.129x L1-data
misses, and 1.750x dTLB misses. This agrees with the earlier q40 closeout: the small difference is distributed
lifecycle/primitive-pool and memory-translation work, not a slower filter, CASE, grouping, or TopN kernel. The query
still has lower allocation and no retained vector or exchange leak, so no operator rewrite is justified by this row.

Maximum p50 sampled query peaks are 1,183.4 MiB Nitro versus 1,132.0 MiB Trino on TPC-H q09, 1,997.4 MiB Nitro on
TPC-DS q23a versus Trino's suite maximum of 1,718.6 MiB on q23b, and 4,205.4 MiB Nitro versus 4,736.4 MiB Trino on
ClickBench q33. Full-heap GCs remain visible around TPC-DS q22/q23/q64 and ClickBench q24/q33; host RAM stayed
uncontended and swap did not materially grow.

Files:

- `tpch.xml`, `tpcds.xml`, `clickbench.xml`: structured complete reports, including operator and boundary metrics.
- `tpch.log`, `tpcds.log`, `clickbench.log`: raw suite output.
- `clickbench-q07-isolated.log`, `clickbench-q40-isolated.log`: fresh paired controls.
- `tpcds-q22-isolated.log`, `tpcds-q23a-isolated.log`: current-build paired controls for the high-CPU history rows.
- `q40-{nitro,trino}-perf.csv`: synchronized 50-measurement hardware counters.
- `q40-{nitro,trino}-perf.log`: corresponding measured SQL rows.
- `q40-{nitro,trino}-perf-5.*`: retained short-window controls demonstrating fixed teardown contamination.
