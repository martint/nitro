# Controlled current SQL benchmark board

This directory records the first complete three-suite screening sweep after the
generated-registry aggregation and SQL-shape harness corrections.  Every run
uses the Nitro Parquet data, JDK 26, one warmup, three measured iterations, and
reports median wall and process CPU time.  TPC-DS and ClickBench interleave the
two engines in one JVM; TPC-H uses a fresh JVM per engine.

| Suite | Queries | Wall geomean Nitro/Trino | CPU geomean Nitro/Trino | CPU wins | Total CPU Nitro/Trino |
|---|---:|---:|---:|---:|---:|
| TPC-DS | 103 | 0.622 | 0.440 | 92 | 0.636 |
| ClickBench, raw | 43 | 0.895 | 0.716 | 37 | 0.838 |
| ClickBench, controlled q30 overlay | 43 | 0.865 | 0.688 | 38 | 0.736 |
| TPC-H | 22 | 0.802 | 0.741 | 18 | 0.770 |

The raw board is a regression screen, not final attribution.  Shared-heap
history contaminates some late interleaved measurements: ClickBench q30 clears
from 2.189x CPU to 0.389x in its accepted fresh-process control.  TPC-DS q22
similarly clears from 1.156x CPU to 0.777x in the isolated 4 GB no-spill control.
TPC-DS q67 previously measured at 0.981x CPU in isolation.  Allocation deltas
in the interleaved logs can become negative when worker threads retire and are
therefore screening data only.

The largest reproducible CPU regression on this board is TPC-H q16.  A fresh
targeted run measured Nitro/Trino at about 1.394x CPU.  Its old standalone
benchmark used a fused distinct-count shape that did not match Trino.  After
changing both standalone harnesses to Trino's four physical aggregation stages,
Nitro still wins there at 0.755x wall time and 0.119x allocation.  The remaining
gap is therefore in SQL integration: the distributed partial aggregation sees
small flat string-vector batches, while the corresponding standalone operator
chain sees larger dictionary-encoded batches.

Raw logs:

- `tpcds-both.log`: both engines interleaved, 12 GB heap, 4 GB query memory.
- `clickbench-both.log`: both engines interleaved, 12 GB heap, 3 GB query memory.
- `tpch-trino.log` and `tpch-nitro.log`: separate fresh processes.
