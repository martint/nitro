# ClickBench q05 key-only grouping gate

This gate validates the general routing of state-free, key-bearing aggregation programs through Nitro's distinct-key
indexes. It uses the ClickBench Parquet data, JDK 26, a fixed 12 GiB heap, paired engine ordering, five warmups, seven
measurements, thread-allocation accounting, and query/heap peak-memory sampling.

| engine | p50 wall ms | p50 CPU ms | allocation MiB | query peak MiB |
|---|---:|---:|---:|---:|
| Nitro | 664.350 | 2,447 | 2,478.570 | 138.366 |
| Trino | 801.388 | 2,987 | 6,463.684 | 288.000 |
| Nitro / Trino | 0.829 | 0.819 | 0.383 | 0.480 |

The prior controlled result was 1.063x wall and 0.979x CPU. The improvement confirms that the SQL final distinct
stage had been using generic grouped aggregation while the operator fixture used the optimized distinct-key path.
Exact results pass. `controlled.log` contains the complete command and raw output. No JFR, heap dump, or Kata artifact
was created.
