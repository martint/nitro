# TPC-DS q02 post-layout confirmation

The comparable run used the same analyzed SF10 Parquet catalog and physical plan setup as the synchronized suite, with 10 warmups and 20 alternating measurements in a fresh 12 GiB JDK 26 JVM. Exact results and types matched. Allocation, retained query peak, JVM RSS peak, and per-plan-node operator CPU were enabled.

| Engine | Wall p50 | Query CPU p50 | Operator CPU p50 | Allocation p50 | Query peak p50 |
|---|---:|---:|---:|---:|---:|
| Trino | 182.318 ms | 571 ms | 569.225 ms | 2,191.715 MiB | 39.858 MiB |
| Nitro | 164.251 ms | 357 ms | 356.419 ms | 344.551 MiB | 6.632 MiB |
| Nitro / Trino | 0.901x | 0.625x | 0.626x | 0.157x | 0.166x |

The independently summed operator CPU reconciles with query CPU. Median semantic CPU is 245.2 ms source plus 299.1 ms aggregation for Trino, versus 3.1 ms source plus 326.0 ms aggregation for Nitro. Join output work matches at 315,121 positions per engine. Nitro scan pushdown reduces source output from 43,489,850 to 146,828 positions while preserving the aggregation input required by the SQL plan.

`q02-analyzed-paired-w10m20.log` is the valid confirmation. The earlier `q02-paired-w10m20.log` in this directory is an invalid comparison: the isolated invocation omitted sweep-mode `ANALYZE`, selected a different physical plan, and raised join work from roughly 315 thousand to 43 million positions. It is retained only to document why analyzed plan setup is mandatory for focused TPC-DS measurements.
