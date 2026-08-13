# ClickBench q19 post-layout confirmation

This current-binary pair used the analyzed ClickBench Parquet catalog, a fresh 12 GiB JDK 26 JVM, an 8 GiB per-node query-memory limit, five warmups, and ten alternating measurements. Both engines processed 99,997,497 input positions and returned the same result. Allocation, peak memory, and per-plan-node operator CPU were enabled.

| Engine | Wall p50 | Query CPU p50 | Operator CPU p50 | Allocation p50 | Query peak p50 |
|---|---:|---:|---:|---:|---:|
| Trino | 6,767.349 ms | 22,860 ms | 22,858.146 ms | 37,245.606 MiB | 3,218.911 MiB |
| Nitro | 5,152.688 ms | 21,010 ms | 20,982.828 ms | 21,664.341 MiB | 2,809.375 MiB |
| Nitro / Trino | 0.761x | 0.919x | 0.918x | 0.582x | 0.873x |

The remaining CPU is adaptive partial aggregation, not scan work or unequal input. Trino spends a median 3,630 ms in scan, 6,050 ms in partial aggregation, 11,220 ms in final aggregation, and 1,297 ms in partitioned output. Nitro fuses scan and partial aggregation: the stable fully aggregated state spends about 11,160 ms there, 8,244 ms in final aggregation, and 1,436 ms in partitioned output.

The ten Nitro iterations expose the tradeoff directly. Seven fully aggregated iterations consume 99,997,497 rows and emit 57,548,820 partial rows. Two hybrid iterations aggregate about half the input and emit 78–79 million rows; one nearly complete bypass aggregates 140,000 rows and emits 99,943,308 rows. Their combined partial-plus-final-plus-output CPU falls from about 20.8 seconds in the fully aggregated state to 19.1–19.2 seconds in the hybrid states and 18.4 seconds in the near-bypass state. More contraction is therefore not automatically cheaper.

A global lower unique-row threshold is not the solution: prior 0.60/0.70 controls moved all work downstream and worsened wall time. The general next design is a host-owned cost decision that combines observed local aggregation cost per suppressed row with an immutable downstream-row cost supplied from the physical plan. The control must remain independent of query identity, logical types, arity, and table or column names.
