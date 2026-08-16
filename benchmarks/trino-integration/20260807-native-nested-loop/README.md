# Native nested-loop integration gate

TPC-DS q09 validates Nitro's nested-loop kernel at Trino's existing physical
nested-loop boundary. Cross joins remain outside Nitro optimizer islands, and
the local execution planner preserves Trino's one-build-driver/one-probe-driver
topology while substituting Nitro's native build and join factories.

The accepted run used SF10 Parquet inputs, JDK 26, a 12 GiB test JVM, five
complete warmups, three alternating measurements, exact-result comparison,
thread-allocation measurement, and operator CPU metrics. No JFR or heap dump
was created.

| Engine | p50 wall | p50 CPU | mean allocation | query peak | heap peak |
| --- | ---: | ---: | ---: | ---: | ---: |
| Trino | 717.849 ms | 3,653 ms | 12,618.095 MiB | 33.001 MiB | 8,808.611 MiB |
| Nitro | 512.285 ms | 2,201 ms | 335.917 MiB | 0.261 MiB | 7,362.983 MiB |
| Nitro / Trino | 0.714x | 0.603x | 0.027x | 0.008x | 0.836x |

Operator metrics report 15 `TrinoNitroHashBuildOperator` and 15
`TrinoNitroHashJoinOperator` instances. Those adapters select Nitro's
`NestedLoopJoinSession` for this plan; their hash-oriented names are now a
naming mismatch, not an algorithm mismatch. The native join boundary handled
15 probe positions and emitted 15 positions. The complete `TestTrinoNitro*`
core cohort passed 234 tests with no failures, errors, or skips.
