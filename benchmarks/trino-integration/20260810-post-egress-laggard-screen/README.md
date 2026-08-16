# Post-egress laggard screen

This fresh-JVM screen separates repeatable compute gaps from lifecycle rows in the complete sequential board after
`Optimize Nitro remote page adaptation`. All relevant controls use JDK 26, a 12 GiB heap, an 8 GiB query-memory
limit, explicit warmups, exact result comparison, allocation measurement, and heap-dump generation disabled.

| Query | Warmups / measures | Wall ratio | CPU ratio | Allocation ratio | Interpretation |
| --- | ---: | ---: | ---: | ---: | --- |
| TPC-DS q57 | 10 / 5 | 0.497 | 0.406 | 0.486 | The sequential-board 1.426x wall row does not reproduce. |
| TPC-DS q23a | 5 / 3 | 0.907 | 0.978 | 0.732 | The sequential CPU loss clears; no join/aggregation change is justified. |
| TPC-H q16 | 10 / 5 | 1.013 | 1.041 | 0.554 | Small residual CPU edge, not the 1.083x sweep row. |
| ClickBench q40 | 20 / 100 | 1.128 | 1.033 | 0.705 | Reproduces the earlier counter-qualified bounded lifecycle/scheduling edge. |

`tpch-q16.log` is an invocation audit artifact: without the performance-sweep selector the harness ran its default
count query. `tpch-q16-corrected.log` is the q16 result used above.

Q30 plan-node metrics continue to place its isolated CPU excess in two `PartitionedOutputOperator` nodes. A generic
sparse-flat-null direct-array candidate was tested because the join output carries a sparse mask. The existing sparse
word-boundary correctness test passed, but the same 20-warmup/five-measurement q30 protocol remained at 1.125x CPU
versus the 1.131x control, allocation rose slightly, and wall remained noisy. The candidate was removed. This rejects
the null accessor as the remaining structural cause; the next useful boundary must remove or combine partition-output
work rather than tune an unproven inner accessor.
