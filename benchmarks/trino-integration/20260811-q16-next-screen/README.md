# Post-cardinality next-target screen

This screen tested the remaining small TPC-H q16 CPU edge after representative native-source cardinality admission.
The control used JDK 26, a 12 GiB heap, ten complete warmups, five alternating measurements, exact result comparison,
allocation measurement, plan-node metrics, and disabled heap dumps. Nitro measured 290.027 ms p50 wall / 709.8 mean
CPU-ms / 994.7 MiB allocation versus Trino at 284.773 ms / 683.2 CPU-ms / 1,787.3 MiB: 1.018x wall, 1.039x CPU,
and 0.557x allocation.

Plan-node attribution again localized most of the small excess to the distributed four-key partial DISTINCT. It
receives 1,186,543 rows, emits 1,186,339, and costs about 165.5 CPU-ms in Nitro. A general candidate enabled the
already-qualified 4,096-row initial-cardinality observation for ordinary optional partial aggregations, while
preserving required aggregation and independent duplicated-factory state. Focused tests passed, but q16 retained the
same 1,186,339-row output and the node rose slightly to 169.1 CPU-ms. Whole-query Nitro CPU was unchanged at 709.0
ms and allocation was unchanged. The candidate was removed and the Trino worktree restored to `b29ed68d`.

The current ClickBench q01/q41/q43 rows were also attributed. Each runs 165 source drivers. Nitro uses substantially
less source CPU, but downstream exchange sources remain blocked about 20--50 ms longer, exposing fixed task and
exchange scheduling latency. A proposed ten-warmup/fifty-measurement control was intentionally stopped before it
produced a result after the 12 GiB process entered repeated full-GC cycles. The partial logs are lifecycle evidence,
not performance results. No operator, connector, or Trino scheduling change is justified by this screen.

Files:

- `q16-control.log`: accepted warmed q16 control.
- `q16-candidate.log`: rejected ordinary-partial cardinality candidate.
- `clickbench-q01-latency.log`, `clickbench-q41-latency.log`, and `clickbench-q43-latency.log`: invocation audits
  and deliberately stopped GC-contaminated long controls.

No JFR, heap dump, or Kata artifact was created.
