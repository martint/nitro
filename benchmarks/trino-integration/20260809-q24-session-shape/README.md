# ClickBench q24 host-driven operator shape

The q24 operator benchmark was aligned with the SQL source pipeline in four
steps. Each partial split now uses `TopNSession`, feeds every native scan batch
through a persistent `BatchFeedOperator`, and constructs the exact
`NitroParquetBatchSource` plus general source-ingress chain used at the Trino
Hive boundary. The final shape also divides the 100 files into the same 120 MiB
ranges as Hive, producing 165 source pipelines.

Both runs used one fork, three warmups, five measurements, the GC profiler, and
a 12 GiB heap:

- host-driven `TopNSession`: 16755.770 ms/op and 29,524,304,432 B/op
- `BatchFeedOperator` plus `TopNSession`: 16668.729 ms/op and 29,527,101,830 B/op
- direct `NitroParquetBatchSource` plus general ingress: 16637.099 ms/op and
  29,590,140,400 B/op
- direct source plus 165 Hive-shaped ranges: 17341.644 ms/op and
  32,624,828,630 B/op
- prior pull-based corrected benchmark: 16739.133 ms/op and 29,613,139,717 B/op

The whole-file variants are equivalent for CPU/time purposes. The exact range
shape adds 704.5 ms/op and 3.03 GB/op, proving that split fragmentation is a
material part of the SQL/operator difference. The controls rule out the
host-driven TopN session, native batch feed, compatibility scan facade, and
source-ingress implementation. The remaining gap is dominated by concurrent
Trino source-driver execution; the matched controls are documented in
`../20260809-q24-driver-concurrency/README.md`.

No JFR, heap dump, or Kata review artifacts were created.
