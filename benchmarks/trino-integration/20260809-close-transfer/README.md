# Allocator close-transfer experiment

ClickBench q24 was run in separate fresh 12 GiB JVMs with five warmups and five
measurements, using the native Nitro Parquet source, 165 split-local pipelines,
thread-allocation accounting, peak-memory sampling, and per-plan-node CPU.

The candidate transferred every idle allocator-local vector to the explicitly
owned engine `PrimitiveArrayPool` during `Allocator.close()`. A focused test
proved cross-allocator identity reuse, but q24 rejected the design:

| implementation | wall p50 ms | mean CPU ms | allocation p50 MiB | peak p50 MiB |
| --- | ---: | ---: | ---: | ---: |
| accepted | 5278.816 | 21376 | 36321.298 | 222.847 |
| close transfer | 5258.487 | 21888 | 37651.382 | 228.585 |

The exact-capacity owner pool has insufficient hit rate for split-local,
variable-capacity vector working sets; synchronized transfer/lookup adds work
without avoiding enough allocation. Production and test changes were restored,
and the accepted Nitro artifacts were reinstalled.

No JFR, heap-dump, or Kata artifact was created.
