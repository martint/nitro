# TPC-DS q23a controlled follow-up

The complete-board q23a wall ratio did not reproduce in a controlled run.
This paired SF10 Parquet run used JDK 26, a 12 GiB test JVM, five complete
warmups, five alternating measurements, exact comparison, allocation and peak
memory tracking, and operator/boundary metrics.

| Engine | p50 wall | p50 query CPU | p50 allocation | p50 query peak |
| --- | ---: | ---: | ---: | ---: |
| Trino | 6,205.155 ms | 27,939 ms | 72,532.707 MiB | 2,382.816 MiB |
| Nitro | 5,970.089 ms | 27,313 ms | 48,939.476 MiB | 2,595.586 MiB |
| Nitro / Trino | 0.962x | 0.978x | 0.675x | 1.089x |

The earlier full-sweep 1.056x wall ratio was caused by suite-order/heap-state
noise rather than a persistent execution regression. Nitro is faster on both
wall and query CPU in the warmed control and allocates about 32.5% fewer bytes.
No production change was made. No JFR or heap dump was created.
