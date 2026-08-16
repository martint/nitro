# TPC-DS q57 controlled follow-up

The full-sweep q57 wall regression does not reproduce. This paired SF10
Parquet run used JDK 26, a 12 GiB test JVM, five complete warmups, seven
alternating measurements, exact comparison, allocation tracking, and peak
memory sampling.

| Engine | p50 wall | p50 query CPU | p50 allocation | p50 query peak |
| --- | ---: | ---: | ---: | ---: |
| Trino | 4,033.956 ms | 18,682 ms | 30,657.207 MiB | 1,888.305 MiB |
| Nitro | 1,993.115 ms | 8,527 ms | 16,652.582 MiB | 544.347 MiB |
| Nitro / Trino | 0.494x | 0.456x | 0.543x | 0.288x |

The complete-board 1.139x wall row was caused by accumulated suite heap/GC
state, not a persistent Nitro execution cost. The isolated warmed result
reproduces the large operator-level advantage. No production change was made.
No JFR or heap dump was created.
