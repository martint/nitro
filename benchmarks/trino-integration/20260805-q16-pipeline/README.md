# TPC-H q16 final-distinct investigation

This directory records the fresh-JVM investigation of the remaining warmed q16 CPU gap after native Parquet scan
and native single local exchange.

Whole-stage plan-node attribution corrected the initial hypothesis: Nitro's scan, lookup join, anti-join, partial
aggregation, and partition output stage was already faster than the equivalent Trino stage. The residual was the
distributed four-key final DISTINCT, which receives 1,186,564 rows and removes only 225 duplicates.

The packed-four-field and generalized blocking-identity candidates were rejected. Generated dictionary-hash
activation was also inapplicable after exchange flattening. The retained change reuses the existing exact two-long
normalized identity as the flat record for eligible LONG/BINARY layouts, avoiding redundant copies of interned
binary bytes into the variable-width record arena. Ordinary record hashing, fallback equality, and grouped-output
materialization remain intact.

The adjacent five-warmup/seven-measurement gate was:

| configuration | Nitro p50 wall | Nitro mean CPU | final DISTINCT input CPU | Nitro allocation |
| --- | ---: | ---: | ---: | ---: |
| normalized record copy (control) | 305.127 ms | 719.714 ms | about 160 ms | 948.769 MiB |
| normalized id-backed record | 288.858 ms | 656.857 ms | about 120 ms | 950.456 MiB |

Against its paired Trino measurements, the enabled leg was 0.936x median wall, 0.948x mean CPU, and 0.533x
allocation. A separate five-warmup/three-measurement confirmation reported 0.945x wall, 0.940x CPU, and 0.533x
allocation. One additional enabled process exhibited a repeatable 7.7-second idle interval with unchanged 660 ms
CPU and unchanged plan-node work; the next fresh process did not reproduce it, so that wall row is retained as an
environmental outlier rather than used as optimization evidence.

The symbolized perf capture also showed that two warmups left roughly 14% of process samples in C2 compilation.
The acceptance gates therefore use five complete warmups. No JFR or heap dump was created.
