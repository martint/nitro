# TPC-DS q30/q81 isolated controls

Fresh 12 GiB JVMs ran each query independently against the SF10 Parquet data. Each backend received five complete
warmups followed by five measured iterations. Results were exact and include allocation, peak-memory, operator, and
per-plan-node metrics. No JFR or heap-dump artifact was created.

| Query | Nitro/Trino wall p50 | Nitro/Trino CPU p50 | Trino allocation | Nitro allocation |
|---|---:|---:|---:|---:|
| q30 | 1.064x | 0.998x | 1,626.9 MiB | 1,472.6 MiB |
| q81 | 0.986x | 0.957x | 2,083.9 MiB | 1,551.1 MiB |

Neither previous full-board wall laggard reproduces as a CPU regression. q81 is faster on both wall and CPU. q30 has
CPU parity and a small median wall gap, while one 459.7 ms Nitro sample raises its p95 and mean; this is a short-query
latency-tail question rather than evidence that a Nitro operator is doing materially more CPU work. The earlier board
ratios (q30 1.256x wall and q81 1.413x wall) must not be used as current optimization targets.

`q30-control.log` and `q81-control.log` contain the complete measurements and diagnostics.
