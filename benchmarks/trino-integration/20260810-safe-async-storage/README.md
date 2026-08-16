# Rejected take-only asynchronous exchange-storage reuse

This directory records a rejected ownership experiment. It allowed an asynchronous exchange lease to recycle
untaken borrowed storage while relinquishing any vector tree transferred through `Output.take()`. Focused tests and
isolated query checks passed, but the subsequent full-suite sequence proved that take is not the only lifetime that
crosses exchange-batch close.

All SQL runs used JDK 26, five warmups, three measured executions, a 12 GiB JVM heap, 8 GiB query memory, thread
allocation measurement, query/process peak-memory reporting, boundary and plan-node metrics, and exact result checks.

| Query | Engine | Wall p50 | CPU | Allocation p50 | Query peak p50 |
| --- | --- | ---: | ---: | ---: | ---: |
| TPC-DS q02 | Trino | 893.1 ms | 3,252 ms | 9,511 MiB | 404 MiB |
| TPC-DS q02 | Nitro | 1,155.8 ms | 2,694 ms | 6,155 MiB | 134 MiB |
| TPC-DS q22 | Nitro | 4,757.7 ms | 10,763 ms mean | 10,949 MiB | 315 MiB |
| TPC-DS q23a | Nitro | 5,368.3 ms | 27,303 ms mean | 44,700 MiB | 2,551 MiB |

The isolated q02 run passed, and q22/q23a showed approximately 13% lower allocation than the accepted lease-owned
baseline. Those are diagnostic observations, not admissible results. In the full-suite sequence, q01 first populated
the storage pool and q02 then duplicated 2,513 rows exactly as in the original unsafe experiment. The isolated q02
was a false negative because no matching recycled backing was available for an overwrite.

The complete Nitro suite passed 1,705 tests with zero failures and 567 skipped, and the Trino `TestTrinoNitro*` cohort
passed 248 tests, demonstrating why focused gates were insufficient here. Both candidate code commits were abandoned
and the accepted lease/GC-owned behavior restored. The failed sequence is in the sibling
`20260810-safe-async-storage-full-board/tpcds.log`. No JFR or heap dump was produced for this gate.
