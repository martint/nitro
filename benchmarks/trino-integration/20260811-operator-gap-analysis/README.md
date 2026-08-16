# Operator-gap analysis

This directory compares strong SQL-shaped operator results with warmed, fully integrated SF10 Parquet queries. Runs
use JDK 26, a 12 GiB test JVM, task concurrency one, exact result checks when both engines run, and the warmup and
measurement counts encoded in each filename. No JFR or heap-dump artifact is retained.

The principal finding is a dynamic-filter admission regression. A per-file threshold allowed probe scans to run
before useful filters arrived. Replacing it with one deadline shared by every source operator from a factory restores:

| Query | Nitro CPU | Trino CPU | CPU ratio | Nitro wall | Trino wall | Wall ratio |
|---|---:|---:|---:|---:|---:|---:|
| TPC-DS q05 | 650 ms | 2,794 ms | 0.233x | 417 ms | 984 ms | 0.423x |
| TPC-H q17 | 2,619 ms | 6,295 ms | 0.416x | 573 ms | 1,530 ms | 0.374x |
| TPC-H q20 | 894 ms | 2,418 ms | 0.370x | 331 ms | 655 ms | 0.505x |

Q23a/q23b do not share that simple explanation. The archived Aug 2 q23a CPU result was 6.13 seconds after the suite
prefix. At the exact revision, q23a is 27.6 seconds in an isolated five-warmup run but 8.0 seconds after q01--q22.
The next revision, which removed unbounded source waiting, raises the same suite-prefix result to 21.6 seconds. Current
bounded and first-narrowing experiments do not reproduce the old position reduction. Those neutral experiments are
retained as logs only and are not production changes.

The accepted production logs are `tpcds-q05-final-shared-deadline-w3m3.log`,
`tpch-q17-final-shared-deadline-w3m3.log`, `tpch-q20-shared-deadline-w5m5.log`, and
`tpch-q21-shared-deadline-w5m5.log`. Revision and rejected-candidate logs document the q23 attribution.
