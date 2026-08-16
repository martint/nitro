# Direct-range join warmed Nitro/Trino board — 2026-08-06

Configuration: SF10 Parquet inputs, both engines in one runner, five warmups and three measurements per query,
12 GiB test JVM heap, and an 8 GiB ClickBench query-memory cap. The raw suite logs retain operator CPU, thread
allocation, query/heap peak memory, native-source counts, and boundary metrics. All 168 queries completed with
correct results.

Nitro commit `3f02722b` admits bounded payload-bearing single-long joins to the existing exact direct-range build.
The full Nitro suite passed before measurement: 1,665 tests, 0 failures or errors, 567 skipped.

| Suite | Queries | Raw wall geomean | Prior board | Raw CPU geomean | Prior board |
|---|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.691x | 0.720x | 0.685x | 0.689x |
| TPC-DS | 103 | 0.568x | 0.579x | 0.393x | 0.391x |
| ClickBench | 43 | 0.882x | 0.874x | 0.690x | 0.673x |
| All suites | 168 | 0.652x | 0.662x | 0.488x | 0.484x |

ClickBench q30's in-sequence row was contaminated after the heap-heavy q23/q33 region: it reported 0.952x CPU,
whereas a fresh 10-warmup/10-measurement control returned 0.304x CPU and 0.317x wall, matching the prior board's
0.303x CPU.

TPC-DS q20 and q22 were also contaminated in sequence. q20's raw 1.204x CPU becomes 0.107x mean CPU and 0.195x
wall in a 15W20M control, allocating 0.53 GiB versus Trino's 7.30 GiB. q22's raw 1.193x CPU becomes 0.649x mean
CPU and 0.736x wall in a 15W20M control, allocating 9.20 GiB versus Trino's 30.69 GiB. Replacing q20, q22, and
q30 with these controls gives the corrected current board:

| Scope | Corrected wall geomean | Corrected CPU geomean |
|---|---:|---:|
| TPC-DS | 0.558x | 0.381x |
| ClickBench | 0.861x | 0.671x |
| All suites | 0.641x | 0.476x |

The target TPC-DS q81 join shape improves in the full sweep from 0.899x to 0.885x CPU. A fresh 5W10M isolated
control reports 0.990x mean CPU (0.993x median), with Nitro allocating 1.397 GiB versus Trino's 2.067 GiB. Its
large payload-bearing dimension builds now take about 26--27 ms instead of about 49 ms; remaining q81 CPU is
primarily the wider query pipeline and small TopN stages rather than hash-build initialization.

Median thread-allocation geomeans are 0.173x Trino for TPC-H, 0.195x for the 95 valid TPC-DS pairs, and 0.318x
for the 40 valid ClickBench pairs. Missing pairs are counter-overflow artifacts and are excluded. Maximum measured
median query peak memory is:

| Suite | Nitro | Trino |
|---|---:|---:|
| TPC-H | q18, 886.4 MiB | q09, 1,118.7 MiB |
| TPC-DS | q23b, 2,279.0 MiB | q23a, 1,828.6 MiB |
| ClickBench | q33, 5,921.9 MiB | q33, 4,627.0 MiB |

No TPC-H query is slower by CPU. Fresh controls clear both raw TPC-DS CPU regressions. The remaining in-sequence
ClickBench rows above parity are q12/q14/q19/q40. Prior isolated controls put q12 at 0.968x, q19 at 0.995x, and
q40 at 1.007x; q14 is a near-parity short query at 1.009x in sequence.

ClickBench q24 repeated at roughly 0.90x CPU in isolation, versus 0.77x on the prior board. Its delta is entirely
inside the native Parquet scan/filter pipeline, not TopN or a Page boundary. A strict parent-artifact control at
`ce2e5a09` was equally slow (29.6 s Nitro CPU versus 30.3 s on the candidate), proving the direct-range join change
is not causal. q24 remains below Trino CPU, but its wide `SELECT *` result has lower parallel utilization and a wall
regression that merits separate scan/code-generation investigation.

Raw logs are `tpch.log`, `tpcds.log`, and `clickbench.log`. Focused controls are
`clickbench-q30-isolated-10w10m.log`, `clickbench-q24-isolated-5w5m.log`, and
`clickbench-q24-parent-ce2e5-5w3m.log`, plus `tpcds-q20-isolated-15w20m.log` and
`tpcds-q22-isolated-15w20m.log`; q81 controls are in the parent benchmark directory as
`q81-direct-range-default-5w3m.log` and `q81-direct-range-default-5w10m.log`.

`current-sql-shape-operator-board.csv` and `.md` reconcile the combined suite logs at the actual distributed
operator boundaries. Attributed operator CPU covers 99.9% of query CPU and totals 0.715x Trino for TPC-H, 0.544x
for TPC-DS, and 0.778x for ClickBench. These raw totals intentionally retain the q20/q22 suite-order contamination;
the focused controls above remain authoritative for those two query rows.
