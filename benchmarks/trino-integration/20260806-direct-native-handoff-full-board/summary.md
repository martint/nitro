# Direct-native-handoff warmed Nitro/Trino board — 2026-08-06

Configuration: SF10 Parquet inputs, both engines in one runner, five warmups and three measurements per query,
12 GiB JVM heap, and an 8 GiB ClickBench query-memory cap. The raw logs retain operator CPU, thread allocation,
query/heap peak memory, native-source counts, and boundary metrics. TPC-DS q38--q99 was resumed after the direct
handoff ownership correction; all 168 queries completed with correct results.

| Suite | Queries | Nitro/Trino wall geomean | Prior board | Nitro/Trino CPU geomean | Prior board |
|---|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.720x | 0.734x | 0.689x | 0.676x |
| TPC-DS | 103 | 0.579x | 0.607x | 0.390x | 0.410x |
| ClickBench | 43 | 0.874x | 0.872x | 0.673x | 0.668x |
| All suites | 168 | 0.662x | 0.683x | 0.483x | 0.496x |

The overall board improves by about 2.1 wall-ratio points and 1.3 CPU-ratio points. TPC-DS supplies the gain;
TPC-H and ClickBench are effectively stable at suite level. Several TPC-DS queries now reproduce operator-scale
wins end to end: q39a/q39b use about 0.07x Trino CPU, q40/q42/q52 use about 0.09--0.10x, and the heavy q72 uses
0.192x CPU and 0.226x wall.

Maximum measured median query peak memory:

| Suite | Nitro | Trino |
|---|---:|---:|
| TPC-H | q18, 1,007.2 MiB | q09, 1,254.7 MiB |
| TPC-DS | q23a, 2,340.5 MiB | q23a, 1,754.7 MiB |
| ClickBench | q33, 5,921.9 MiB | q33, 5,020.8 MiB |

CPU regressions requiring follow-up:

| Suite | Query | CPU ratio | Wall ratio | Initial classification |
|---|---|---:|---:|---|
| TPC-DS | q20 | 1.171x | below 1x | Short-query CPU sample; rerun isolated. |
| TPC-DS | q22 | 1.149x in sequence | varies | Isolated 5W3M control is 1.063x; SQL shape matches the current operator benchmark. |
| TPC-DS | q23a | 1.059x | 1.163x | Prior 15-warmup control cleared CPU to 0.960x. |
| ClickBench | q05 | 1.008x in sequence | 1.122x | Isolated 5W3M CPU is 0.973x; wall is 1.078x from lower utilization. |
| ClickBench | q12 | 1.014x in sequence | 1.333x | Isolated 5W3M CPU is 0.968x; wall remains 1.229x from lower utilization. |
| ClickBench | q19 | 1.108x in sequence | below 1x | Isolated 5W3M CPU is 0.995x and wall is 0.781x; the sweep CPU row is run-order/GC noise. |
| ClickBench | q40 | 1.023x in sequence | below 1x | Isolated 5W3M CPU is 1.007x (1.013x mean) and wall is 0.790x: measurement parity. |

Wall-only regressions are concentrated in short queries and reduced parallel utilization. TPC-DS q57 is 1.110x
wall with 0.832x CPU; q81 is 1.158x wall with 0.910x CPU; q85 is 1.082x wall with 0.804x CPU. ClickBench has
12 wall regressions but only four CPU regressions. q41/q43 are the clearest fixed-overhead examples: 1.847x and
1.771x wall respectively, while q43 still uses 0.906x CPU and lasts only 34--61 ms.

Fresh 5W3M controls reject two of those TPC-DS wall rows as sequence artifacts: q57 is 0.485x wall / 0.381x CPU,
and q85 is 0.748x wall / 0.634x CPU. q81 is the only reproduced TPC-DS outlier, narrowed to 1.047x wall / 1.045x
CPU. Disabling dynamic filtering expands q81 to 1.309x wall / 1.177x CPU; the enabled path therefore recovers most
of the difference. The remaining roughly four-percent enabled gap is concentrated in its partitioned hash joins and
small wide-key TopN stages, not scanning or exchange partitioning.

ClickBench q12 received a synchronized post-warmup 30-measurement hardware-counter control in separate JVMs. Nitro
uses 0.992x mean query CPU and is 1.074x by median wall (1.019x by mean), so the full-sweep 1.333x wall row is not a
stable regression. Nitro retires more instructions and branches over the whole attachment interval, but sustains
2.89 IPC versus Trino's 2.37 and has the lower branch-miss and L1D-miss rates. The stale standalone operator shape
is an important comparison caveat: it uses `MarkDistinct -> filtered aggregate`, while current optimized SQL uses
`partial distinct -> final distinct -> partial count -> final count` across two hash-distributed fragments.

The sweep encountered major GCs in heap-heavy TPC-DS q64/q78 and several ClickBench queries, especially q23,
q33--q35. Those rows remain valid for correctness and broad CPU attribution, but their wall and thread-allocation
measurements require fresh-JVM controls. Negative ClickBench allocation totals are counter overflow/measurement
artifacts and must not be interpreted as allocation improvements.

The first TPC-DS pass exposed a correctness defect at q38: segmented `DISTINCT` aggregation output was advertised
as safe for immediate adjacent native detachment, but that stronger ownership guarantee did not hold. Commit
`1f8812ee` separates managed local-exchange output support from direct-transfer ownership. Stateful aggregation
keeps the proven Page boundary; stateless pipelines, scans, and joins explicitly advertise independent output
ownership. q38 then passed at 0.572x CPU and 0.691x wall in the warmed resumed sweep.

Raw suite logs are `tpch.log`, `tpcds.log`, `tpcds-q38-q99.log`, and `clickbench.log`. ClickBench controls are
`clickbench-q05-isolated-5w3m.log`, `clickbench-q12-isolated-5w3m.log`, `clickbench-q19-isolated-5w3m.log`, and
`clickbench-q40-isolated-5w3m.log`; q12's plan, driver critical path, and synchronized counters are in the other
`clickbench-q12-*` files. TPC-DS controls are `tpcds-q57-isolated-5w3m.log`, `tpcds-q81-isolated-5w3m.log`,
`tpcds-q81-dynamic-filtering-off-5w3m.log`, and `tpcds-q85-isolated-5w3m.log`.
