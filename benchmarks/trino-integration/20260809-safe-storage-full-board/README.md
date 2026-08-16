# Safe-storage full SQL board

This is the complete 168-variant board after Nitro commit `b34070cd` and Trino integration commit `7eb0b85c`.
Every query passed exact result comparison. Each suite ran in a fresh 12 GiB JVM against the native Nitro Parquet
reader and the same SF10/ClickBench Parquet data, with five warmups, three alternating measurements, an 8 GiB query
limit, thread-allocation accounting, sampled query/heap peaks, boundary counters, and per-plan-node operator CPU
metrics. No JFR, heap dump, or Kata artifact was created.

| suite | queries | wall geomean | CPU geomean | allocation geomean | wall wins | CPU wins | allocation wins |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.735x | 0.692x | 0.181x | 21 | 22 | 22/22 |
| TPC-DS | 103 | 0.585x | 0.406x | 0.186x | 92 | 101 | 101/102 valid |
| ClickBench | 43 | 0.770x | 0.596x | 0.259x | 34 | 41 | 41/41 valid |

Weighted wall/CPU ratios are 0.699x/0.733x for TPC-H, 0.677x/0.564x for TPC-DS, and 0.697x/0.667x for
ClickBench. Maximum p50 sampled query peaks are 988.5 MiB Nitro versus 1,084.3 MiB Trino on TPC-H q09,
1,985.7 MiB Nitro on TPC-DS q23a versus Trino's suite maximum of 1,677.5 MiB on q23b, and 4,205.4 MiB Nitro
versus 4,768.7 MiB Trino on ClickBench q33.

The current plan-node board accounts for 99.93%, 99.85%, and 99.87% of Nitro query CPU in TPC-H, TPC-DS, and
ClickBench. Its operator-family ratios are 0.232x/0.436x/0.670x for TPC-H aggregation/join/scan-project,
0.603x/0.370x/0.427x for TPC-DS, and 0.460x aggregation plus 0.217x scan-project for ClickBench. The query CPU
ratios therefore come from the measured execution operators rather than unaccounted adaptation or harness work.

The safe raw-storage recycling change preserves the board while materially improving ClickBench allocation. Against
the preceding post-affine board, ClickBench moves from 0.789x/0.622x/0.330x wall/CPU/allocation geomeans to
0.770x/0.596x/0.259x. TPC-H and TPC-DS remain close to their preceding 0.711x/0.685x and 0.575x/0.403x wall/CPU
geomeans; their small wall shifts are within the suite-order variation already seen in isolated controls.

The high-value integrated successes remain visible. ClickBench q29 measures 0.329x wall / 0.337x CPU, q30 measures
0.036x / 0.012x, and q23 measures 0.510x / 0.517x. These SQL shapes preserve the corresponding grouping and fused
pipeline advantages rather than losing them at the Trino boundary.

The raw board's CPU candidates were TPC-DS q20 (1.247x), TPC-DS q23a (1.113x), ClickBench q40 (1.128x), and
ClickBench q19 (1.052x). Fresh current-build five-warmup/five-measurement controls clear three of them: q20 is
0.102x CPU, q23a is 1.015x CPU / 0.958x wall, and q19 is 1.013x CPU / 0.769x wall. Q40 remains the only
reproducible CPU edge at 1.075x, with a noisy 1.516x wall ratio. This agrees with its earlier lifecycle and
memory-translation closeout rather than identifying a slower filter, grouping, or TopN kernel. Large wall-only losses
on short ClickBench q01/q41/q42/q43 occur despite much lower Nitro CPU and therefore point to fixed
scheduling/driver latency, not slower kernels.

Thread-allocation deltas wrapped across thread retirement for TPC-DS q39a, ClickBench q14, and Trino ClickBench q33.
Those rows are retained in the XML and raw logs but excluded from allocation geomeans. The invalid signed means seen
on some rows are likewise not treated as allocation measurements.

Files:

- `tpch.xml`, `tpcds.xml`, and `clickbench.xml`: structured complete reports, including operator and boundary metrics.
- `tpch.log`, `tpcds.log`, and `clickbench.log`: raw suite output.
- `sql-shape-operator-board.csv` and `sql-shape-operator-board.md`: current per-plan-node CPU reconciliation.
- `tpcds-q20-isolated.log`, `tpcds-q23a-isolated.log`, `clickbench-q19-isolated.log`, and
  `clickbench-q40-isolated.log`: current-build paired controls for the raw CPU candidates.

## Value-driven remote-output follow-up

Stage-aware reconciliation identified TPC-DS q22's stage-3 `PartitionedOutputOperator` as a structural regression:
Nitro spent about 4.30 CPU-seconds repartitioning 26.52 million encoded positions, versus about 1.10 seconds for
Trino. Destination-native compaction was being admitted by RLE null/error metadata even though the value dictionaries
did not meet the immutable partition policy's reuse bounds.

Nitro commit `2a38b423` exposes that policy's dictionary decision to the integration boundary. Trino commit
`8d6085f6` admits native destination compaction only for reusable dictionary or RLE value streams; other shapes use
the authoritative partition assignments with the existing buffered Page builders. A five-warmup/three-measurement
paired q22 acceptance records 0.787x wall and 0.695x CPU, with Nitro at 4,734.8 ms wall, 11,180 CPU-ms, and
13,179.6 MiB allocated. A final value-only screen reduces the partition node to about 0.19 CPU-seconds and Nitro
allocation to 12,405.6 MiB. Its p50 wall is 4,763.3 ms; one externally stalled measurement makes its p95 unusable.

The warmed q23a guard remains healthy at 5,818.8 ms wall, 29,105 CPU-ms, 52,871.8 MiB allocated, and a 2,496.8 MiB
sampled query peak. The Nitro full suite passes 1,699 tests with 567 skipped, and 262 focused Trino/Nitro execution
tests pass. Relevant logs are `tpcds-q22-conservative-remote-acceptance.log`,
`tpcds-q22-value-encoding-admission-screen.log`, and `tpcds-q23a-value-encoding-admission-guard.log`. The next-largest
exchange excess also activates correctly: q67 improves from 4,699.3 ms / 16,363 CPU-ms on the publication board to
4,519.8 ms / 14,029 CPU-ms, and its hot partitioned-output node falls from about 459 to 214 CPU-ms. Its guard is
`tpcds-q67-value-encoding-admission-guard.log`.
