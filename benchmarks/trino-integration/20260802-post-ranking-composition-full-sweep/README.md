# Post-ranking-composition SQL benchmark board — 2026-08-02

## Result

All 168 persisted-Parquet queries completed through their selected engine: 22
TPC-H, 103 TPC-DS, and 43 ClickBench queries. The broad screen reports the sum
of per-query medians below. Ratios are Nitro divided by Trino; lower is better.

| Suite | Wall ratio | Query CPU ratio |
|---|---:|---:|
| TPC-H | 0.927x | 0.860x |
| TPC-DS | 0.803x | 0.581x |
| ClickBench | 0.887x | 0.804x |

The broad screen used fresh engine JVMs and deliberately favored coverage. Its
cold, low-repetition rows are triage signals, not isolated-query conclusions.
The suite-level `perf` attachments also started after some queries had already
run and therefore must not be compared as if they covered equal query windows.

## Controlled overlays

Suspect rows were repeated after warmup. These controls show that several broad
outliers were startup or ordering noise rather than persistent regressions.

| Query | Nitro/Trino wall | Nitro/Trino query CPU | Conclusion |
|---|---:|---:|---|
| ClickBench q07 | 1.037x | 0.521x | CPU win; small wall noise |
| ClickBench q16 | 1.015x | 0.936x | parity |
| ClickBench q19 | 0.819x | 1.013x | wall win; CPU parity |
| ClickBench q30 | 0.422x | 0.388x | confirmed Nitro win |
| TPC-H q01 | 0.687x | 0.784x | confirmed Nitro win |
| TPC-H q03 | 0.954x | 1.021x | wall win; CPU parity |
| TPC-H q05 | 0.961x | 0.774x | confirmed CPU win |

TPC-H q03 used five warmups and twenty measurements per engine in separate,
verified 16 GB JVMs. Nitro reported 629.424 ms wall and 3,170 CPU-ms at p50;
Trino reported 659.755 ms and 3,106 CPU-ms. A separate 30-measurement control
was similarly close at 0.980x wall and 1.009x mean CPU.

## Synchronized q03 hardware counters

The test harness wrote its PID and waited after warmup. `perf stat` attached to
that PID before the measurement loop was released. Heap dumps were disabled on
both JVMs. The 15-second capture covered essentially the complete 20-query
measurement interval; events were multiplexed at comparable rates.

| Counter | Nitro | Trino | Nitro/Trino |
|---|---:|---:|---:|
| cycles | 399.53 B | 408.47 B | 0.978x |
| instructions | 934.85 B | 937.84 B | 0.997x |
| instructions/cycle | 2.34 | 2.30 | 1.017x |
| branches | 190.41 B | 185.35 B | 1.027x |
| branch misses | 1.927 B | 1.733 B | 1.112x |
| cache misses | 4.884 B | 6.855 B | 0.713x |
| L1 data-load misses | 12.622 B | 13.769 B | 0.917x |
| dTLB-load misses | 0.285 B | 0.201 B | 1.419x |

The hardware result does not support a q03 hot-path regression: Nitro executes
2.2% fewer cycles, essentially the same instruction count, and fewer cache and
L1 data-load misses. The 2.1% query-CPU difference is within the small variance
also seen in the longer control and does not justify a specialized engine path.
The earlier `*-queryphase.perf.csv` q03 files are invalid for comparison because
attachment began late; only the `*-synchronized.perf.csv` files are comparable.

## Operator attribution

The broad instrumented logs aggregate the largest operator families as follows
(CPU-ms; family names differ because each engine reports its own operators):

| Suite | Nitro leading families | Trino leading families |
|---|---|---|
| TPC-H | PipelineSource 28,959.9; AggregationSource 15,724.2; HashJoin 8,304.0; Aggregation 5,786.6; HashBuild 2,469.4 | ScanFilterAndProject 32,302.6; HashAggregation 18,298.7; LookupJoin 15,291.1; HashBuilder 3,123.1 |
| TPC-DS | Aggregation 60,360.2; PipelineSource 53,042.0; HashJoin 44,255.9; AggregationSource 29,602.2; PageProcessor 13,450.0 | ScanFilterAndProject 124,564.6; HashAggregation 114,146.3; LookupJoin 99,907.5; FilterProject 8,603.0 |
| ClickBench | AggregationSource 158,307.3; Aggregation 78,650.0; PipelineSource 59,421.7 | ScanFilterAndProject 194,838.3; HashAggregation 157,102.9; TableScan 12,746.6; AggregationOperator 4,698.5 |

These totals corroborate that the operator-level advantages now survive the SQL
boundary at suite scale. Controlled query overlays remain necessary before
acting on individual cold-screen reversals.

## q03 SQL-shape benchmark correction

The old three-key operator fixture described roughly 64,000 groups, but its
correlated modulo expressions produced only 256 distinct tuples. Nitro commit
`2551a06f` corrects the two- and three-key Cartesian distributions and adds a
TPC-H q03-like case with 16 million rows and 4 million distinct groups.

The default packed triple implementation measured about 394.470 ms/op. Forcing
the generated multi-long alternative measured about 720.931 ms/op, so the
current representation is the appropriate general implementation for this SQL
shape. Generated/adaptive compact-long admission, a separate packed control, a
split-tail loop, batch reservation, a flat three-key table, and VarHandle-based
decimal-state encoding were each tested against integrated q03 and reverted
because they were neutral or worse. No speculative production change remains.

No JFR recording or heap dump was created for this sweep.
