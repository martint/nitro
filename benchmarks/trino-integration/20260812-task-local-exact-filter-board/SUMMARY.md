# Synchronized three-suite board after task-local exact join filtering

This capture runs all 168 TPC-H SF10, TPC-DS SF10, and ClickBench queries over
persisted Parquet data with three warmups and three measured iterations. Both
engines use the same SQL, data, query memory limits, task concurrency, and JDK.
Result equality is checked before measurement.

| Suite | Queries | Wall geomean | CPU geomean | Weighted CPU | Allocation geomean | Weighted allocation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-H | 22 | 0.752x | 0.710x | 0.757x | 0.226x | 0.365x |
| TPC-DS | 103 | 0.555x | 0.383x | 0.536x | 0.183x | 0.286x |
| ClickBench | 43 | 0.789x | 0.567x | 0.628x | 0.255x | 0.308x |
| Overall | 168 | 0.632x | 0.459x | 0.598x | 0.204x | 0.303x |

Nitro wins 149 of 168 wall-time medians and 167 of 168 CPU medians. The sole
CPU result above parity is ClickBench q12 at 1.007x (996 versus 989 CPU-ms),
which is within the iteration-level noise and reverses when comparing means.

## Operator CPU by semantic family

Operator CPU sums add-input, get-output, and finish CPU per operator and
iteration. Per-query family values are medians across the three measured
iterations, then summed across queries.

| Family | Trino CPU | Nitro CPU | Ratio | CPU saved | Share of net saving |
| --- | ---: | ---: | ---: | ---: | ---: |
| Scan / filter / project / aggregation | 606.9 s | 377.2 s | 0.622x | 229.7 s | 74.4% |
| Join and build | 114.9 s | 41.5 s | 0.361x | 73.4 s | 23.8% |
| Sort / TopN / window | 10.5 s | 2.4 s | 0.224x | 8.1 s | 2.6% |
| Exchange and output | 33.4 s | 35.0 s | 1.048x | -1.6 s | -0.5% |

Scan, filter, projection, and aggregation are intentionally one family. Nitro
fuses these stages into source pipelines, so splitting the family would assign
the same work to different labels in the two engines. Exchange/output is the
only material family above parity and is the clearest next integration target.

The full query board is in `board.csv`, per-query operator CPU is in
`operator-cpu-all.csv`, and the family aggregation is in `operator-family.csv`.
The AWK builders document the aggregation rules.
