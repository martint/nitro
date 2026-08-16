# Rejected cross-batch partial-aggregation admission

A candidate retained the 4,096-row per-batch cardinality sample and extended the initial observation horizon from
4,096 to 16,384 rows. It was reverted after its isolated result failed to reproduce with the analyzed benchmark plan.

All runs used TPC-DS Parquet SF10, a 12 GiB JVM, an 8 GiB query-memory limit, three warmups, and three measurements.

| Query | Prior board wall | Candidate wall | Prior board CPU | Candidate CPU |
|---|---:|---:|---:|---:|
| q47 | 0.986x | 0.510x | 0.837x | 0.432x |
| q22 | 0.897x | 0.849x | 0.830x | 0.704x |
| q04 | 0.599x | 0.476x | 0.513x | 0.519x |
| q67 | 0.915x | 1.069x | 0.903x | 0.906x |

These isolated invocations omitted `trino.test.nitro-tpcds-performance-sweep=true`; the fixture therefore skipped
`ANALYZE` and selected materially different plans. Q47's Trino CPU was 32.1 seconds in isolation rather than 13.0
seconds in the representative full-suite plan.

The analyzed full-suite continuation is retained separately under `20260812-cross-batch-full-board`. Q47 remained
passthrough at each relevant Nitro partial, with approximately 6.599 million input and output rows per pipeline. It
measured 1.038x wall / 0.879x CPU; q22 measured 0.946x / 0.840x. The continuation was stopped after q51 and is not a
complete board. None of the isolated candidate ratios are promoted to the synchronized board or presentation.

The full `io.trino.operator.nitro.Test*` package passed 282 tests with no failures, errors, or skips.
