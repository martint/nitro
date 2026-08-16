# Remote flat-buffer borrowing experiments

## Rejected variable-width candidate

This candidate exposed dense integer and binary Nitro buffers directly to Trino's remote-output destination appender
during its synchronous callback. Focused adapter tests and 249 broader Nitro integration tests passed. Isolated
five-warmup/three-measurement controls were initially favorable: TPC-DS q23a measured 1.044x wall / 1.006x CPU,
q22 measured 0.752x / 0.652x, and ClickBench q12 measured 1.186x / 0.991x. The full 22-query TPC-H board completed at
0.725x wall / 0.680x CPU versus the accepted 0.729x / 0.700x.

The full TPC-DS gate rejected the design at q31. Nitro returned 286 rows versus Trino's 288 and changed multiple
aggregate values. After abandoning candidate commit `9ffca0f8`, reinstalling accepted commit `ad800644`, and running
q31 in a fresh JVM, exact comparison passed. The synchronous callback does not prove that every destination appender
has finished reading a flat input block. Future work needs either a proven eager-copy appender API or an ownership
lease that extends across buffered destination state.

`tpcds.log` contains the failure after q01--q30. `tpcds-q31-reverted-control.log` is the passing restored control.
No JFR or heap-dump artifact was created.

## Accepted fixed-width candidate

The narrower implementation borrows only dense I32/I64 arrays. Variable-width, CHAR, dictionary, and RLE blocks all
retain the accepted owned-copy behavior. q31 passed both in isolation and after a complete q01--q30 sequence. Split
q01--q31 and q32--q99 logs cover all 103 TPC-DS variants with exact comparison.

| Suite | Queries | Fixed-width Nitro/Trino wall | Accepted wall | Fixed-width Nitro/Trino CPU | Accepted CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| TPC-H | 22 | 0.700 | 0.729 | 0.681 | 0.700 |
| TPC-DS | 103 | 0.583 | 0.580 | 0.393 | 0.403 |
| ClickBench | 43 | 0.871 | 0.881 | 0.684 | 0.704 |

The final implementation passed 249 focused Trino/Nitro tests and was committed as `fa62d46b`. The original
`tpch.log` belongs to the rejected variable-width candidate; `tpch-fixed-width.log` is the accepted measurement.
`clickbench-fixed-width.log` is the complete accepted ClickBench board.
