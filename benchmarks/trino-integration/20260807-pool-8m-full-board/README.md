# Eight-MiB vector-pool diagnostic board

This board compares an 8 MiB cap for both the allocator's local and compatible vector pools with the accepted
64 MiB defaults. Runs used fresh 12 GiB JVMs, five warmups, three measurements, SF10 Parquet input, and the committed
Nitro/Trino code preceding the synchronous remote-output borrowing slice.

| Suite | Queries | 8 MiB Nitro/Trino wall | 64 MiB Nitro/Trino wall | 8 MiB Nitro/Trino CPU | 64 MiB Nitro/Trino CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| TPC-H | 22 | 0.707 | 0.729 | 0.684 | 0.700 |
| TPC-DS | 103 | 0.581 | 0.580 | 0.393 | 0.403 |
| ClickBench | 43 | 0.921 | 0.881 | 0.697 | 0.704 |

CPU improved by about 1--2.5% in all three suites. Measured allocation also fell, including from 0.185x to 0.175x
Trino on TPC-H and from 0.188x to 0.181x on the 102 TPC-DS variants with valid nonzero allocation samples. ClickBench
wall nevertheless regressed about 4.6% through GC and scheduling variation despite its lower CPU. The 8 MiB cap is
therefore retained as a diagnostic result, not selected as the new default.

The initial ClickBench sweep stopped at q32 on a one-row top-10 result mismatch. q32 passed exact comparison in an
immediate isolated reproduction and again at the start of a fresh q32--q43 continuation. The complete board combines
q01--q31 from `clickbench.log` with q32--q43 from `clickbench-tail.log`; `clickbench-q32-reproduction.log` preserves
the independent reproduction. No JFR or heap-dump artifact was created.
