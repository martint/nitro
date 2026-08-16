# Sparse-exchange complete board

This board measures Trino commit `c82f2235` (`Avoid retaining sparse exchange
generations`). TPC-H and TPC-DS were rerun after the sparse-exchange change;
the ClickBench rows are the complete same-commit chunked sweep from
`../20260807-sparse-exchange-board`. All runs used the 100-million-row Parquet
data, a 12 GiB test JVM, five query-specific warmups, three alternating
measurements, exact result comparison, thread-allocation measurement, sampled
query and heap peaks, operator CPU, boundary counters, and aggregation-phase
metrics. No JFR, heap dump, or Kata artifact was created.

| Suite | Queries | Nitro / Trino wall | Nitro / Trino CPU | Allocation | Weighted wall | Weighted CPU | Wall wins | CPU wins | Maximum sampled query peak MiB (Nitro / Trino) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-H | 22 | 0.724x | 0.678x | 0.177x | 0.713x | 0.723x | 22/22 | 22/22 | 1,032.3 / 1,109.2 |
| TPC-DS | 103 | 0.559x | 0.386x | 0.180x\* | 0.653x | 0.535x | 94/103 | 99/103 | 2,361.6 / 1,750.0 |
| ClickBench | 43 | 0.838x | 0.658x | 0.331x\* | 0.732x | 0.715x | 30/43 | 41/43 | 5,921.9 / 4,718.6 |
| **All queries** | **168** | **0.641x** | **0.476x** | **0.210x\*** | **0.694x** | **0.634x** | **146/168** | **162/168** | **5,921.9 / 4,718.6** |

The first three ratios are geometric means of per-query p50 ratios. Weighted
ratios divide sums of p50 values. The allocation mean excludes two TPC-DS rows
and one ClickBench row whose process-wide thread-allocation delta became
negative when worker threads terminated, leaving 101/103, 42/43, and 165/168
valid allocation comparisons respectively.

All 168 query variants passed exact comparison, with no failures, errors,
skips, or OOMs. Relative to the preceding complete board, TPC-H improves from
0.738x wall / 0.694x CPU to 0.724x / 0.678x, while TPC-DS improves from 0.573x
/ 0.395x to 0.559x / 0.386x. ClickBench remains the already-qualified
same-commit 0.838x / 0.658x result.

TPC-H has no wall or CPU losses. TPC-DS q20 and q22 reported 1.184x and 1.025x
CPU in the long in-sequence sweep, but fresh-JVM controls classify both as
process-order effects. The new five-measurement q22 control is 0.749x wall,
0.645x CPU, and 0.342x allocation. The previously recorded fresh q20 control
is 0.255x wall / 0.121x CPU. q30, q57, and q81 again show wall-only sequence
movement while retaining CPU wins.

TPC-DS q23a/q23b are not stable CPU regressions. Fresh post-change controls
with five warmups and five measurements put q23a at 1.002x wall / 1.008x CPU
and q23b at 0.881x / 0.925x; Nitro allocation was 0.678x and 0.654x
respectively. Their higher Nitro query-memory peaks remain a separate memory
issue: the fresh controls measured 2,851.8 / 2,333.6 MiB for q23a and 2,843.4
/ 2,504.8 MiB for q23b. Earlier phase attribution localized that reservation
to dictionary-backed intermediate composite-key transport, but it is not
currently an execution-time regression.

ClickBench's remaining board CPU losses are q19 and q40. Fresh controls put
q19 at 1.013x CPU with 0.797x wall and q40 at effective CPU parity (1.009x);
the other wall losses are dominated by fixed latency on short queries. The
suite's maximum sampled query memory is q33 at 5,921.9 MiB Nitro versus
4,718.6 MiB Trino. This peak-reservation result coexists with a 0.331x
allocation mean and should not be interpreted as allocated-byte growth.
Replacing packed full-width pair identity with adaptive multi-long grouping
increased q33 peak to 8,651.9 MiB, allocation by 26.8%, and wall time by 7.6%.
Capping generic grouped-state growth had no effect because the registered
aggregation units already request exact capacity and use incremental storage.
Both candidates were rejected; details are in `../20260807-q33-memory`.

`tpcds.log` and `tpch.log` are the complete refreshed sweeps.
`tpcds-q22-isolated.log` is the fresh control used to classify q22. The four
ClickBench range logs and its isolated controls remain in
`../20260807-sparse-exchange-board`. The post-change q23 controls are in
`../20260807-post-sparse-q23`.
