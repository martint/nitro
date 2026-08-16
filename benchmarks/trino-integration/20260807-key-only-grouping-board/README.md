# Key-only grouping board

This board measures Trino commit `ba4edf31` (`Use distinct indexes for key-only
aggregation`) against Nitro commit `a315b727` (`Stream key-only grouping through
distinct indexes`). Every suite used a 12 GiB test JVM, five query-specific
warmups, three measured executions, alternating engine order, exact result
comparison, thread-allocation measurement, peak query/heap sampling, and
operator/boundary metrics.

| Suite | Queries | Nitro / Trino wall | Nitro / Trino CPU | Allocation | Weighted wall | Weighted CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-H | 22 | 0.738x | 0.694x | 0.178x | 0.718x | 0.734x |
| TPC-DS | 103 | 0.573x | 0.395x | 0.184x | 0.671x | 0.546x |
| ClickBench | 43 | 0.868x | 0.686x | 0.359x | 0.776x | 0.762x |

The first three ratios are geometric means of per-query p50 ratios. Weighted
ratios divide the sums of p50 wall or CPU time. All 168 query variants passed
exact comparison with zero test failures or skips. Maximum sampled query peak
memory was 1,304.6 MiB Nitro / 1,022.4 MiB Trino for TPC-H, 2,244.9 / 1,744.5
MiB for TPC-DS, and 5,921.9 / 4,827.5 MiB for ClickBench.

The key-only path changes ClickBench q05 from the prior controlled 1.063x wall /
0.979x CPU to 0.829x / 0.819x. In the complete in-sequence board it measures
0.765x wall / 0.821x CPU. The complete ClickBench aggregate improves from the
previous accepted 0.893x / 0.698x board to 0.868x / 0.686x.

TPC-DS remains effectively at its prior 0.563x / 0.389x aggregate. q23a/q23b
reported about 1.17x wall / 1.06x CPU in sequence, but matched fresh-JVM
controls put q23a at 0.959x wall / 0.959x CPU with the key-only path and 0.942x /
0.946x with a temporary admission rollback. The small difference does not
justify changing admission, and the much larger sequence movement is process,
heap, and compilation state rather than a causal q23 regression.

TPC-DS q20's in-sequence 0.884x wall / 1.192x CPU row is likewise not a stable
operator regression. A fresh seven-measurement control reports 0.255x wall /
0.121x CPU because Trino's fresh-process execution moves from roughly 91 ms to
524 ms. A transient-ordering-copy TopN experiment passed correctness but made
the named Nitro TopN phase worse and was fully removed. An adaptive key-only
partial-grouping experiment also left q05 CPU unchanged at 0.819x and was fully
removed because the SQL partial distinct stage is not a state-free physical
program.

The initial ClickBench sweep stopped at q32 after the known nondeterministic
ordering of tied TopN rows differed. `q32-immediate-reproduction.log` passed
immediately, and `clickbench-q32-q43.log` completed the remaining exact board;
q01--q31 in `clickbench.log` remain valid. No JFR, heap dump, or Kata artifact
was created.
