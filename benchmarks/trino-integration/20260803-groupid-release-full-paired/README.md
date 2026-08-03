# GroupId release full paired SQL sweep

This board refreshes all three native-Parquet SQL suites after `4c915e23` fixed
borrowed batch-buffer release in `GroupIdOperator`.  Every accepted run used JDK
26, a 12 GiB test JVM, one warmup, one measured invocation, task concurrency 1,
and paired Nitro/Trino execution. Heap-dump generation was explicitly disabled;
no JFR or heap-dump artifact is retained.

TPC-DS ran with the production default 2 GiB per-node query limit. ClickBench
uses the established 4 GiB suite limit because its high-cardinality grouping
queries exceed the test harness's 1 GiB default in both-engine comparisons.

| Suite | Queries | Raw wall geomean | Raw CPU geomean | CPU wins | Valid allocation geomean | Allocation wins |
|---|---:|---:|---:|---:|---:|---:|
| TPC-H | 22 | 0.759 | 0.708 | 19 | 0.188 | 22/22 |
| TPC-DS | 103 | 0.610 | 0.424 | 100 | 0.212 | 98/100 |
| ClickBench | 43 | 0.874 | 0.710 | 36 | 0.334 | 40/40 |
| All raw rows | 168 | 0.689 | 0.518 | 155 | 0.233 | 160/162 |

The raw board is intentionally a one-measurement, long-process screen. Its
combined total ratios are 0.748 wall, 0.705 CPU, and 0.358 allocation over rows
with valid allocation deltas. Live-thread allocation deltas are invalid when a
measured worker terminates; TPC-DS q22 and three ClickBench rows are therefore
excluded from allocation geomeans rather than interpreted as negative
allocation.

## Controlled board

The accepted controlled board retains the established fresh-process controls
from `20260803-adaptive-source-batch-board`. The only production change relevant
to those controls is GroupId lifecycle ownership, so TPC-DS q22 is replaced by
the new three-measurement default-memory control:

- wall: 4,481.285 / 6,287.504 ms = 0.713;
- CPU: 9,362 / 15,760 ms = 0.594;
- allocation: 9,281.302 / 33,686.337 MiB = 0.276.

| Suite | Controlled wall geomean | Controlled CPU geomean | CPU wins | Total CPU ratio |
|---|---:|---:|---:|---:|
| TPC-H | 0.742 | 0.700 | 20/22 | 0.740 |
| TPC-DS, q22 refreshed | 0.494 | 0.364 | 103/103 | 0.519 |
| ClickBench | 0.700 | 0.567 | 43/43 | 0.699 |
| All suites | 0.570 | 0.444 | 166/168 | — |

The controlled SQL CPU ratios continue to corroborate the frozen SQL-shaped
operator ratios (TPC-H 0.752, TPC-DS 0.561, ClickBench 0.790). TPC-H is slightly
better than its operator ratio, while TPC-DS and ClickBench retain larger SQL
wins; there is no broad operator-kernel regression to justify changing their
implementations.

The long-process screen reproduces the already-attributed history effects:
TPC-DS q22 has an invalid allocation delta and 0.961 CPU in the accumulated JVM
but 0.594 CPU fresh; ClickBench q30 consumes 33,018 CPU-ms in sequence versus the
accepted fresh 8,098 CPU-ms. Both raw rows are retained in the logs.

## Default-memory mismatch found by the sweep

The first ClickBench attempt used the harness's 1 GiB default and stopped at the
first high-cardinality grouping query with 1,022 MiB reserved. The complete 4
GiB rerun passed all 43 queries. That failure also exposed a shutdown-only
`Shared resource is not acquired` warning when a Parquet source lease closed
after its allocator had already been closed. It did not affect accepted query
results, but is retained in `clickbench-default-1gb-failure.log` for the next
lifecycle-hardening slice.
