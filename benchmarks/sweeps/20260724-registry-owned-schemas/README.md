# Registry-owned logical schema validation

This directory qualifies the architecture slice that replaces transitional
named `Schema.unspecified(...)` values at the TPCH, TPC-DS, and ClickBench scan
boundaries. It does **not** replace any row on the published 165-query board.

All qualified captures used `/opt/java/openjdk` JDK 26, one unpinned JMH
thread, one fork, five 1-second warmup and measurement iterations, a 12 GiB
heap, normalized allocation, and the complete standard hardware-counter set.

## Boundary

The Nitro core now exposes a host-neutral `TypeRegistry` contract. Standalone
benchmark planner adapters translate TPCH and TPC-DS catalog metadata, or
ClickBench Parquet metadata, into immutable Nitro `Schema` and opaque
`TypeBinding` values before constructing a source. Catalog, Parquet, and Trino
objects do not cross that boundary.

Registry and schema caches are explicit planner-scoped objects. Operators and
the evaluator neither resolve nor inspect type identities. The schemas retain
field names, nullability, and parameterized decimal/character identities.

## Rejected uncached shape

The first ClickBench adapter reopened the Parquet footer for every measured
scan construction. `nitro-clickbench-q43-uncached.*` allocated 101.931 MB/op,
about 4.4% above the earlier 97.625 MB/op architecture control. The uncached
TPCH and TPC-DS captures are retained for audit, but this execution-time
metadata translation was rejected.

Schema resolution is now cached by planner/source identity and projected
column list. The cache contains immutable metadata only; it owns no execution
data, decoder buffers, or host-engine objects.

## Qualified controls

The most useful adjacent parent/candidate pair is ClickBench q43:

| Metric | Cached candidate vs adjacent parent |
|---|---:|
| Duration | +6.15% |
| Allocation | -0.05% |
| Instructions | +0.41% |
| Cycles | +4.00% |
| L1D misses | -0.03% |
| L1D loads | +0.07% |
| dTLB misses | +13.24% |
| dTLB loads | +26.66% |
| Branch misses | -6.89% |
| Branches | -0.61% |

Retired work, allocation, and L1D traffic are effectively flat. Wall time,
cycles, and especially dTLB counts were unstable on this host; the reverse
TPC-DS q64 parent varied from 2.56 seconds to 2.10 seconds within one
measurement fork. Those metrics are preserved but are not treated as evidence
of an architectural regression or improvement.

The final q64 candidate measured 2025.547 ms, 388.472 MB/op, and 35.795 billion
instructions. Against the earlier accepted source-port capture this is +0.59%
duration, +0.01% allocation, and +0.51% instructions. TPC-H q01 retired
instructions were +0.28% and L1D loads +0.49% versus its earlier source-port
capture; its duration/cycle result moved about +5% with host state.

The cached design is retained because the stable work metrics are flat, the
uncached metadata cost is removed, and the implementation changes no execution
or operator shape. A complete same-source sweep is still required before any
published board row changes.

## Validation

- Full JDK 26 Maven gate: 1,304 tests, zero failures/errors, 566 skips.
- TPCH, TPC-DS, and ClickBench scan harnesses are ratcheted against returning
  to `Schema.unspecified`.
- No JFR artifacts were created.
