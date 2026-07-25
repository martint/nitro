# Mask resident-capacity accounting

Date: 2026-07-25

TPC-H q04 failed in both the provider-derived I64 candidate and its exact
parent while closing a filter/semi-join pipeline:

```text
released 40000 resident bytes with only 11102 allocated
```

The failure was not caused by the I64 migration. An allocator-owned mask can
be acquired as `ALL(n)` with zero position capacity, then materialize/grow its
position array during an in-place filter. Resident memory was charged only
when the mask was acquired, but pool eviction debited its final capacity.

Masks now carry their resident-capacity listener with their allocator
ownership. Every backing-array replacement reports the old/new capacities, so
in-place growth reserves the delta and representation compaction releases it.
Resident state is attached to the mask rather than inferred from a transient
context-level “last borrow was resident” flag. Intrusive in-use tracking also
records the exact context owner, preventing a forwarding lifecycle from
unlinking a mask through the wrong context.

Regression tests cover both in-place growth from a zero-capacity all mask and
wrong-context release. The full JDK 26 suite passes 1,353 tests with zero
failures/errors and 566 skips. TPC-H q04 completes repeated warmup and
measurement iterations.

The full three-fork verification uses JDK 26, one unpinned JMH thread, a 12 GB
heap, 10x1s warmup, 5x1s measurement, allocation, and all eight hardware
counters in the same invocation:

| Duration | Allocation | Instructions | Cycles | L1D misses / loads | dTLB misses / loads | Branch misses / branches |
|---:|---:|---:|---:|---:|---:|---:|
| 956.054 ms | 135.409 MB/op | 13.916B | 5.009B | 203.086M / 8.251B | 134.142K / 1.225M | 45.433M / 2.843B |

There is no compatible failing-parent duration, so this is correctness and
methodology evidence rather than a comparative performance claim. The frozen
pre-architecture board remains unchanged.

Primary artifact: `q04-fixed-3fork.json`.
