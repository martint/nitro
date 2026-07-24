# Parquet source-port vertical-slice validation

This directory validates the second architecture-cleanup slice against the
pre-cleanup performance baseline. It does **not** replace any row on the
published 165-query board.

All qualified JMH captures used JDK 26 from `/opt/java/openjdk`, one unpinned
thread, one fork, five 1-second warmup and measurement iterations, a 12 GiB
heap, normalized allocation, and the complete standard hardware-counter set.

## Correctness and lifecycle findings

The real Nitro Parquet decoder was placed behind `OperatorBatchSource` and
`BatchSourceOperator`, with explicit schema and typed native protocols.
Runtime filters crossed the source boundary through a typed domain capability;
the source and engine adapters never inspected each other's implementation
classes. Runtime-filter capability is checked per stable source-column handle,
so a source cannot accidentally advertise pushdown for unsupported columns.

The first q43 activation (`nitro-q43-source-port.*`) exposed a correctness bug:
`poll()` pulled the next legacy batch during `hasNext()`, closing a Parquet
batch whose lazy outputs were still live downstream. Polling now stages only
availability; the decoder advances when ownership transfers in `next()`. A
regression test ratchets this ordering.

The first q64 activation (`nitro-q64-source-port.*`) exposed a second lifecycle
contract mismatch: constrained reborrow is a property of the current Parquet
batch, but the adapter snapshotted it at plan construction. Capabilities are
now live. The native source protocol further removes batch/source wrapper
allocation and dispatch for a Nitro-native compatibility island.

TPC-DS q64 row-count diagnostics from the port and its parent were identical,
including every dynamic-filter stage and survivor count.

## Broad activation decision

A reverse-order capture from parent `c5815f59` measured q64 at 2009.457 ms and
35.837 billion instructions. The best broad source-facade activation
(`nitro-q64-native-protocol.*`) measured:

| Metric | Port activation vs reverse parent |
|---|---:|
| Duration | +5.58% |
| Allocation | +0.46% |
| Instructions | +1.92% |
| Cycles | +3.76% |
| L1D misses | -0.63% |
| L1D loads | +1.07% |
| dTLB misses | +4.39% |
| dTLB loads | +5.20% |
| Branch misses | +1.23% |
| Branches | +1.04% |

Review found that the source SPI had omitted the legacy operator's exact row
count. The facade consequently returned "unknown", changing hash-join admission
and capacity policy. `BatchSource.exactRows()` now preserves that
format-neutral planning fact.

With the full source-port path active, `nitro-q64-exact-rows.*` measured:

| Metric | Final port vs reverse parent |
|---|---:|
| Duration | +0.21% |
| Allocation | -0.03% |
| Instructions | -0.63% |
| Cycles | -1.33% |
| L1D misses | -3.27% |
| L1D loads | -1.29% |
| dTLB misses | -5.93% |
| dTLB loads | -13.58% |
| Branch misses | -2.69% |
| Branches | -0.66% |

The source-port activation is retained in the TPCH, TPC-DS, and ClickBench
operator harness factories. A real synthetic Parquet scan/filter/project test
also exercises explicit schema, native batch transfer, and runtime-filter
translation.

The adjacent q43 control is flat on retired work versus the earlier fixed
source-port capture: -0.27% instructions, -0.67% cycles, -3.24% allocation,
and +1.14% duration. TPC-H q01 versus the first architecture representative was
-3.49% duration, -1.18% allocation, +0.40% instructions, and -3.37% cycles.

These are targeted architecture controls, not a complete same-source
165-query sweep. The published board remains methodologically unchanged.

The remaining JSON/log pairs preserve rejected intermediate activations, the
temporarily restored direct harness, and the reverse-order control for audit.
