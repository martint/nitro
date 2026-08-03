# TPC-H q16 SQL-topology reconciliation

This slice replaces the standalone q16 harnesses' one-stream approximation with the physical
aggregation topology in Trino's distributed plan:

- six independent source-driver partial-DISTINCT states;
- a four-key hash exchange into two final-DISTINCT/partial-count pipelines;
- a three-key hash exchange into two final-count pipelines;
- the narrow `[ps_suppkey, p_brand, p_type, p_size]` join boundary; and
- `count(ps_suppkey)`, rather than the semantically equivalent data-specific `count(*)` shortcut.

The topology adapter is test-only. It schedules existing Nitro aggregation sessions and does not
change any production operator implementation. The Trino twin uses existing Trino operators in
independent driver pipelines and explicit in-memory hash partitioning.

Both real-SF10 reference tests pass:

- `org.weakref.nitro.tpch.TestTpchQueries#query16`
- `org.weakref.trino.tpch.TestQueries#query16`

## Duration

JDK 26, fixed/pre-touched 12 GiB heap, one fork, two 1-second warmups, three 1-second measurements:

| Engine | Mean ms/op |
|---|---:|
| Nitro | 701.748 |
| Trino operators | 731.556 |
| Nitro / Trino | 0.959 |

The adjacent perf/allocation capture was 696.764 ms/op for Nitro and 736.894 ms/op for Trino
operators (ratio 0.946). The older corrected four-stage but one-stream harness measured
498.888/660.413 ms (ratio 0.755), so most of that apparent advantage did not survive the actual
six/two/two state topology.

## Normalized counters

| Metric | Nitro | Trino operators | Nitro / Trino |
|---|---:|---:|---:|
| instructions/op | 19.040 B | 20.312 B | 0.937 |
| cycles/op | 4.578 B | 5.908 B | 0.775 |
| L1D load misses/op | 131.020 M | 168.717 M | 0.777 |
| dTLB load misses/op | 1.224 M | 1.105 M | 1.108 |
| branch misses/op | 15.965 M | 37.804 M | 0.422 |
| allocated bytes/op | 704.402 M | 1,408.393 M | 0.500 |

Nitro still has materially better instruction, cache, branch, and allocation behavior in the
standalone SQL-shaped comparison. Its remaining regression is dTLB misses. The much smaller wall
win than the cycle ratio also means duration alone is not a reliable attribution signal on this
container.

## Nitro inclusive operator profile

A three-iteration profile measured 802.2 ms/iteration. Because wrappers are nested, differences
between adjacent inclusive totals estimate exclusive work:

| Region | Approximate exclusive ms/op |
|---|---:|
| partsupp scan | 90 |
| part hash join | 100 |
| supplier anti join | 25 |
| six-way partial DISTINCT | 234 |
| two-way final DISTINCT + partial count (including hash partitioning) | 316 |
| two-way final count (including hash partitioning) | 9 |
| final sort | 18 |

The aggregation/exchange portion is therefore about 559 ms/op. This establishes that the corrected
operator benchmark no longer predicts a large q16 SQL win. It does not yet explain why integrated
Nitro SQL uses about 948 CPU-ms versus Trino SQL's prior 706 CPU-ms: the integrated Nitro capture's
aggregation regions are actually lower (roughly 439 ms), leaving boundary, exchange, and execution
orchestration as the next attribution target.

## Remote-exchange representation correction

The initial topology adapter reproduced partition and driver counts but passed sparse masks over the original Nitro
vectors at each hash exchange. It therefore preserved dictionary-backed brand/type vectors that the real Trino
remote exchange flattens while partitioning and serializing pages. That was still a material physical-shape mismatch.

The adapter now copies every selected partition into dense, owned vectors before the receiving aggregation session.
This is test-only exchange materialization; production aggregation implementations are unchanged. Exact SF10 Nitro
and Trino-operator results pass. A short adjacent JDK 26 screen measured 727.084 ms for Nitro and 739.975 ms for the
Trino operators (0.983x), versus 661.020/763.564 (0.866x) when the same current source preserved native encodings.
The earlier 0.959 duration and counter rows remain useful evidence for the native-encoding control, but must not be
presented as the current SQL-boundary expectation. A fixed/pre-touched publication recapture is still required before
replacing the benchmark board row.
