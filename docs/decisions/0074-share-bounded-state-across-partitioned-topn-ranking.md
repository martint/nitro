# ADR-0074: Share bounded state across partitioned Top-N ranking

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §9, §11, §16
- **Depends on:** [ADR-0007](0007-make-vector-ownership-explicit.md), [ADR-0019](0019-account-retained-state-to-the-host.md), [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md), [ADR-0072](0072-retain-only-qualifying-topn-ranking-groups.md), [ADR-0073](0073-bind-exact-normalized-ordering-keys.md)
- **Evidence:** Engine Coverage q230 and partitioned ranking state tests

## Context

ADR-0072 bounded unpartitioned Top-N ranking but deliberately left partitioned ranking as a visible gap. The prior
partitioned implementation retained every input batch and selected qualifying rows only after end of input. That made
a small rank limit consume memory and allocation proportional to all input rows.

Creating one complete ranking state and allocator context per partition would bound rows, but would multiply
allocator, row-store, comparison, and schema state by the partition count. Partition identity is also a generic
grouping problem already solved by `GroupingState`; the ranking operator must not rediscover logical type semantics.

## Decision

Partitioned Top-N ranking uses one shared allocator-owned row store and one generic grouping state. Grouping assigns a
dense partition identity from provider-bound key semantics. Each partition retains only its ordered peer-group
metadata and slot identifiers into the shared store.

`ROW_NUMBER`, `RANK`, and `DENSE_RANK` use the same admission and eviction semantics as the unpartitioned state,
independently within each partition. Evicted slots are immediately reusable. Payload columns are copied only for
candidates still live at the end of the offered batch. At finish, grouped partition keys restore the required global
partition ordering before retained peer groups are emitted.

The state is generic over partition and ordering types, key arity, and physical representation. It uses provider-bound
group identity, structural comparison, and optional exact normalized order keys rather than recognizing logical types.
The host-session and Nitro pull entry points share this state.

## Consequences

- Retained row storage scales with qualifying results and required ties, rather than total input.
- Per-partition overhead is limited to peer metadata; partitions do not own separate allocators or row stores.
- Distinct partition-key storage still scales with partition cardinality because each output partition is observable.
- Large qualifying boundary tie groups can still be large by SQL semantics.
- Partitioned and unpartitioned ranking now share one bounded-state contract, while retaining separate internal state
  implementations suited to their metadata needs.

## Alternatives considered

**Retain all input and partition at finish.** Correct, but preserves the row-proportional allocation and memory defect.

**Create a complete Top-N state per partition.** Bounds retained rows but makes substantial operator and allocator
state proportional to partition count.

**Specialize the benchmark's integer partition key.** Avoids generic grouping overhead but violates provider-owned
logical semantics and does not cover composite, nullable, variable-width, or provider-defined keys.
