# ADR-0077: Materialize selected capable window results by forward batch range

- **Status:** accepted
- **Date:** 2026-09-04
- **Spec:** §8, §11, §12.1
- **Depends on:** [ADR-0007](0007-make-vector-ownership-explicit.md), [ADR-0015](0015-keep-aggregate-state-with-function-providers.md), [ADR-0041](0041-target-zero-row-proportional-steady-state-allocation.md), [ADR-0076](0076-bind-reversible-window-aggregation-state.md)
- **Evidence:** Engine Coverage q241 attribution and `2026-07-23-tpcds-q51-incremental-window-rejection.md`

## Context

Large bounded aggregate windows can retain complete result planes even when downstream planning selects only a small
subset of function outputs. Engine Coverage q241 attributes substantial allocation and peak memory to those planes
after reversible updates removed frame replay. The earlier universal q51 experiment streamed every row-final window
function, improved CPU and allocation, but regressed translation work and was rejected. That experiment did not have
the current positional-frame provider lifecycle, output-demand selection, or replay requirements.

## Decision

`RunningWindowFunction` exposes an optional forward batch-range materialization capability. Nitro initially implements
it only for positional-frame `RegisteredAggregationWindowFunction`. The operator admits it only for selected outputs,
omits their complete result planes, and eagerly materializes independently owned result streams bounded by the public
batch size in `next()`.

Each range receives the complete partition `WindowPositionIndex`. Calls advance without gaps within a partition and
preserve provider state and reversible-frame continuity across output batches. Partition boundaries reset state,
physical source-page transitions rebind input, and unsupported or non-monotonic frame movement retains exact replay.
Legacy functions retain complete-plane evaluation. Lazy source outputs remain lazy, retained output batches do not
borrow mutable function storage, and duplicate selected channels receive separate ownership.

## Consequences

- Selected positional aggregate results retain at most one public batch of destination storage per live batch rather
  than one complete result plane.
- Output production now advances capable function state during `next()`, so calls must remain forward and serialized.
- Complete partition metadata and retained source pages still exist; this decision does not make window input streaming.
- Mixed capable and legacy windows use both lifecycles concurrently without weakening legacy behavior.
- Omitting a capable output avoids allocating its provider state or result plane.

## Alternatives considered

**Universally stream every row-final window function.** Rejected again because the q51 experiment demonstrated a
repeatable translation-locality regression and because partition-final and peer-final functions do not satisfy this
lifecycle.

**Materialize fixed larger slabs and slice public batches.** Rejected because q51 slab experiments did not provide a
stable locality crossover and would retain an additional policy-sized result plane.

**Teach `WindowOperator` which aggregate frames are safe.** Rejected because state, frame-result, null, and replay
semantics belong to the function adapter and provider capabilities.
