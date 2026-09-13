# ADR-0104: Integrate validated filter optimizations without opt-ins

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §13, §15, §17, §19
- **Amends:** [ADR-0100](0100-carry-correlated-dynamic-filters-across-fragment-outputs.md)
- **Adopts:** [ADR-0101](0101-bound-correlated-hash-domains-through-promotion.md),
  [ADR-0103](0103-publish-native-filter-domains-with-primitive-position-ordering.md)
- **Depends on:** [ADR-0025](0025-use-one-logical-dynamic-filter-lifecycle.md),
  [ADR-0027](0027-gate-adaptive-defaults-with-breadth-evidence.md)
- **Evidence:** `~/notes/nitro/2026-09-12-performance-recovery-completion-plan.md`,
  `~/notes/nitro/2026-09-13-primitive-source-filter-ranges.md`; explicit user direction on 2026-09-13

## Context

Keeping a working integration behind an experimental switch is not an acceptable final disposition. The
implementation has passed repeated 409-query correctness gates and a 26-query construction/consumption inventory.
The latter has CPU geometric mean 0.9799 enabled/disabled; q72 CPU is 0.6795 and allocation 0.4451.
It also exposed genuine q49 construction costs and variable q93 results, not universal improvement.
Subsequent removal of source-side scalar-range materialization reduces q93 CPU to 0.8779 of its parent and
allocation to 0.6204, while preserving q72 and passing all409queries. This is not a full performance-board claim.

The user explicitly requires a functioning normal path, followed by a full board to direct further regression
work. We must not disguise remaining costs with a default-off implementation or pretend they have disappeared.

## Decision

Correlated remote-output filtering is part of Nitro execution, not a separately enabled feature. Remove the
production configuration property, planner boolean and disabled constructor path. Selecting Nitro selects this
implementation. Ordinary Cork execution and the host's global dynamic-filter lifecycle remain unchanged.

The existing static binary-equality pushdown is also part of normal execution. Earlier benchmark campaigns
enabled it explicitly while the default policy disabled it. Remove that opt-in policy field and system-property
override as well; preserve registry-provided equality semantics and explicit source enforcement acceptance.
The no-override board must therefore execute the same admitted binary pushdown used by the earlier measurements.
Legacy operator benchmark adapters must pass the same supplied logical schema into the Parquet source, rather than
retagging an outer adapter around an unspecified inner source. Typed filter validation must not be weakened to
accommodate that mismatch. Tests cover binary enforcement through nested source adapters using specified types.

Adopt the bounded sparse collection, global promotion, immutable publication and decoder-owned storage described
by ADR-0101, and the logical primitive-position publication of ADR-0103. Retain automatic semantic and physical
admission: exact tuple lineage, complete partition state, bounded storage, registered types, possible consumers,
and observed selectivity. These internal checks are correctness and cost controls, not user opt-ins. No query IDs,
special logical types, new scheduling policy, or SQL-shape exceptions are introduced by this decision.

The benchmark harness/composition adapter must reject either value of the removed overrides, rather than silently running an old
OFF/ON script as two identical executions. Validate the ordinary Nitro configuration with no enabling override.
Preserve historical toggled evidence as historical; future regression boards measure the actual normal engine.

Run the complete four-suite board after the no-override correctness gate. Reuse qualified unchanged Cork CPU,
latency and peak controls where configuration and workload match. Do not compare old live-thread allocation
counters to corrected cumulative counters. Missing compatible allocation controls remain explicitly unavailable
until recaptured under a matching contract.

## Consequences

- Callers need no separate feature setting; tests and benchmarks exercise the normal path.
- The obsolete configuration is rejected instead of accepted as a nonfunctional compatibility switch.
- q49 metadata construction overhead remains a visible regression to resolve, not a completed optimization.
- Mixed-version sparse-envelope compatibility remains subject to ADR-0101; this does not claim rolling-upgrade
  negotiation or broad productization readiness.
- Performance admission is supported by breadth evidence but does not promise a win for every workload. Further
  regression repairs must improve the normal implementation instead of reintroducing opt-ins.

## Alternatives considered

**Keep default-off while optimizing every residual.** Rejected explicitly by the user; this leaves the integration
unfinished and makes normal-engine benchmarks unrepresentative.

**Flip the defaults and retain the switches.** Rejected as unnecessary alternate execution state. Keep automatic
conservative admission and the normal host-level choice of engine instead.

**Remove correlation entirely.** Discards the measured q72 reduction in work, CPU and allocation. The correctness
and breadth evidence supports keeping the implementation and fixing its remaining costs.
