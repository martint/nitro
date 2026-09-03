# ADR-0060: Generate aggregation over aligned encoded domains

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §12.1
- **Depends on:** [ADR-0055](0055-link-generated-aggregation-targets-as-constants.md), [ADR-0059](0059-pass-variable-width-contributions-as-binary-regions.md)

## Context

A generated grouped-aggregation loop can remove row dispatch yet still repeat the same update for every logical row.
When a grouping key and all aggregation inputs share one dictionary mapping, the useful values live in the smaller
physical domain. Existing provider batch implementations can exploit that fact, so replacing them with a generated
logical-row loop would be an architectural and performance regression.

The engine cannot infer whether applying one contribution with a frequency is equivalent to applying it repeatedly.
That property belongs to the aggregation implementation. Nor may it assume that independently encoded value and null
streams share a domain merely because their logical lengths match.

## Decision

Nitro may generate a grouped-aggregation kernel over a physical encoded domain only when:

1. the group key, every value contribution, and every present null stream have the same row-to-domain mapping;
2. filters and distinct processing do not require logical-row identities at this stage;
3. every update supplies an exact provider-owned repeated-update target; and
4. the configured reduction admission threshold is met.

The batch boundary counts selected logical rows per domain value and resolves each used domain key to a group once.
The generated kernel then visits each used domain position, performs combined null elimination, loads the declared
physical contribution tuple, and invokes the constant-linked repeated target with the exact multiplicity. It does not
recognize a function, logical type, or provider state class.

This is a distinct physical kernel contract rather than an overloaded logical-row loop. Its arguments name domain
frequencies and domain-to-group identities explicitly, which prevents accidental confusion between logical positions
and physical domain positions.

## Consequences

- Generated adapters preserve dictionary-domain work reduction instead of trading it for cheaper per-row dispatch.
- Extrema may ignore multiplicity, sums and counts may apply it arithmetically, and order-sensitive functions may
  decline the contract; those choices remain provider-owned.
- Mixed fixed-width and binary-region contributions use the same mechanism.
- Independently mapped, nullable, filtered, or distinct inputs retain the existing correct logical-row path.
- Stable mapping identity is a correctness proof, not a heuristic based on equal cardinality.

## Alternatives considered

**Always generate a logical-row loop.** Simpler, but can turn one update per physical value into billions of repeated
updates and regress an already encoding-aware provider.

**Let the engine classify idempotent functions.** This teaches the execution engine function semantics and is not
classloader-neutral.

**Invoke generic method handles from a Java domain loop.** Correct, but gives up the constant call sites and concrete
array operations required for JIT inlining and loop optimization.

**Assume mappings align when domain sizes match.** Incorrect: equal-sized dictionaries can have unrelated row
mappings, ordering, and null domains.
