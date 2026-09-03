# ADR-0056: Allow generated aggregation updates to consume primitive tuples

- **Status:** accepted
- **Date:** 2026-09-03
- **Spec:** §8, §12.1, §16
- **Depends on:** [ADR-0012](0012-execute-over-encoded-domains.md), [ADR-0015](0015-keep-aggregate-state-with-function-providers.md), [ADR-0055](0055-link-generated-aggregation-targets-as-constants.md)

## Context

The first exact grouped-update convention accepted one primitive contribution. It covered count, sum, extrema, and
states composed of multiple physical vectors, but not one semantic update that consumes multiple inputs. Statistical
aggregates, weighted aggregates, and other multi-argument functions would otherwise retain a bespoke row loop or
require an interface for every carrier tuple.

Null handling is part of this problem. For a strict multi-argument aggregate, the provider update must not be called
when any contributing input is null. Encoded-domain execution additionally requires proof that every value and null
mapping refers to the same physical domain; matching only one input is insufficient.

## Decision

A provider-authored grouped update declares an ordered, non-empty tuple of primitive contributions. Each contribution
is an input value, a double input value, or a constant with an optional input null check. The exact target accepts the
opaque state, group id, and the declared carriers in that order. A repeated target, when present, appends one logical
multiplicity after the complete tuple.

The generated grouping backend flattens all declared contributions into reusable physical bindings, performs the
combined null check, loads each specialized primitive array, and invokes one constant-linked target. It creates no
per-row tuple or argument array.

Multi-input updates remain on the generated logical-row path until the physical binder can prove that every value and
null mapping shares one encoded domain. Lack of that proof disables the domain shortcut, not the generated update.

## Consequences

- One convention covers arbitrary long/double carrier tuples without a combinatorial interface hierarchy.
- Providers retain their exact update algorithm and opaque state layout.
- The hot loop has monomorphic primitive loads and a constant-linked exact call, with no row-proportional allocation.
- Strict null semantics are enforced once around the complete update.
- Encoded-domain admission remains conservative until all input mappings are jointly proven.

Engine Coverage q189 validates covariance, correlation, slope, and intercept over one billion logical rows. Its
warmed complete-Nitro run used 7.970 CPU-seconds and 584.5 MB allocated, versus 41.576 CPU-seconds and 20.5 GB for
unchanged Cork execution (0.192x CPU and 0.029x allocation). This is evidence for the general tuple convention, not
engine recognition of those functions.

## Alternatives considered

**Publish one interface per carrier tuple.** Direct but combinatorial, and it couples provider implementations to an
engine-owned interface taxonomy.

**Pass an array or tuple object to one generic target.** Compact API surface, but it adds allocation or indirect
loads inside the row loop and prevents primitive call-site specialization.

**Split a semantic update into independent single-input updates.** Invalid for algorithms whose state transition
depends atomically on all inputs, such as covariance.

**Assume encoded inputs align when their cardinalities match.** Fast but incorrect; equal domain size does not imply
equal logical-row mapping or null mapping.
