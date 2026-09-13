# ADR-0101: Bound correlated hash domains through promotion

- **Status:** accepted
- **Adopted by:** [ADR-0104](0104-integrate-correlated-filtering-without-an-opt-in.md). The proposal and its prior validation conditions below record the investigation; ADR-0104 states the current normal-path decision and remaining costs.
- **Date:** 2026-09-12
- **Spec:** §13, §15, §17, §19
- **Amends if accepted:** [ADR-0100](0100-carry-correlated-dynamic-filters-across-fragment-outputs.md)
- **Depends on:** [ADR-0027](0027-gate-adaptive-defaults-with-breadth-evidence.md)
- **Evidence:** `~/notes/nitro/2026-09-11-correlated-remote-output-dynamic-filter-validation.md`;
  `~/notes/nitro/2026-09-12-performance-recovery-completion-plan.md`

## Context

The correlated-filter prototype removes most remote producer rows in one high-fanout join, but a fixed full-capacity
Bloom filter in every build driver, merge, and transport is disproportionate for small domains. Reducing Bloom capacity
from local cardinality degraded the merged filter: local bounds do not bound the union of all partitions.

The unresolved experiment must finish with a breadth-validated general implementation or removal, not an indefinitely
disabled alternative. The exact join and independent column filters remain authoritative and unchanged.

## Proposed decision

Each driver collects exact tuple hashes, bounded by the smaller of the host's constructed Bloom-admission cardinality
and raw published-hash payload budget (byte budget divided by eight). Exceeding that limit drops the optional
correlation, not the independent scalar domains. A driver does not construct a Bloom filter. Publication produces
one sorted distinct hash array. Collection hash-table overhead and scratch are separately included in retained-state
accounting; the raw payload bound is not a claim that those temporary structures or JSON encoding occupy eight bytes
per entry.

Keep admitted sparse domains compact through transport, translation, and consumption. A partition union exceeding
the sparse limit promotes to the established full-capacity Bloom representation, not a locally sized one. Partition
unions retain every contributing hash; missing partitions or incompatible tuple shapes still drop the correlation.
Hash collisions can only retain extra rows.

Published domains are immutable. Retained bytes include builder storage, scratch, published domain storage, and
transported state under the existing host accounting owners. The correlation budget must not compact or displace
independent scalar domains. No behavior of Cork's existing scalar Bloom implementation changes.

Both task-local and coordinator collection use the independent-domain size for semantic simplification. The
coordinator still counts the full retained payload to trigger early partition union; separating these quantities
must not turn queue memory pressure into scalar range simplification or an unconstrained domain merely because a
bounded correlation was attached. The original scalar-only size-limit outcomes remain unchanged.

Builders receive the host's existing typed-hash compiler during construction, rather than generating fresh classes
per driver. Dense partition union uses a private merge destination without copying a full bitset per partition;
published inputs remain immutable. Exact union can reuse immutable arrays when all nonempty partitions have the
same sorted hashes. Reuse must still honor the smallest limit from every partition, including empty ones; a stricter
empty partition can require Bloom promotion. Check this before entering the general merge kernel: the reuse path
does not need merge scratch and the general merge retains its existing inner loop. This is a representation
optimization, not an admission/default change.
Mutable hash-domain storage is encapsulated by its builder. Finishing seals the
builder and transfers the sorted publication array into the immutable domain without a second defensive copy;
subsequent input is rejected. Public constructors remain defensive. Native island collection charges its retained storage to the island allocator
and releases the reservation on publication or close, just as the boundary adapter accounts its collection state.

The JSON-only creator may take ownership of exact arrays and Bloom storage created exclusively by the decoder,
instead of defensively copying them again. It retains the same representation, ordering, uniqueness and limit
validation. The host mapper requires public creators, so this factory follows the existing `@DoNotCall` and
deprecation convention for framework-only entry points. Normal Java construction remains defensive; no new wire
field, shared deserialization identity, or permission to mutate a published domain is introduced.

Readers accept the older Bloom-only correlation envelope. The new sparse envelope requires matching capable workers;
it does not imply that an older worker can consume a sparse payload. Mixed-version negotiation is not implemented by
this experiment and must not be advertised as supported.

The consumer is evaluated without an explicit wait as a separate admission decision; scan-side host readiness and the
scheduler are unchanged. Default enablement requires the complete activation inventory and all multi-key build
candidates, not only the seven previously observed remote-output consumers. Collection can incur cost even when a
consumer never applies the filter. Include repeated launches where noise obscures the result. This record remains proposed until that evidence
settles representation and readiness policy.

Native build admission skips optional tuple collection when the join probe subtree provably has no remote-output
consumer: after expanding Nitro islands it contains neither a remote source nor a remote exchange. Native scans
currently consume independent column domains and native join-filter capabilities, not this host tuple envelope.
The ordinary host page-filter consumer is unchanged. A remote source only establishes possible consumption; exact
lineage and runtime readiness are still separate checks. Build-only fragments with no visible probe remain
conservatively admitted. This neither rewrites the plan nor changes scalar collection, the exact join, or scheduling.
If a future native scan capability accepts the tuple envelope, its admission must extend this consumer check with
corresponding coverage; the current absence of that consumer is not a permanent architectural restriction.

## Consequences

- Small domains need memory proportional to represented hashes rather than a full Bloom bitset.
- Promotion preserves the global union's Bloom capacity even when every local partition is small.
- Sparse lookup uses binary search and therefore adds a representation-dependent probe cost to validate.
- The wire contract changes and requires explicit coverage for both envelopes.
- A favorable q72 result alone does not authorize enabling the capability by default.

## Alternatives considered

**Shared primitive-pool collection scratch.** Rejected after testing allocator-backed hash-table and batched-hash
storage. Isolated large-domain collection reduced allocation, but repeated tiny-domain collection became slower.
The warmed q17/q49/q72 SQL check did not recover q49 and increased q17 allocation by about 10%. This does not
reject allocator ownership in general; it rejects this particular pooling implementation without sufficient
end-to-end benefit. The implementation and its dedicated fixture were removed from the introducing commit.
The patch and measurements remain in `~/notes/nitro/2026-09-12-q49-pooled-correlation-scratch.md`.

**Stop at the scalar exact-set collection threshold.** Rejected after the preservation check: it discarded q72's
correlation and removed its benefit. The raw-payload/cardinality-bound candidate restores that benefit without
requiring a full Bloom in each driver. Its final breadth and corrected allocation measurements remain required.

**Promote within each driver.** The full construction surface exposed 3–6% CPU increases on several additional
TPC-DS queries. The current candidate removes this branch instead of retaining an unused dense-builder mode.

**Smaller per-driver Bloom filters.** Rejected: the global union can saturate despite every local partition meeting its
own cardinality bound. This was measured, not merely a theoretical objection.

**Only optimize merge or serialization of the fixed bitset.** Earlier experiments failed to recover the small-query
overhead. The proposed representation remains compact through all phases rather than expanding before publication.

**Unbounded exact tuple hashes.** Rejected because build size and transport would no longer be bounded.

**Remove the correlated filter now.** A valid final disposition if breadth remains unfavorable, but the substantial
avoided intermediate work justifies completing this bounded representation experiment first.
