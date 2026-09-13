# ADR-0126: Reuse exact generated scalar bindings

- **Status:** rejected; production cache removed after SQL qualification
- **Date:** 2026-09-13
- **Spec:** §8, §16
- **Related:** ADR-0037, ADR-0071, ADR-0120; independent of proposed ADR-0124 and rejected ADR-0125

## Context

Constant linkage makes a target optimizable, but does not preserve the compiled loop when each query compilation
creates another hidden class. The q35 profile that reproduces the literal-binding regression contains 18 distinct
generated scalar classes across five measured queries after 30 warmups. Physical I32 admission alone has not
established a separated whole-query improvement. Binding lifetime is therefore an independent hypothesis to test.

## Proposal and correlation

The instance-owned host registry binding retains a bounded cache of immutable resolved scalar implementations.
The key includes the exact resolved host function, bound logical signature, original target identity, primitive
literal positions and raw bits, and failure convention. Different provider targets cannot share linkage. Double
literals preserve signed zero and NaN payloads rather than using rounded text or canonicalized NaN equality.
Only admitted session-free, instance-free exact targets qualify. Arbitrary host-only specialization objects remain
uncached; an explicit immutable semantic key contract would be needed to retain them safely.

The cache belongs to its constructed binding/provider, not a static service. Its construction-time bounds default
to 256 bindings and one hour after creation, matching the host scalar implementation cache's age limit. A passed
ticker makes expiry testable without sleeping. Expiry and size eviction may discard warmed code, but cannot change
semantics; active queries keep their binding alive. As in the host cache, physical cleanup is opportunistic: expiry
limits reuse and is not a promise of immediate classloader unloading while an otherwise idle registry remains alive.
Cache publication is atomic for a key. Entries retain neither query IR/references nor input vectors,
allocators, execution state, or output buffers. Each call returns its current runtime expressions, and each
execution continues to create independent allocator-owned state.

This fills a lifetime gap while conforming to SPEC §8/16 and ADR-0037's provider-owned immutable linkage. It does
not introduce function-specific behavior, result caching, an enabling flag, or a new process-global registry.
It neither removes mapped-error scratch nor depends on physical shape specialization.

## Qualification

Test exact binding reuse, current argument references, target/signature/literal/failure separation, raw floating
bits, independent registries and execution state, concurrent publication, expiry and eviction semantics. Compare cache-only
and cache-plus-physical-shape runtimes against the same frozen host/engine control. Retain the old native-provider
measurement as the recovery target. Require local guards and remote qualification before acceptance.

## Alternatives

Per-query generation avoids retention but repeatedly discards warmed code. A static cache violates provider and
classloader ownership. Caching complete bound calls retains query expressions. Caching by display identity alone
does not prove target or literal equivalence. Unbounded retention is unacceptable for user-supplied literals.

## Outcome

The cache passes its176 focused tests and improves some local short scalar workloads, notably q35. It does not
qualify on the wide source-less joins. After the independent selected-error correction, remote q242 CPU median
increases145.499→189.137seconds (1.2999), latency24.393→31.188seconds (1.2786), and cumulative JVM allocation
133.546→171.905GB (1.2872). All measured ranges are separated. Five warmups and five measurements use the same
heap, topology and physical input; there is no profiler. Earlier local and profiled cache losses also remain evidence.

Unchanged fork variation prevents assigning every earlier difference to caching. It does not justify retaining a
default with this unresolved30% CPU and38GB allocation loss. Remove the cache and cache-specific tests at the
introducing mutable host change; retain this record and frozen implementations/evidence. No opt-in remains.
Stop further cache qualification rather than spend another macro cohort on an already unqualified candidate.

This rejects the implementation, not the general value of retaining optimized immutable linkage. Reconsider only
with a reproduced compiler/allocation mechanism and breadth qualification. The generated adapters and literal
projection capability repair remain; no function semantics or host scheduler changes are made.

Evidence: selected-error-cache-q242-checkpoint-comparison-20260913.txt and the completed q242 checkpoint under
selected-error-qualification-20260913-cache-diagnostic in the external Nitro evidence tree. The unfinished q243 arm
is not a completed measurement. The rejected patch is retained as rejected-generated-binding-reuse-cork-20260913.patch.
