# ADR-0125: Specialize scalar loops by physical input shape

- **Status:** rejected
- **Date:** 2026-09-13
- **Spec:** §8, §16, §18
- **Related:** [ADR-0037](0037-link-generated-scalar-targets-as-constants.md), [ADR-0051](0051-contain-mapped-failures-during-values-evaluation.md), [ADR-0120](0120-specialize-provider-owned-primitive-literals.md)

## Context

The scalar adapter's direct-array convention assumes that a logical long scalar carrier uses an I64 input vector.
I32 inputs therefore fall through to row-wise accessors. Literal binding exposed this on ClickBench q35: controlled
local parent/literal/parent CPU was 5.814/6.888/5.721 seconds, and paired CPU profiles reproduced the regression.
The changed calling path is expensive; the extent to which physical admission alone repairs it needs measurement.

## Proposal and correlation

Generate direct-array loops for observed physical argument tuples, independently of their logical scalar carriers.
An I32 input to a long carrier uses an int-array load followed by widening. Each argument has its own physical
representation; arity and combinations remain generator inputs, not a family of Java calling interfaces. Exact
provider targets, logical signatures, output types, strict null propagation and mapped failures are unchanged.
This fills an admission gap in SPEC §8/16 and preserves ADR-0037/0051 rather than changing function semantics.

Each resolved binding owns a bounded cache of immutable generated shapes. The generator receives its shape budget
during construction (default 32, including the original shape). Only observed shapes generate classes; a saturated
cache retains existing shapes and uses the established accessor path for additional shapes. It never evicts and
regenerates classes on alternating inputs. A query-local invocation state remembers its last admitted shape, so
steady-state lookup allocates no metadata and takes no cache lock. Cold generation is synchronized; published
entries are immutable. Neither cache nor entries retain input vectors, arrays, queries or allocators. Linkage
lifetime follows the binding/provider, not a static cache. There is no enabling property or session flag.

Flat and dictionary loops share the same physical tuple, and RLE run evaluation uses the corresponding dictionary
loop. Existing mixed-encoding and null/error paths remain valid. This does not remove error scratch or infer that a
function cannot fail from its physical inputs. It is independent of proposed ADR-0124.

## Qualification

Test mixed I32/I64/double/boolean/reference arguments; dense/sparse masks; dictionary and RLE mappings; nulls,
existing errors, mapped failures and independent stream demand; shape transitions, reuse, concurrent generation,
cache saturation and fallback. Validate generated direct loads behaviorally, not using package-name tests.
Measure q29/q35 against both the original provider and literal-only implementation, plus broader scalar and macro
guards. Keep remote board binaries frozen. Accept only after uninstrumented local and remote qualification.

## Alternatives

**Materialize I64 inputs.** Adds copying and intermediate storage simply to fit an internal convention.

**Branch on physical kind for every row.** Keeps avoidable dispatch in the hot loop and relies on loop unswitching.

**Generate all possible tuples eagerly.** Grows combinatorially and retains classes for shapes never observed.

**Add function-specific cases or restore a handwritten q35 path.** Does not repair the calling framework.

## Disposition

Rejected after local SQL qualification on 2026-09-13. The physical-only candidate passed 2,170 tests
(394 skipped) and 167 focused host tests. W30/M9 q35 control/candidate/control CPU was
7.361/6.914/7.146 seconds with overlapping ranges. It did not establish recovery of the older provider's
5.7–5.8 seconds. In a separate factorial comparison, binding reuse alone measured 5.957 seconds while
binding reuse plus physical shapes measured 5.976 seconds; there was no independent q35 CPU benefit.

Retain the existing accessor fallback and reject this additional shape cache/code-generation machinery for now.
Remove its implementation and candidate-specific tests from the introducing mutable commit; preserve frozen
binaries, test logs and comparisons in the investigation artifacts. This is not a claim that physical-input
specialization can never help another workload. Reconsider only with independent evidence justifying the added
generation and lifetime complexity. ADR-0126 investigates binding reuse separately.
