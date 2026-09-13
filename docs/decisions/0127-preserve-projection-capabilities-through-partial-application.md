# ADR-0127: Preserve projection capabilities through partial application

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §8, §16
- **Related:** ADR-0037, ADR-0040, ADR-0071, ADR-0120

## Context

The full literal-binding board exposes a missing capability-preservation contract. Coverage q163 and q164
previously fused guarded integral expression trees. After partial application, their selected bindings expose
the generated scalar target but no provider projection program. Operator CPU increases from approximately
9.7 seconds to 47 seconds. Generated physical-program counters disappear. A faster individual literal call
does not compensate for losing expression fusion.

## Decision and correlation

Keep the selected provider's projection capability alongside its literal-bound scalar implementation. Add an
optional, classloader-neutral projection-builder view that substitutes exact non-null literals, remaps remaining
argument indexes in original order, and emits the reduced runtime signature. The provider still sees its original
argument shapes and authors all value, null, and fallback expressions. The resulting program belongs to the
original builder. No arithmetic semantics or function identities move into engine code.

The host composes this capability only from the exact ordinary provider binding for the same function and logical
signature. It must not copy unrelated capabilities whose argument indexes or invocation contracts may change.
Unsupported literal representations decline generation without weakening failure semantics. Initially the
physical builder represents long and boolean literals; other literal carriers retain scalar invocation.

This fills a gap in ADR-0120 rather than reversing provider-owned specialization. The generated scalar adapter
remains the ordinary invocation and guarded fallback implementation. MAY_FAIL remains MAY_FAIL; no data-derived
infallibility claim is introduced. Binding reuse and its separate qualification under ADR-0126 are independent.

## Qualification

Retain the capability-preserving adapter. Exact/native q163/q164 checks and generated-program counters confirm
restored expression fusion; remote operator CPU returns from about 47 seconds to about 8.7 seconds per query.
The corresponding b04/b05 bundle recovery persists in later macro screens. Literal-order, null, fallback and
selected-error tests pass, followed by the combined 2,178-test Nitro suite, 45 focused host tests and 411-query
remote functional gate. No rejected binding cache or function-specific evaluator behavior is retained.

Verify all literal positions and multiple literals, reduced signature/order, nested partial application, exact
provider admission, null reads, preserved fallback predicates, invalid signature rejection, and unsupported
representations. Exercise compiled integral expression trees including overflow, zero division and selected errors.
Compare q163/q164 against the frozen literal-only runtime and the older fused runtime; require generated-program
counters as well as exact results and timings. Then run ordinary-query guards before acceptance.

## Alternatives

Disabling literal binding by query or narrow logical type hides the interaction and discards independent gains.
Declaring fallible scalar trees infallible is incorrect. Duplicating arithmetic semantics in the generic compiler
violates the registry boundary. Keeping the bound constants as runtime vectors defeats partial application and
cannot generalize to host-only specialization arguments.
