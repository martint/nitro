# ADR-0120: Specialize provider-owned primitive literals

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §8, §15, §16
- **Related:** [ADR-0037](0037-link-generated-scalar-targets-as-constants.md), [ADR-0071](0071-bind-specialization-constants-before-generated-scalar-adaptation.md)

## Evidence and gap

An RLE vector exposes repetition to the evaluator, but its scalar value can remain a runtime argument inside
the target. A local generated-adapter probe binds the same remainder target to divisor 100: dense flat duration
0.496 versus 1.861 ns per logical slot and dense dictionary 0.743 versus 1.915 ns. All six encoding/mask cases improve;
the result includes argument removal and physical-dispatch changes, not just possible JIT arithmetic lowering.
The probe does not establish a SQL gain. Its mask/null/mapped-error equivalence tests and full Nitro suite pass.

The host-only specialization path from ADR0071 does not consume ordinary primitive literals. Explicit native
arithmetic bindings also correctly bypass the generic fallback. Extending the fallback's precedence would silently
replace provider choices and is not the proposed design.

## Decision and correlation

Permit the generated call binding to consume exact non-null primitive constants as well as host-only immutable
specialization carriers. Bind them into the exact host target before generation and retain their positions, types
and values in the resolved identity. Runtime arguments retain their original order. Null constants are not bound
by this specialization. Preserve exact target error mapping and logical carriers; do not introduce arithmetic
semantics, function identities or query shapes into the evaluator.

The integral arithmetic provider explicitly registers this call-aware implementation alongside its existing batch
implementation. Literal calls may use the generated target; ordinary calls retain the original batch binding.
The registry's explicit-call/native/fallback precedence is unchanged. This fills primitive-literal coverage in the
existing provider-owned model, rather than replacing ADR0071's authoritative exact provider selection.

## Qualification

Cover literal position/order and identity separation, strict nulls, selected errors, zero divisors and overflow,
integral logical types and physical encodings. Existing call-aware functions and decimal guards must remain correct.
Use matched local and remote SQL comparisons, including ordinary benchmark neighbors. Retain the production
change only if qualified; otherwise remove it at its introducing mutable commit. No session switch or opt-in.

Evidence is retained in `2026-09-13-scalar-literal-binding-investigation.md` and its referenced run artifacts.

## Qualification outcome

Literal binding alone improved remote q242 CPU186.237→158.456s and q243384.915→303.766s. A later identical
literal-only repeat measured 151.296 s and 282.797 s, so do not attribute between-JVM variation to another code change.
Local ordinary-query controls show overlapping CPU ranges forq15/q20/q72/Hq09/q13/q21/CBq39. Paired allocation
measurements expose a3–4% increase; this is a cost of the chosen calling path, not an allocation-free claim.

The attempted scratch-removal and primitive-merge companions were rejected under ADR0121/0123. They are not part
of this implementation. Retain literal binding only with the original scalar failure-containment implementation.
The final exact/native SQL gate passed all 411 queries: 43 ClickBench, 22 TPC-H, 103 TPC-DS and 243 Engine Coverage.
The restored implementation also passed 2,164 Nitro tests (zero failures/errors; 394 skipped) and 164 focused Cork
integration tests. Function semantics remain owned by the selected provider; the engine does not implement integral
arithmetic rules. The refreshed full performance board is a subsequent breadth measurement, not evidence claimed
by the focused comparisons above.
