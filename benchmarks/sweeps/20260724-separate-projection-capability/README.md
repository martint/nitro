# Separate projection capability qualification

Date: 2026-07-24

This slice makes `ProjectionCodeProvider` a dynamically registered
`FunctionCapability` and moves `lt` projection lowering from the hot
`LessThanI64` scalar implementation into the separately constructed
`LessThanI64RangeOptimization` capability. The same provider now owns the
function's projection program and range-bound semantics. `PrimitiveRegistry`
prefers capability metadata while temporarily retaining direct-interface
discovery for built-ins not yet migrated.

The engine and projection compiler do not recognize the function name. A test
registers a provider under a name unknown to the engine and proves that the
generated projection path is selected. An architecture ratchet prevents
projection/range provider interfaces from returning to the hot `LessThanI64`
implementation.

All controls use JDK 26, one unpinned JMH thread, a 12 GB heap cap, transparent
huge pages, 10x1s warmup, 5x1s measurement, steady-state allocation, and the
fixed eight-event `perfnorm` bundle. The exact parent is `75e3dbd5`.

Artifacts:

- `candidate-q20-q43-screen.json`
- `parent-reverse-q20-q43-screen.json`
- `candidate-q20-q43-3fork.json`
- `parent-reverse-q20-q43-3fork.json`
- `candidate-q43-confirmation-3fork.json`

Q20 remains in the already documented compiler modes. The candidate's three
forks all remained in the 131--134 ms mode and averaged 132.606 ms. The parent
sampled two 133--134 ms forks plus one transient 109.5 ms fork, averaging
125.632 ms. Comparing the slow states, the capability form is neutral to
slightly better; the parent aggregate is not a causal source comparison.

The first q43 candidate set averaged 120.509 ms versus the reverse parent at
119.300 ms. The immediate candidate confirmation averaged 119.976 ms. Its
duration interval overlaps the parent. Versus the parent aggregate, the
confirmation has -0.14% instructions, +1.38% cycles, -0.96% L1D misses,
-0.31% L1D loads, +3.87% dTLB misses, -1.65% dTLB loads, +4.16% branch
misses, -0.24% branches, and -2.52% allocation. The miss-counter error bars
are much larger than those deltas; stable retired-work counters are neutral.

This is architecture qualification only. It does not replace a published
board row. The retained direction is that optional code-generation semantics
belong to constructed registry capabilities, not additional interfaces on hot
scalar implementations. Remaining built-ins still using the compatibility
path are explicit migration debt.
