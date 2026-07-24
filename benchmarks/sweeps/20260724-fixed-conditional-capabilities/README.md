# Fixed-width conditional projection capabilities

Date: 2026-07-24

This moves `if_i64` and `if_f64` projection lowering from scalar execution
classes into separately constructed registry capabilities. The semantic
programs are unchanged; the evaluator continues to compose only generic
physical projection IR. The exact parent is `d6d3ea7c`.

All measurements use JDK 26, one unpinned JMH thread, a 12 GB heap cap,
transparent huge pages, 10x1s warmup, 5x1s measurement, steady-state
allocation, and the fixed eight-event `perfnorm` bundle.

Separating only the fixed-width conditionals did not qualify. The initial
three-fork pair was:

| query | duration | allocation | instructions | cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | branch misses | branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-H q12 | +0.74% | -0.25% | +0.54% | +0.78% | -0.28% | +0.04% | -0.02% | +5.17% | -0.52% | +0.22% |
| TPC-H q14 | +0.40% | +1.51% | +0.13% | +0.26% | -2.74% | -0.09% | -1.37% | +0.36% | +0.59% | +0.11% |

Q14's physical slice stopped at unsupported `starts_with_utf8`. The retained
implementation adds a generic UTF-8 byte-prefix primitive to the
classloader-neutral projection builder; the function provider owns the
lowering, and the engine has no function-name case. This completes
`starts_with_utf8 → if_f64` fusion. Its three-fork q14 result versus the same
exact parent is:

| metric | delta |
|---|---:|
| duration | -1.46% |
| allocation | -0.45% |
| instructions | -1.60% |
| cycles | -1.17% |
| L1D misses | -1.80% |
| L1D loads | -1.82% |
| dTLB misses | -0.73% |
| dTLB loads | +2.48% |
| branch misses | -32.21% |
| branches | -1.46% |

Separating UTF-8 equality and set-membership projection lowering from their
scalar/mask execution classes completes the provider boundary used by q12.
The final q12 three-fork result is duration +0.31%, allocation -0.91%,
instructions +0.11%, cycles +0.28%, L1D misses -0.31%, L1D loads +0.50%,
dTLB misses +0.59%, dTLB loads +2.35%, branch misses +1.67%, and branches
-0.23%. The final one-fork q14 confirmation after those class changes remains
strongly positive (1170.361 ms versus the parent's 1194.074 ms).

No scalar execution class now implements `ProjectionCodeProvider`; every
projection program is supplied through a separately constructed registry
capability. Mask evaluation remains on the scalar classes for now and is a
separate protocol/design slice.

Artifacts:

- `candidate-screen.json`, `parent-reverse-screen.json`
- `candidate-q12-q14-3fork.json`, `parent-reverse-q12-q14-3fork.json`
- `candidate-q14-prefix-3fork.json`
- `candidate-q12-utf8-separated-3fork.json`
- `candidate-final-q14.json`

This is an architecture qualification, so no published board row changes.
