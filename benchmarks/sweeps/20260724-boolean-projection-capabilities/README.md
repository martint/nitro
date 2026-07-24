# Boolean projection capability migration — rejected

Date: 2026-07-24

This experiment attempted to move `and`, `or`, and `not` projection lowering
off the hot scalar implementations and into separately constructed registry
capabilities, following the qualified I64/F64 pattern. The implementation was
correct and focused tests/checkstyle passed, but the performance gate found a
large TPC-DS q67 regression. The code migration is fully reverted.

The exact parent is `e7420993`. All measurements use JDK 26, one unpinned JMH
thread, a 12 GB heap cap, transparent huge pages, 10x1s warmup, 5x1s
measurement, steady-state allocation, and the fixed eight-event `perfnorm`
bundle.

Artifacts:

- `candidate-q40-q21-q54-q67.json`
- `parent-reverse-q40-q21-q54-q67.json`
- `candidate-q67-confirmation.json`
- `and-not-only-q67-recovery.json`
- `and-only-q67.json`
- `parent-q67-confirmation.json`

The initial cohort was neutral or better on ClickBench q40 and TPC-DS q21/q54.
Q67, however, regressed 9.93% duration, 18.90% instructions, 7.97% cycles,
41.41% L1D loads, 15.89% branches, and 4.36% dTLB loads. A candidate
confirmation reproduced the high-work state at 151.4B instructions and 64.8B
L1D loads. Exact-parent confirmation remained in the low-work state at 129.0B
instructions and 45.7B L1D loads.

Q67's relevant projection contains `is_null_i64`, `is_null_i32`, `or`,
`multiply`, and `if_i64`. It cannot currently enter the fused projection path
because the two null tests have no projection lowering. The experiment then
restored `OrBoolean`, leaving only separately registered `and`/`not`
capabilities. Q67 remained slow despite referencing neither migrated function.
Restoring `NotBoolean` as well and leaving only the unused `AndBoolean`
capability still produced 151.0B instructions, 64.8B L1D loads, and 29.2B
branches.

This establishes that the regression is not boolean semantics, provider lookup
on q67's runtime path, or one specific boolean function. Changing the
registered capability/class population perturbs q67's generated-code/JIT
layout enough to select a materially worse compilation shape. Preserving the
old direct provider is a diagnostic rollback, not an architectural preference.

The next attempt must identify and stabilize q67's compilation dependency—most
likely by completing a generated projection shape for its null-test/boolean/
multiply/conditional chain or by otherwise making generated-code/class-loading
order irrelevant. Adding dummy interfaces/classes to reproduce the favorable
layout would be cargo culting and is forbidden. No published board row changes.
