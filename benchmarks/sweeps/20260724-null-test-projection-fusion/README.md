# Null-test projection fusion qualification

Date: 2026-07-24

This experiment completes the registry-provided generated-projection path for
TPC-DS q67's mixed-width null-test / boolean / multiply / conditional shape.
The exact parent is `d8efe3cd`.

All measurements use JDK 26, one unpinned JMH thread, a 12 GB heap cap,
transparent huge pages, 10x1s warmup, 5x1s measurement, steady-state
allocation, and the fixed eight-event `perfnorm` bundle.

The retained implementation adds a provider-visible `NULLS_ONLY` input shape.
`is_null_i32` and `is_null_i64` share one registry-owned projection capability
whose generated program reads only nullness; it does not claim a logical or
physical value type. The compiler merges a null-only use with a value use of
the same input. Thus q67's I32/I64 inputs are loaded because its multiplication
consumes them, while q97's null-only dictionary inputs are not gathered.

A first long-carrier version exposed why this distinction matters. It improved
q67 but regressed q97 because the generated kernel gathered dictionary values
that the null tests never read. The `NULLS_ONLY` version removed that work and
changed q97 from +5.31% to -4.22% duration in the corresponding screens.

Q01 then reproducibly regressed in a three-fork pair: +1.84% duration, +1.10%
instructions, and +2.09% cycles despite -4.70% allocation. Its generated slice
had only two operations and read only null streams. The retained generic cost
rule therefore leaves a two-step all-null-only slice on the interpreter; three
or more steps, or a slice with any value-consuming input, retains the normal
two-step fusion threshold. This is a physical-shape rule, not a function or
query exception.

Final affected-cohort result versus the reverse-order exact parent:

| Query | Duration | Allocation | Instructions | Cycles | L1D loads | Branches |
|---|---:|---:|---:|---:|---:|---:|
| q01 | -0.03% | +0.03% | -0.12% | -0.27% | -0.44% | +0.38% |
| q40 | +0.07% | -4.37% | -1.10% | +0.50% | +1.14% | -1.22% |
| q49 | +0.51% | -1.27% | +0.77% | +2.05% | +1.13% | +0.66% |
| q67 | -9.25% | -0.03% | -9.05% | -6.62% | -13.93% | -6.11% |
| q77 | -3.31% | -1.22% | -0.52% | -1.15% | -1.25% | -0.49% |
| q80 | -3.02% | -1.90% | -1.19% | -1.80% | -2.07% | -1.14% |
| q97 | -10.21% | -18.12% | -13.65% | -2.24% | -12.48% | -14.07% |

The seven-query geometric-mean duration improves 3.69%; sum of durations
improves 8.37%. Final q67 also improves dTLB misses/loads 25.99%/16.85%, while
L1D misses (+1.33%) and branch misses (+3.86%) are adverse small-counter
movements. Other small one-fork miss/load movements remain mixed and are
retained in the JSON rather than summarized away.

Artifacts:

- `candidate-q67.json`, `parent-reverse-q67.json`: initial long-carrier screen
- `candidate-q67-3fork.json`, `parent-reverse-q67-3fork.json`: long-carrier
  q67 confirmation
- `candidate-affected-cohort.json`, `parent-reverse-affected-cohort.json`:
  long-carrier affected cohort
- `candidate-nulls-only-affected-cohort.json`: first null-only cohort
- `candidate-nulls-only-q01-3fork.json`,
  `parent-reverse-q01-3fork.json`: q01 cost-model diagnosis
- `candidate-final-affected-cohort.json`: retained implementation

These are architecture-slice controls, not a replacement complete-board
sweep. No published board row changes.
