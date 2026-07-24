# F64 projection capability cohort

Date: 2026-07-24

This slice moves projection lowering for F64 `add`, `subtract`, `multiply`,
`eq`, `lt`, `gt`, `lte`, and `gte` off their hot scalar implementations and
into separately constructed registry capabilities. A provider-side
`BinaryF64ProjectionOptimization` scaffold supplies only the common
null-propagating binary F64 program shape. Each registered capability supplies
its own arithmetic or comparison operation; neither the evaluator nor the
projection compiler contains a function catalog.

The exact parent is `4fd24976`. All qualification uses JDK 26, one unpinned
JMH thread, a 12 GB heap cap, transparent huge pages, 10x1s warmup, 5x1s
measurement, steady-state allocation, and the fixed eight-event `perfnorm`
bundle.

Artifacts:

- `candidate-tpch-q01-q06-q19.json`
- `parent-reverse-tpch-q01-q06-q19.json`
- `candidate-tpch-q01-3fork.json`
- `parent-reverse-tpch-q01-3fork.json`

The one-fork representative control covers q01 arithmetic projection, q06
range comparisons and multiplication, and q19 multi-range comparison
structure:

- q01 improves duration 2.46%, cycles 2.37%, and L1D loads 1.08%. Allocation
  is +1.14% and dTLB loads are +4.50%, so q01 was escalated.
- q06 is +0.49% duration with allocation -0.92% and stable high-volume
  counters within 0.5%.
- q19 improves duration 2.65%, allocation 16.19%, instructions 2.38%, cycles
  3.13%, L1D misses/loads about 2.5%, branch misses 4.13%, and branches 2.40%.

Across the three-fork q01 candidate/reverse-parent pair, candidate is -0.34%
duration, allocation flat, +0.02% instructions, -0.33% cycles, +0.11% L1D
misses, -0.16% L1D loads, +1.67% dTLB misses, +5.28% dTLB loads, -0.46%
branch misses, and +0.04% branches. The dTLB load estimate remains explicit
adverse evidence; it is low volume relative to 18.5B L1D loads and does not
correspond to adverse duration or stable retired work.

This is architecture qualification only and does not replace published board
rows. The architecture ratchet prevents the migrated I64 and F64 hot scalar
classes from implementing optimizer capability interfaces. Boolean,
conditional, and UTF providers remain separate migration cohorts.

The full JDK 26 lifecycle gate passed 1,336 tests with zero failures/errors
and 566 skips. No JFR artifacts were created.
