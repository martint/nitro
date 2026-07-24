# I64 projection capability cohort

Date: 2026-07-24

This slice moves projection lowering for `add`, `subtract`, `multiply`, and
`eq` off their hot scalar implementations and into separately constructed
function capabilities. `lt` was migrated in the immediately preceding slice.
The equality provider now owns both static equality pushdown and its
provider-authored projection program. Arithmetic functions each register an
adjacent provider; there is no engine function-name or implementation-class
catalog.

The exact parent is `ecaace10`. All qualification uses JDK 26, one unpinned
JMH thread, a 12 GB heap cap, transparent huge pages, 10x1s warmup, 5x1s
measurement, steady-state allocation, and the fixed eight-event `perfnorm`
bundle.

## ClickBench q20/q43

Artifacts:

- `candidate-q20-q43-3fork.json`
- `parent-reverse-q20-q43-3fork.json`

Both revisions sampled only q20's documented slow compiler state. Candidate
versus parent is 134.675 vs 134.858 ms, -0.23% instructions, +0.11% cycles,
+0.44% L1D misses, -0.50% L1D loads, effectively flat dTLB misses, -0.35%
branch misses, and -0.15% branches. Candidate allocation is higher because
the parent sampled two lower-allocation compiler modes; this distribution is
already documented and is not filtered.

Q43 candidate is 118.185 vs 120.362 ms and improves instructions, cycles, L1D
misses/loads, dTLB misses/loads, branch misses, and branches. It samples both
known allocation modes.

## TPC-DS representative cohort

Artifacts:

- `candidate-tpcds-q04-q18-q44-q64.json`
- `parent-reverse-tpcds-q04-q18-q44-q64.json`
- `candidate-tpcds-q18-3fork.json`
- `parent-reverse-tpcds-q18-3fork.json`

The one-fork q04 and q64 controls improve duration by 1.07% and 4.08% and
improve nearly every high-volume counter. Q44 is +0.33% duration while
instructions and L1D loads improve 1.90% and 2.88%.

The initial q18 screen appeared adverse at +2.58% instructions and +6.69% L1D
loads, so it was escalated rather than averaged into the favorable cohort.
Across the required three-fork candidate/reverse-parent pair, q18 is +1.43%
duration, -0.36% allocation, +0.02% instructions, +0.12% cycles, -2.00% L1D
misses, +0.22% L1D loads, -1.87% dTLB misses, -10.58% dTLB loads, -2.81%
branch misses, and -0.50% branches. The stable retired-work counters are
neutral.

This is an architecture qualification and does not replace published board
rows. The retained migration ratchet prevents these five I64 hot scalar
classes from implementing optimizer capability interfaces. Direct projection
provider discovery remains only for the unmigrated compatibility cohort.

The full JDK 26 lifecycle gate passed 1,336 tests with zero failures/errors
and 566 skips. No JFR artifacts were created.
