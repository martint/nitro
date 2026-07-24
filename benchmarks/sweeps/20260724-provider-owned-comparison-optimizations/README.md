# Provider-owned comparison optimization qualification

Date: 2026-07-24

This slice removes equality-function identity, arity, and operand semantics
from `FilterOperator`. Dynamically registered functions may expose an optional
physical capability for static long equality pushdown. The engine discovers
that capability from registry metadata attached to the resolved primitive
binding; a function name neither grants nor prevents the optimization.

Qualification compares this candidate with its exact adjacent parent
`0f9e5d18`. Both builds use JDK 26, one unpinned JMH thread, a 12 GB heap cap,
transparent huge pages, 10x1s warmup, 5x1s measurement, one fork,
steady-state allocation, and the fixed eight-event `perfnorm` bundle.

An attempted migration of `PlanEvaluator`'s constant-long range fusion to the
same provider pattern was rejected. The direct hot-class implementation lost
q20's 105 ms compiled mode. Moving the provider into descriptor metadata
restored that mode, but q43 still measured 123.225 ms versus 118.688 ms for the
exact parent across three forks, with +4.52% allocation, +1.21% instructions,
and +3.97% cycles. Moving bound decoding to evaluator construction restored a
118.664 ms one-fork screen, but the required three-fork result remained
123.225 ms and retained adverse counters. All rejected artifacts are preserved
here; the range change is fully backed out.

The retained equality-only slice is qualified by
`candidate-metadata-q20-3fork.json` against
`parent-q20-reverse-3fork.json`. Both exhibit the same one-fast/two-slow
compiler distribution. Candidate versus parent is +0.52% duration, -0.73%
allocation, +0.10% instructions, +0.73% cycles, +0.19% L1D misses, +0.46%
L1D loads, +0.28% dTLB misses, +0.62% dTLB loads, -0.18% branch misses, and
+0.57% branches. The confidence intervals overlap; no stable high-volume
counter moves by 1%.
