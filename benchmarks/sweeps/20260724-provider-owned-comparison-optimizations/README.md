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

The rollback is diagnostic, not an exception to the architecture. The
remaining `lt` recognition in `PlanEvaluator` is forbidden debt. A follow-up
must compare the fast and slow compiled modes and isolate interface shape,
descriptor metadata, lookup, per-batch allocation, construction-time decoding,
and generated-plan binding effects. The endpoint remains registry-owned
semantics lowered before execution with no function identity in the evaluator;
preserving incidental source shape without explaining the compiler behavior is
not an acceptable solution.

The retained equality-only slice is qualified by
`candidate-metadata-q20-3fork.json` against
`parent-q20-reverse-3fork.json`. Both exhibit the same one-fast/two-slow
compiler distribution. Candidate versus parent is +0.52% duration, -0.73%
allocation, +0.10% instructions, +0.73% cycles, +0.19% L1D misses, +0.46%
L1D loads, +0.28% dTLB misses, +0.62% dTLB loads, -0.18% branch misses, and
+0.57% branches. The confidence intervals overlap; no stable high-volume
counter moves by 1%.

## Follow-up: structural provider-owned range lowering

The rejected variants above were diagnostics, not the final result. A
subsequent implementation lowers registry-provided range metadata into a
structural physical mask before execution. `PlanEvaluator` now sees an opaque
input, bounds, and kernel; it does not recognize `lt`, comparison arity,
operand roles, literal placement, or integer vector carriers. Those details
belong to `LessThanI64RangeOptimization`, which is registered as a function
capability separately from the hot scalar implementation. Merely naming a
function `lt` grants no range semantics, and an aliased function with the
capability is lowered.

This investigation also invalidated the earlier causal interpretation of
q20's approximately 105 ms mode. In the final candidate, a single JVM moved
from roughly 135 ms to 110 ms during measurement; another stayed near 109 ms,
and another stayed near 133--139 ms. In earlier warmups the direction was
reversed: roughly 110 ms iterations were followed by roughly 135 ms
iterations. Allocation was stable within those transitions. The exact parent
also alternates between these modes across otherwise identical invocations.
Full `LogCompilation` and lightweight compilation logging perturb or delay the
transition and produced only the slower mode. Therefore, whether one of three
forks happens to capture 105--110 ms cannot be attributed to provider
placement or evaluator function knowledge.

The final paired artifacts are:

- `candidate-structural-provider-range-final-q20-q43-3fork.json`
- `parent-structural-provider-range-reverse-q20-q43-3fork.json`
- `candidate-structural-provider-range-q43-confirmation-3fork.json`

Candidate q20 averages 124.391 ms versus the exact `e3152da7` parent at
133.764 ms, but the mixed compiler modes make the aggregate descriptive, not
a stable source delta. The first q43 candidate invocation averages 122.386 ms
versus the parent at 119.815 ms. The immediate three-fork confirmation averages
119.586 ms, with 2.311B instructions, 0.668B cycles, 44.852M L1D misses,
22.398K dTLB misses, 1.347M branch misses, and 0.541B branches. This overlaps
the parent duration and slightly improves instructions, cycles, branch misses,
and branches; L1D/dTLB load estimates remain somewhat adverse and noisy.

Q43 allocation is also multimodal. The parent produced both approximately
89--90 MB/op and 98 MB/op forks, while earlier equality-only candidate forks
produced approximately 87--89 MB/op and 101 MB/op. The final structural
candidate samples happened to remain near 98--100 MB/op. This must not be
reported as a board improvement, filtered, or explained as an abstraction
cost without compiler evidence. It is a follow-up compiler/escape-analysis
target.

The retained architectural conclusion is nevertheless firm: the evaluator
cannot recover semantics from a function name. An architecture test now
prevents `"lt"`, `LessThanI64`, `constantBound`, and the old `RangeFusion`
recognizer from returning to `PlanEvaluator` or `FilterOperator`. Provider
kernels may ultimately be generated through the Classfile API, but their
semantic contract and lifecycle remain registry-owned; the evaluator invokes
only the generic structural protocol.

The final JDK 26 gate passed 1,336 tests with zero failures/errors and 566
skips. No JFR artifacts were created. The large raw `LogCompilation` XML files
were removed after their findings were summarized here; the compact JMH JSON,
unified compilation logs, negative controls, and paired qualification
artifacts remain.
