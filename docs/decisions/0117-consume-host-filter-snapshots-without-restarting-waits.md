# ADR-0117: Consume host filter snapshots without restarting waits

- **Status:** rejected; blanket source-wait removal loses pruning
- **Date:** 2026-09-13
- **Spec:** §13, §16, §18
- **Clarifies:** [ADR-0025](0025-use-one-logical-dynamic-filter-lifecycle.md)
- **Evidence:** `~/notes/nitro/2026-09-13-post-board-regression-investigation.md`

## Context and architectural correlation

The current Hive adapter interprets DynamicFilter.isAwaitable as a mandatory source-readiness barrier. The host SPI
actually describes permission to await a narrower filter. Cork's ConnectorAwareSplitSource separately applies its
finite wait budget; the worker-local filter remains awaitable until all domains arrive and advertises a preferred
timeout of zero. Waiting again in every Nitro source can delay reading after the host has already released splits.
Local q20 pipeline envelopes show about 24 ms of source input-blocked time versus zero in Cork at equal row counts.
The envelope identifies a hypothesis, not yet the specific future responsible for that entire gap.

This corrects an implementation interpretation under ADR-0025, not the host scheduler or filter authority. It also
corrects the empirical principle that equated isAwaitable with a host instruction to block. Any accepted change
must state that correction explicitly; old evidence of pruning benefits from waiting is not invalidated by it.

## Proposed decision

Once the host dispatches a split, install its current safe filter snapshot before the first read without adding a
new wait. Continue accepting narrower snapshots for unread data through the existing source capability and reader
lifecycle rules. Static predicates and exact join enforcement are unchanged. Do not add a timeout, feature flag,
query-specific policy, or modify host split scheduling. Never treat an incomplete build's raw domain as a safe
predicate: only consume the host's published getCurrentPredicate contract.

## Evaluation gate

Test pending filters, incremental narrowing, completion, static predicates, unsupported columns and delegate
blocking. Compare q20 with completed pipeline evidence and warmed A/B/A runs, then check dynamic-filter-sensitive
queries for result, pruning, CPU and latency regressions. A lower wait alone does not justify increased total work.
Retain no inactive experiment in production if the proposal is rejected.

## Initial evidence

The original implementation fails two new contract tests; the candidate passes all 17 adapter tests. Local q20
with 100 warmups and 30 measurements in parent/candidate/parent order gives median CPU109/108/108ms and
latency102/83/102ms, with overlapping ranges. Source input-blocked time becomes zero and physical input rows/bytes
are identical. Peak reservation medians22.4/56.6/22.3MB expose increased overlap/retention and must remain visible
beside the latency benefit. This supports attribution of q20's source wait but is not sufficient for acceptance.
The q23a/q23b historical premature-scan cliff makes those queries mandatory breadth checks, along with q72.

## Rejection

The q23a breadth check reproduces that cliff: median CPU 5.156 s becomes 14.442 s, latency 1.360 s becomes 3.462 s,
allocation 5.256 GB becomes 24.138 GB and peak reservation 0.445 GB becomes 1.965 GB. Every measured candidate run reads
184,382,306 physical positions versus 155,479,315 before (29 million additional positions); all those ranges are
separated. The candidate is rejected and both implementation and candidate-specific tests are removed. Retain the
proposal and patch as evidence, not a production opt-out or dormant path. Remaining paired breadth checks only
characterize the rejected proposal; they cannot override this established regression.

The SPI observation remains valid: awaitability alone is not a mandatory scheduling instruction. However, simply
removing the source barrier is not performance-equivalent to the host's full execution dependency model. A future
proposal must preserve timely exact pruning while recovering overlap, and make that choice explicit under ADR0025.
The accepted source behavior and SPEC remain unchanged by this rejected experiment.

The full parent/candidate/parent breadth check confirms rejection: q23a CPU5.156/14.442/5.144s,
q23b5.107/15.839/5.108s, q724.122/6.427/4.194s. All candidate CPU and latency ranges are separated from
both parents. q72 physical input positions are unchanged, illustrating that equal scan positions do not establish
equal downstream work. The restored Hive adapter passes all16 original tests and its main class is byte-identical
to the parent snapshot. No source-wait code change is committed to Cork.
