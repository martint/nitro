# ADR-0133: Copy primitive vector selections without boxing

- **Status:** accepted
- **Date:** 2026-09-13
- **Spec:** §5, §6, §11, §16, §18
- **Related:** ADR-0041, ADR-0131/0132

## Evidence

After the projected-batch recycling fix, an allocation profile still attributes 42% of sampled allocation weight to
Integer objects. The dominant caller is PlanEvaluator.copyVector delegating to BooleanVector.copyMasked, not one of
the evaluator's own position loops. I32, I64, F64 and ErrorVector masked copies use the same boxed iteration form.
Instrumentation alters allocation bands; sampled percentages are not exact unprofiled byte attribution.

## Proposal and correlation

Use exact dense-prefix array copying for fully selected I32/I64/F64/boolean masks and primitive iteration for their
sparse masks. Copy mask.size positions, not backing capacity; a fully selected logical prefix can be smaller than
the vector. Empty masks do not access mutable target storage. Resolve the target array once outside sparse loops.
ErrorVector keeps its diagnostic-aware per-position copy body with primitive traversal. Preserve allocation/growth,
inactive positions, physical floating-point bits and rich/presence-only error diagnostics. This conforms to the
selected-position and row-proportional allocation contracts. It adds no type cases to consumers, function semantics,
new interfaces or enabling flag. Density here is an exact mask property, not a speculative semantic inference.

## Rejected initial form

The iterator-only form passed all 2,176 Nitro and 45 focused host tests and removed the profiled BooleanVector copy
boxing site. Local q242/q243 CPU ratios were 0.8940/0.9282 versus the low-allocation R14 repeat. Remote ratios were
1.1404/1.1471 with no meaningful allocation benefit. It did not qualify and is replaced in this mutable proposal by
the explicit dense/sparse form above. Keep those frozen binaries and reports as evidence; do not publish the local
wins alone or claim the JVM reason for the remote CPU difference has been established.

## Qualification

Retain the revised dense/sparse implementation. It passed the tests below, exact/native SQL checks, and the
twelve-query remote macro screen with no separated CPU regression (b05 improves 1.7% with separated ranges).
Against the first R14 control, remote q242 is 1.9% higher CPU with separated within-fork ranges and q243 overlaps.
A fresh identical R14 control changes from 131.209/182.903 to 152.763/213.677 CPU-seconds; revised copies measure
133.725/180.607. Local primary ranges overlap the original control. Thus neither the initial apparent 14% regression
nor the later apparent 12–15% improvement establishes a causal SQL effect from this change. Keep both control forks
and the rejected variant's evidence. The retained justification is exact dense-copy efficiency and removal of a
profiled composition-dependent boxing site, not a claimed universal end-to-end speedup or allocation-free query.
The combined recovery stack still receives a full functional gate and synchronized board.

The revised dense/sparse form passes all 2,176 Nitro and 45 focused host tests. A two-fork component screen at
8,192 positions found Boolean dense/prefix copy time ratios of 0.710/0.620, sparse 1.011; I64 ratios were
0.974/0.970/0.989. Both implementations allocate effectively zero bytes per isolated operation. These short
component measurements do not establish SQL performance or disprove composition-dependent boxing in a real query.
The benchmark keeps one allocator domain for its one repeatedly invoked producer and closes it after the trial.

Tests cover empty, sparse and full selections extending beyond cached Integer positions, existing destination reuse,
negative zero/NaN bits and inactive diagnostics. Run full Nitro and focused host tests, then compare frozen R14/R15
q242/q243 and macro guards with identical engine classes and only the five SPI vector classes changed. Verify
classpath precedence explicitly: an older SPI overlay must not hide the candidate. Reject and remove unqualified
production changes, preserving this record and evidence. The published board remains separate.
