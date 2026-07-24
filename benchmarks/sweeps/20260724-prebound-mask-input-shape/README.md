# Rejected broad prebound mask input shape

Date: 2026-07-24

This retry moved null-test mask input shape into a classloader-neutral provider
and decoded it at `PlanEvaluator` construction. To avoid any registry lookup in
execution, the prototype also prebound every `MaskEvaluablePrimitiveFunction`,
including functions without the new capability, into a new engine-owned
invocation record.

That scope was too broad. It changed the invocation shape for unrelated
comparison predicates and failed the q30 five-fork gate. The implementation is
fully reverted; only evidence remains.

All measurements use JDK 26, one unpinned JMH thread, a 12 GB heap cap,
transparent huge pages, 10x1s warmup, 5x1s measurement, allocation, and the
fixed eight counters in one invocation. The exact parent is `9c1c2bf1`.

Q81 was highly multimodal in the initial three-fork run. Its five-fork
escalation favored the candidate across stable metrics (duration -6.01%,
allocation -0.60%, instructions -1.24%, cycles -3.39%, L1D loads -1.34%,
branches -1.48%), though both variants retained isolated slow iterations.

Q30 rejected the broad binding across five fresh forks:

| Metric | Candidate vs parent |
|---|---:|
| duration | +0.71% |
| allocation | +1.39% |
| instructions | +0.53% |
| cycles | +2.16% |
| L1D misses | +1.52% |
| L1D loads | +0.55% |
| dTLB misses | +4.29% |
| dTLB loads | +6.46% |
| branch misses | +2.06% |
| branches | +0.43% |

The next retry must be capability-selective: calls without
`MaskInputShapeProvider` must retain the exact legacy resolution/invocation
path. Construction-time binding may apply only to participating calls, and its
hot-path admission must not enlarge or replace the path used by unrelated
comparisons.

Artifacts:

- `candidate-q30-q40-q81-3fork.json`
- `parent-q30-q40-q81-reverse-3fork.json`
- `candidate-q30-5fork.json`
- `parent-q30-reverse-5fork.json`
- `candidate-q81-5fork.json`
- `parent-q81-reverse-5fork.json`

The published board is unchanged.
