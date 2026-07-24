# Boolean projection capability separation — retry

Date: 2026-07-24

This retries moving `and`, `or`, and `not` projection lowering from hot scalar
implementations into separately constructed registry capabilities. The exact
parent is `d5537a4a`, which completes q67's null-test projection shape and
removes the compilation blocker documented in the original rejected
`20260724-boolean-projection-capabilities` experiment.

All measurements use JDK 26, one unpinned JMH thread, a 12 GB heap cap,
transparent huge pages, 10x1s warmup, 5x1s measurement, steady-state
allocation, and the fixed eight-event `perfnorm` bundle.

The retry is rejected and the boolean source migration is fully reverted.

With all three boolean providers separated, the initial four-query pair no
longer reproduced the old q67 catastrophe. Duration ranged from -0.62% to
+0.79%, and q67 measured 137.2B instructions / 51.9B L1D loads rather than the
rejected attempt's approximately 151B / 64.8B. A three-fork q40/q54/q67 pair
confirmed neutral or better duration:

| Query | Duration | Allocation | Instructions | Cycles | L1D loads | Branches |
|---|---:|---:|---:|---:|---:|---:|
| ClickBench q40 | +0.02% | +6.44% | -0.57% | -0.28% | -0.62% | -0.41% |
| TPC-DS q54 | -0.51% | +0.13% | +0.65% | -0.30% | +0.44% | +0.40% |
| TPC-DS q67 | -4.19% | +0.00% | +1.11% | -5.65% | +2.31% | +1.15% |

Q40's allocation regression was stable across all candidate forks (188–192
MB/op) versus all parent forks (177–180 MB/op). It is approximately one
boolean-sized intermediate over the surviving rows. The existing
thread-allocated-byte operator profiler did not identify a deterministic stage:
when enabled with shared JMH-like allocator/registry resources, candidate and
parent allocated essentially the same bytes at every wrapped boundary. The
instrumentation therefore suppresses the allocation mode rather than
explaining it.

A final bisection restored only `AndBoolean`'s direct provider while leaving
the unused `or`/`not` capabilities separated. Q40 then became dramatically
unstable: three-fork mean 768.1 ms versus 625.2 ms for exact parent, with
individual iterations from 618.6 to 1050.6 ms. Instructions rose from 10.31B
to 11.00B, cycles from 3.47B to 4.30B, and L1D loads from 4.55B to 4.78B.
Thus neither a full nor partial boolean migration is safe, and preserving
selected class shapes would be another cargo-cult workaround.

Q40's relevant projection is comparison / `and` / `if_utf8`. Its variable-width
root cannot enter the current fused projection backend. Completing q67 removed
one exposed compiler-layout dependency; q40 now demonstrates the same issue at
the remaining variable-width generated/interpreted boundary. The next
architectural fix is general provider-authored variable-width projection
support with explicit sizing/copy semantics—not function-name admission or
dummy interfaces/classes.

Artifacts:

- `candidate-cohort.json`, `parent-reverse-cohort.json`
- `candidate-q40-q54-q67-3fork.json`,
  `parent-reverse-q40-q54-q67-3fork.json`
- `and-direct-q40-3fork.json`

No published board row changes.
