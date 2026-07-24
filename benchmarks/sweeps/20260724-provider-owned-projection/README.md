# Provider-owned projection lowering architecture control

This control covers the architecture slice that removes function-name lowering
from `FusedProjectionCompiler`, lets dynamically registered providers supply
their own physical projection programs, moves the compiler cache under
`OperatorCodeGenerationResources`, and moves the generic `VectorAccess` helper
from the builtin-function package to the data layer. It does not replace any
published board row.

## Method

- JDK `/opt/java/openjdk` (26)
- Nitro operator harnesses only, unchanged from the Trino-SQL-derived shapes
- unpinned, one JMH thread, 12 GiB heap, transparent huge pages
- candidate first, exact parent `742e8992` from a separate jj workspace second
- allocation and all eight hardware counters captured in every invocation
- initial screen: one fork, 10 x 1 s warmup, 5 x 1 s measurement
- escalated q39/q43: three forks with the same warmup and measurement policy

Positive deltas mean the candidate used more than the reverse-order parent.

| Control | Candidate/parent ms | Duration | Allocation | Instructions | Cycles | L1D misses | L1D loads | dTLB misses | dTLB loads | Branch misses | Branches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TPC-H q01, one fork | 1781.3 / 1770.3 | +0.62% | -0.73% | -0.37% | +0.65% | +0.79% | -0.87% | +0.78% | -0.23% | +1.56% | +0.07% |
| ClickBench q43, three forks | 118.9 / 123.1 | -3.42% | -7.69% | -2.22% | -2.24% | -1.00% | -2.34% | -0.71% | -22.17% | +2.22% | -2.87% |
| TPC-DS q39, three forks | 465.6 / 516.0 | -9.76% | +0.21% | -2.37% | -7.25% | -7.90% | -2.00% | -18.73% | -12.73% | -6.81% | -2.07% |

The one-fork q39 screen initially selected its known slow placement mode and
looked 9.0% slower. The required three-fork escalation reversed that result:
all ordinary-work and hardware-counter metrics improved, with allocation flat.
Fork means were 472.6/455.7/468.6 ms for the candidate and
525.2/530.5/492.3 ms for the parent.

Q43 improved duration, allocation, instructions, cycles, L1D work, dTLB work,
and total branches. Its 2.22% branch-miss increase is a small isolated adverse
movement and is not claimed as an architectural performance win. Q01 is
neutral within the short screen. There is no stable broad regression, but the
q39 placement sensitivity remains an architectural defect to eliminate.

The matching JSON files contain machine-readable duration, allocation, and all
counter results. Logs contain the complete JMH/JVM configuration and raw
iterations. No JFR was used.
