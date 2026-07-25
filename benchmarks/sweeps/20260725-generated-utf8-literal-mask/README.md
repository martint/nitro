# Generated UTF-8 literal masks

Date: 2026-07-25

This slice compiles registry-provider `utf8Equal(input, literal)` projection IR
directly to an engine-owned mask program. The evaluator sees only the existing
opaque `CompiledMask` protocol, argument references, and structural null/error
exclusions. It contains no `eq_utf8` identity, UTF-8 branch, fixed arity, or
provider implementation class.

`ProjectionMaskCompiler` binds the planner literal as UTF-8 bytes. A Classfile
API backend emits a hidden matcher whose instructions contain the exact literal
length and bytes. Generic JIT support handles flat, one/nested dictionary, and
single-level RLE physical encodings. Dictionary entries are evaluated once per
batch; a one-match dictionary uses direct ID mask compaction. The reusable
dictionary scratch is owned by the constructed compiled program. No static
pool, cache, or service was introduced.

Correctness:

- focused compiler, architecture, operator, and Parquet tests passed;
- the full suite passed 1,353 tests with zero failures/errors and 566 skipped;
- nullable/error components remain structural exclusions applied generically
  by the evaluator;
- no JFR artifact was produced.

## Qualification

Candidate and exact parent `67bf653a` were each run with JDK 26, one unpinned
JMH thread, `-Xmx12g`, 10x1s warmup, 5x1s measurement, three forks, allocation,
and the fixed eight counters in the same invocation. The parent was rebuilt
from a clean `target/` and run second.

| Query | Duration | Allocation | Instructions | Cycles | L1D misses / loads | dTLB misses / loads | Branch misses / branches |
|---|---:|---:|---:|---:|---:|---:|---:|
| TPC-H q03 | +0.71% | -0.29% | +0.38% | +0.60% | +2.58% / -0.40% | -9.19% / -3.54% | -3.79% / +0.49% |
| TPC-H q16 | +0.77% | -0.11% | -0.38% | -0.23% | -1.42% / -0.77% | -3.15% / -3.91% | -1.46% / -0.09% |
| TPC-H q21 | -2.11% | +0.41% | -5.72% | -2.06% | -0.40% / -4.45% | -1.28% / -9.53% | -0.14% / -5.10% |

For this affected set, candidate/parent duration is -0.22% geometrically and
-1.25% by sum (5,418.990 ms versus 5,487.615 ms). Q21 improves every hardware
counter. Q03 remains mixed and q03/q16 duration are mildly adverse, so these
focused results do not replace any frozen-board row.

Primary evidence:

- `candidate-q03-q16-q21-3fork.json`
- `parent-reverse-q03-q16-q21-3fork.json`

## Rejected follow-ups

The first dictionary version used a boolean match lookup for every logical row.
Recognizing a single matching dictionary entry and compacting directly by ID
removed q16's excess instructions and loads and is retained.

A generated short-literal matcher that called a shared byte-packing helper was
rejected. Compared with the retained unrolled matcher it regressed q03/q16
duration by 3.37%/4.38%, cycles by 3.34%/4.32%, and instructions by
1.53%/7.17%. Moving the loop across that call boundary did not preserve the old
in-class compiled shape.

A flat-only compiled program that declined dictionaries was also rejected. It
removed nearly all of q21's work reduction and added a failed compiled dispatch
before the ordinary provider fallback for q03/q16. Physical-encoding admission
is general, but this particular boundary was not useful.

The published operator-comparison board is unchanged.
