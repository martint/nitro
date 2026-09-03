# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Nitro is a columnar query evaluation engine in Java. It explores vectorized,
batch-based execution with mask-driven row selection. Beyond synthetic
micro-benchmarks it now reads real Parquet data and runs subsets of the TPC-DS
and ClickBench query suites, benchmarked apples-to-apples against an equivalent
tree of Trino operators.

## Architecture specification and decisions

- `~/notes/specification-decision-record-practice.md` defines the reusable maintenance and correlation protocol.
- `docs/SPEC.md` is the normative architecture contract for Nitro and its Cork/Trino integration. Read the relevant
  section before architecture work. A contradiction is either an implementation defect or a deliberate spec change.
- `docs/decisions/` records why consequential choices were made. Add a numbered decision record when a change selects
  among viable alternatives, changes a hard-to-reverse contract, deliberately accepts a cost, or declines a
  capability. Accepted records are immutable; amendments and reversals create a new record and update both ends plus
  the index.
- A consequential architecture change updates the spec and decision record in the same Nitro commit. A paired Cork
  integration commit cites the Nitro ADR number. Do not leave the current contract only in a dated campaign note.

### Required architecture correlation

Before making a material change to architecture, behavior, physical policy, or a cross-engine boundary:

1. Read and cite the applicable sections of `docs/SPEC.md`, `docs/decisions/`, and
   `~/notes/nitro/design-principles.md`.
2. State whether the change conforms to them, fills an unspecified gap, or contradicts/reverses an existing contract
   or principle. Also inspect proposed ADRs so work does not accidentally implement or foreclose an open decision.
3. Treat contradiction and backtracking as legitimate design outcomes, but never implicit ones. Discuss the conflict
   and make the choice explicitly before implementation. A reversal or correction creates a new ADR that supersedes or
   amends the old record and updates both records plus the index.
4. Land the implementation, spec update, ADR, and any revised empirical principle as one coherent change whenever they
   describe the same decision. A paired Cork commit cites the ADR and must not precede the explicit decision.

Material means more than public API changes: it includes defaults and admission rules that alter which physical path
runs, ownership/lifetime changes, scheduler behavior, function/type semantics, connector or Page/Block boundaries,
and benchmark methodology changes that alter what published results mean.

## Design knowledge base — `~/notes/nitro/`

Durable design principles, learnings, and design decisions live under `~/notes/nitro/`. Read the
relevant notes before starting perf/architecture work, and **keep them current as new principles and
decisions come up** — this is part of the task, not optional cleanup:

- `design-principles.md` — the empirical and performance rules we optimize toward. It complements but does not replace
  `docs/SPEC.md`. When a new
  *generalizable* principle emerges (a lever that works, a class of refuted levers, a methodology, an
  architectural invariant), add it here. When a change violates one, that's usually the bug or the lever.
- `tooling-and-methodology.md` — profiling/benchmark/Velox-harness recipes and Vector-API/JVM caveats.
- Dated `YYYY-MM-DD-*.md` notes — session narratives and design decisions (what was tried, measured,
  kept/refuted, and why). Write one when you close out a non-trivial investigation.

Rules of thumb: a **current architectural contract** → `docs/SPEC.md`; a consequential choice among alternatives → a
`docs/decisions/` record; a **generalizable empirical learning** → a `design-principles.md` entry (or sharpen an
existing one); a **recipe** → `tooling-and-methodology.md`; a **narrative** → a dated note. A one-off query-specific
fix belongs in the auto-memory, not here. If a new learning corrects an earlier note, update that note rather than
leaving both. Prefer editing or sharpening an existing entry over adding a near-duplicate.

## Commands

Java 26 with preview features enabled (`--enable-preview --add-modules
jdk.incubator.vector`). The repo uses Jujutsu (`jj`) for version control.

```bash
# Build (main + tests)
mvn test-compile

# Run all tests
mvn test

# Run a single test class or method
mvn test -Dtest=TestOperators
mvn test -Dtest=TestOperators#testComplex

# Run JMH benchmarks via the exec profile (preferred — sets the right JVM flags).
# benchmark.include is a regex over fully-qualified JMH method names.
mvn -q exec:exec@benchmark \
    -Dbenchmark.include='org.weakref.nitro.tpcds.BenchmarkQueries.query.*$' \
    -Dbenchmark.options="-wi 5 -i 5 -w 1s -r 1s -f 1 -rf csv -rff /tmp/nitro.csv"
```

### Datasets

Parquet inputs are not checked in; they are located via system properties with
home-relative defaults:

- TPC-DS: `-Dnitro.tpcds.parquet.path=<dir>` (default `~/tmp/tpcds-parquet-sf10`),
  scale via `-Dnitro.tpcds.parquet.schema` (default `sf10`).
- ClickBench: `-Dnitro.clickbench.hits.path=<file-or-dir>` (default `~/tmp/clickbench`).

Generate TPC-DS Parquet with the bundled tool (uses Trino's `tpcds` connector as
source and `hive` connector as a local Parquet writer — no network needed):

```bash
mvn -f tools/tpcds-parquet-dumper/pom.xml package
java --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
  -jar tools/tpcds-parquet-dumper/target/tpcds-parquet-dumper-1-SNAPSHOT.jar \
  --output-root <dir> --source-schema sf10 --parquet-compression LZ4
```

## Architecture

### Data Layer (`org.weakref.nitro.data`)

- **`Vector`** — sealed interface; permitted subtypes are `FlatVector`
  (non-sealed, flat array layout), `RleVector` (run-length encoded),
  `DictionaryVector` (dictionary-encoded), and `ConcatenatedBooleanVector`.
- **Flat vectors**: `I64Vector`, `I32Vector`, `F64Vector`, `BooleanVector`,
  `BinaryVector` (UTF-8 / bytes), and the nested `StructVector`, `ArrayVector`,
  `MapVector`. Each carries values plus a separate nulls representation.
- **Aggregation-state vectors** (used by accumulators to carry partial state):
  `SumStateVector`, `CountStateVector`, `AvgStateVector`, `MinUtf8StateVector`,
  `StddevSampStateVector`, `DistinctCountStateVector`.
- **`Mask`** — the set of active row positions in a batch as a sorted `int[]`.
  Factories `all()`, `sparse()`, `range()`; set ops `and`, `andNot`, `or`,
  `complement`, `difference`, `union`. The `all` flag signals no filtering yet.
- **`Allocator` / `VectorAllocator`** — track memory allocation per named
  `Context` for profiling; vectors are allocated through these.
- Supporting types: `Row`, `SelectedPositions`, `Utf8Traits`, `VectorSupport`.

### Operator Layer (`org.weakref.nitro.operator`)

**`Operator` interface** — pull-based, batch-at-a-time iterator:
- `next()` → `Mask` of active row positions for the current batch
- `column(int)` → `Vector` for that column (call after `next()`)
- `constrain(Mask)` — push a narrower mask back to the operator, allowing
  lazy/partial column computation

~64 operator classes, grouped by role:
- **Sources**: `GeneratorOperator` (synthetic data via `operator/generator`),
  `ConstantTableOperator`, `SingleBatchOperator`, and the native Nitro Parquet
  source.
  The engine-facing connector contract is `core.source.BatchSource`, not these
  format-specific classes. Each source constructs stable opaque
  `SourceColumnHandle` instances; runtime-filter support is explicitly checked
  per owned handle, and foreign/provider handles are never inspected by the engine.
  `BatchSourceOperator` receives a constructed `SourceOperatorIngress`; native,
  generated-column, or host-boundary adaptation policy does not belong in the
  source operator or connector implementation. Transitional operator-backed
  source, batch, selection, and runtime-filter adapters are quarantined under
  `operator.source.compatibility`; that package is not connector SPI. Legacy
  Parquet/Hardwood/Trino format operators are quarantined there as bridge debt,
  not treated as general engine operators or connector APIs.
  Generic non-native batches enter through `ColumnViewSourceOperatorIngress`;
  a constructed `ColumnViewOperatorIngressFactory` binds every schema field to
  its registry/code-generated physical importer. The generic assembler owns
  laziness, selection generations, and lifetime, never type or encoding cases.
  `RegisteredColumnViewOperatorIngressFactory` is the immutable composition-root
  registry for those providers; type identities are lookup keys, not engine
  vocabulary, and no provider or cache is process-static.
  Source `Selection` carries the complete physical `positionCount`; never infer
  it from the greatest selected row. Generic mask translation uses the
  constructed, allocator-owned `AllocatedSelectionOperatorIngress`.
  Nitro `Vector` representations and allocator ownership are connector SPI.
  Each `TypeBinding` declares its supported vector classes; `ColumnView`
  exposes borrow/take stream ownership, and `VectorColumnViewOperatorIngress`
  maps those vectors to private operator outputs without connector knowledge of
  `Output`, `Batch`, evaluator IR, or Trino Page/Block types.
  Connectors allocate vectors through a constructed `VectorAllocator` scope;
  it exposes vector ownership operations but no `EngineResources`, operator,
  compiler, registry, or service lookup.
- **Filter / project / shape**: `FilterOperator`, `ProjectOperator`,
  `LimitOperator`, `OffsetOperator`, `TopNOperator`, `TopNRankingOperator`,
  `MarkDistinctOperator`, `EnforceSingleRowOperator`, `MaterializeOperator`,
  `MultiStageOperator`, `SortOperator`, `UnionAllOperator`.
- **Aggregation**: `AggregationOperator` (global), `GroupOperator` +
  `GroupedAggregationOperator` (grouped), backed by flat hash tables
  (`FlatGroupingTable`, `FlatKeyLayout`/`BigintPairFlatKeyLayout`, etc.).
- **Joins**: `HashJoinOperator`, `NestedLoopJoinOperator`, `FullJoinOperator`,
  `SemiJoinOperator`, with `JoinMatcher` (`EquiJoinMatcher`, `CrossJoinMatcher`)
  and join buffering support. Hash-join output uses `DictionaryVector` rather
  than a separate selection vector.
- **Window**: `WindowOperator` plus running/partition window functions
  (`RunningSumI64`, `RunningMaxI64`, `PartitionSumI64`, `PartitionAverageI64`,
  `PartitionOffsetI64`, `RankWindowFunction`).
- **Output values**: `Output` is the lazy column-stream contract carried by a
  `Batch`. Result sinks belong to the harness or host-integration boundary.

**Aggregation accumulators** (`operator/aggregation`): `Accumulator` interface
with `Sum`, `SumF64`, `Min`, `Max`, `MinUtf8`, `Avg`, `First`, `CountAll`,
`CountColumn`, and `StddevSamp`. Operators execute the accumulator sequence supplied by the
physical plan without recognizing or rewriting combinations of aggregate functions. Providers may
optionally publish exact `GroupedAggregationUpdateTarget` method handles; generated engine code constant-links
those targets while treating provider state as opaque rather than referencing a builtin state class.
The generated single-long grouping backend receives reusable `GeneratedLongGroupingBindings`; physical
I32/I64, dictionary, and null-stream inspection lives in that data-layer adapter rather than in the operator.
State objects are rebound only when aggregate state is allocated or grown, not per input batch. Providers may also
publish an exact repeated-update target for encoded-domain multiplicities; otherwise the engine conservatively
invokes the single update once per logical occurrence.

### Evaluator (`org.weakref.nitro.operator.evaluator`)

A working, lazy/memoized expression evaluator (not a stub):
- **`PlanEvaluator`** — executes an `EvaluationPlan`, memoizing variable
  assignments and resolving mask expressions; dispatches functions through
  `PrimitiveRegistry` / `PrimitiveFunction` / `PrimitiveExecutionContext`.
- **IR** (`evaluator/ir`, ~30 nodes): `EvaluationPlan`, `StreamPlan`,
  `Variable`, `Assignment`, `Call`, `Literal`, `Input`, `Reference`, `Copy`,
  `Merge`, `StructField`, mask expressions (`AllMask`, `AndMask`, `OrMask`,
  `NotMask`, `ReferenceMask` + `MaskExpressionResolver`), execution policies
  (`MaterializationPolicy`, `MemoizationPolicy`), and IR normalization
  (`IrNormalizer` with `CoalesceNormalizationRule`, `IfNormalizationRule`,
  validated by `NormalizedIrValidator`).
- See `evaluator.md` for the full design spec. The older `oldevaluator` package
  has been removed.

### Functions (`org.weakref.nitro.function`)

- `PrimitiveFunction` is the normalized batch invocation contract: it receives
  input/output streams, a first-class mask, and an allocator-backed execution
  context. Function resolution is supplied through the core `FunctionRegistry`;
  `PrimitiveRegistry` is the evaluator's bound-call lookup for legacy named IR.
- `function/scalar` holds annotation loading and immutable descriptors
  (`ScalarFunction`, `ScalarDescriptor`, `AnnotatedScalarLoader`);
  `function/scalar/builtin` holds built-ins across
  numeric, boolean, UTF-8/string, array, and map types (e.g. `AddI64`,
  `EqualUtf8`, `RegexpReplaceUtf8`, `Cardinality`, `MapContainsKeyUtf8`,
  `If*`/`Coalesce*`). Benchmark-only logical helpers remain under test sources.

### Parquet I/O

`io.trino.parquet.ParquetCompressionUtils` (vendored from Trino) provides
codec-specific decompression (Snappy, Zstd, LZ4/LZ4-raw, LZO, GZIP) for the
Parquet scan operators.

### Tests, Benchmarks, and Harnesses (`src/test/java`)

- **Functional**: `TestOperators`, `TestOperatorBatches`, `TestBatchRuntime`,
  `TestParquetOperator`, `TestPrimitiveFunctions`, `TestScalarRegistry`,
  `TestPlanEvaluator`, `TestEvaluationIr`. `OperatorAssertions.operator()` wraps
  any `Operator` for assertj row matching.
- **Micro-benchmarks (JMH)**: `BenchmarkOperators`,
  `BenchmarkAggregationStrategies`, `BenchmarkEvaluationStrategies`,
  `BenchmarkOperatorVectorSupport`.
- **Query suites**: `org.weakref.nitro.tpcds` and `org.weakref.nitro.clickbench`
  hold Nitro operator-tree harnesses (`BenchmarkQueries`, `TestQueries`) plus
  TPC-DS support/catalog classes; `org.weakref.trino.{tpcds,clickbench}` hold the
  equivalent Trino operator-tree harnesses for apples-to-apples comparison.
  `ExplainUnsupportedQueries` dumps Trino plans for not-yet-ported queries.
- Checked-in benchmark results live under `benchmarks/` and `docs/`.

### Code Style and others

- Do not use abbreviations in class, method or variable names, except for well-known terms (e.g. `Rle` for run-length encoding, `max`, `min`, etc).
- Tests and benchmark harnesses must not simulate query-engine execution by draining intermediate operator or page results and replaying them from rows, pages, constant tables, values sources, or other cached intermediate forms.
- If a subplan appears multiple times and the engine does not support reuse or CTE semantics for that shape, the harness must assemble and evaluate that subplan multiple times as operators instead of caching and replaying intermediate results in test code.
- Only the final consumer may iterate a query plan's top-level operator output. Intermediate harness code must compose operators, not execute them.
- Cross-engine tests and benchmarks must be apples-to-apples: both sides must be assembled from the same logical sequence of operators and stage boundaries. Do not compare a Nitro operator tree against a Trino SQL query and treat that as an operator-level comparison.
- This project uses Jujutsu VCS for version control
