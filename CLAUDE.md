# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Nitro is a columnar query evaluation engine in Java. It explores vectorized,
batch-based execution with mask-driven row selection. Beyond synthetic
micro-benchmarks it now reads real Parquet data and runs subsets of the TPC-DS
and ClickBench query suites, benchmarked apples-to-apples against an equivalent
tree of Trino operators.

## Commands

Java 25 with preview features enabled (`--enable-preview --add-modules
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
  `ConstantTableOperator`, `SingleBatchOperator`, and the Parquet scans
  (`ParquetScanOperator` base with `TrinoParquetScanOperator` and
  `HardwoodParquetScanOperator`).
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
- **Output**: `OutputOperator` / `Output`.

**Aggregation accumulators** (`operator/aggregation`): `Accumulator` interface
with `Sum`, `SumF64`, `Min`, `Max`, `MinUtf8`, `Avg`, `First`, `CountAll`,
`CountColumn`, `StddevSamp`, and the fused `FusedMinMaxI64` (+ `AccumulatorFusion`).

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

- `Function` is a `@FunctionalInterface`: `apply(Vector output, Vector[] inputs,
  Mask mask)`, used by `ProjectOperator` for column computations.
- `function/scalar` holds the registry (`ScalarFunction`, `ScalarDescriptor`,
  `ScalarRegistry`); `function/scalar/builtin` holds ~57 built-ins across
  numeric, boolean, UTF-8/string, array, and map types (e.g. `AddI64`,
  `DivideScaleRoundI64`, `EqualUtf8`, `RegexpReplaceUtf8`, `ArraySumI64`,
  `MapContainsKeyUtf8`, `CastUtf8ToI64`, `If*`/`Coalesce*`).

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
