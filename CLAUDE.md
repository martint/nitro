# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Nitro is a columnar evaluation engine prototype in Java. It explores vectorized, batch-based query execution with mask-driven row selection.

## Commands

```bash
# Build
mvn compile

# Run all tests
mvn test

# Run a single test class or method
mvn test -Dtest=TestOperators
mvn test -Dtest=TestOperators#testComplex

# Run JMH benchmarks (benchmarks live in src/test/java)
mvn test -Dtest=BenchmarkOperators
```

Java 24 with preview features enabled (`--enable-preview`).

## Architecture

### Data Layer (`org.weakref.nitro.data`)

- **`Vector`** — sealed interface with two permitted subtypes: `FlatVector` (non-sealed, flat array layout) and `RleVector` (run-length encoded).
- **Concrete vectors**: `I64Vector`, `I64VectorWithNulls`, `F64VectorWithNulls`, `I32VectorWithNulls`, `BooleanVector`. Nullable variants carry separate `values[]` and `nulls[]` boolean arrays.
- **`Mask`** — represents the set of active row positions in a batch as a sorted `int[]`. Supports `all()`, `sparse()`, `range()` factories and set operations (`and`, `andNot`, `or`, `complement`, `difference`, `union`). The `all` flag is an optimization indicating no filtering has occurred.
- **`Allocator`** — tracks memory allocation statistics per named `Context`. Used by operators to record vector allocations for profiling.

### Operator Layer (`org.weakref.nitro.operator`)

**`Operator` interface** — pull-based, batch-at-a-time iterator:
- `next()` → `Mask` of active row positions for the current batch
- `column(int)` → `Vector` for that column (call after `next()`)
- `constrain(Mask)` — push a narrower mask back to the operator, allowing lazy/partial column computation
- Columns are addressed by index; negative indices (e.g. `-1`, `-2`) refer to input columns in `ProjectOperator`

**Operators**:
- `GeneratorOperator` — produces synthetic data via `I64Generator` implementations (`SequenceGenerator`, `ConstantGenerator`)
- `ConstantTableOperator` — in-memory table from a `List<Row>`
- `FilterOperator` — applies a `VectorPredicate` / `I64Predicate` to one column
- `ProjectOperator` — evaluates a list of `Invocation`s (function + input column indices + output vector factory), supports CSE via intermediate result reuse
- `AggregationOperator` — global aggregation via `Accumulator` implementations
- `GroupOperator` — assigns group IDs to rows
- `GroupedAggregationOperator` — grouped aggregation consuming `GroupOperator` output
- `LimitOperator`, `TopNOperator`, `NestedLoopJoinOperator`, `OutputOperator`

**Aggregation** (`operator/aggregation`): `Accumulator` interface with `Sum`, `Min`, `Max`, `First`, `CountAll`, `CountColumn`, `SumF64`.

### Evaluator (`org.weakref.nitro.operator/evaluator`)

A work-in-progress expression evaluator with an IR layer:
- **IR** (`evaluator/ir`): `Variable`, `Assignment` (variable = operation @ mask), `Call`, `Literal`, `Reference`, `Stream`
- **`Evaluator`** — mostly commented out; intended to do lazy, memoized evaluation of expression trees
- **Functions** (`evaluator/functions`): `AddI64`, `AddI64Exact`, `SubtractI64`, `SubtractI64Exact`, `DivideI64`, `Or` — operate on vectors given a `Mask`, writing into a reusable `Result`
- **Examples** (`evaluator/example`): `Example1`–`Example3` demonstrate manual expression evaluation with null/error tracking
- The old evaluator lives in `operator/oldevaluator` and is superseded

### Function Interface (`org.weakref.nitro.function`)

`Function` is a `@FunctionalInterface`: `apply(Vector output, Vector[] inputs, Mask mask)`. Used by `ProjectOperator` for column computations.

### Tests and Benchmarks

- `TestOperators` — JUnit 5 functional tests; uses `OperatorAssertions.operator()` to wrap any `Operator` for assertj-based row matching
- `BenchmarkOperators`, `BenchmarkAggregationStrategies`, `BenchmarkEvaluationStrategies`, `BenchmarkAddExact` — JMH benchmarks in `src/test/java`
            
### Code Style and others

- Do not use abbreviations in class, method or variable names, except for well-known terms (e.g. `Rle` for run-length encoding, `max`, `min`, etc).
- Tests and benchmark harnesses must not simulate query-engine execution by draining intermediate operator or page results and replaying them from rows, pages, constant tables, values sources, or other cached intermediate forms.
- If a subplan appears multiple times and the engine does not support reuse or CTE semantics for that shape, the harness must assemble and evaluate that subplan multiple times as operators instead of caching and replaying intermediate results in test code.
- Only the final consumer may iterate a query plan's top-level operator output. Intermediate harness code must compose operators, not execute them.
- Cross-engine tests and benchmarks must be apples-to-apples: both sides must be assembled from the same logical sequence of operators and stage boundaries. Do not compare a Nitro operator tree against a Trino SQL query and treat that as an operator-level comparison.
- This project uses Jujutsu VCS for version control
