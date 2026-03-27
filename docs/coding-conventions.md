# Nitro Coding Conventions

## Be explicit about concrete vector assumptions

If a helper or code path semantically assumes one concrete vector type, prefer a
direct cast over a fake abstraction.

For example, prefer:

```java
return ((I64Vector) vector).values();
```

over:

```java
return switch (vector) {
    case I64Vector values -> values.values();
    default -> throw new IllegalArgumentException(...);
};
```

Use polymorphism or real multi-encoding dispatch only when the code genuinely
supports multiple runtime shapes. If there is only one valid shape, keep the
assumption explicit so the code is easier to read and maintain.

When code genuinely supports multiple encodings, dispatch once outside the hot
loop and keep the chosen loop body concrete. Prefer branching into specialized
array-based loops over introducing per-row polymorphic access such as
`value(position)` calls inside the loop.

## Keep one semantic kernel per function

When a function has multiple loop shapes or encoding-specific paths, keep the
core scalar semantics in one place.

For example, if `flat/flat` and `rle/rle` paths both implement the same
arithmetic or error rule, factor that rule into one helper and have the loop
variants call it instead of duplicating the logic.

This reduces the risk that one path drifts semantically from the others while
still allowing the hot loops themselves to stay specialized and readable.

## Keep traits open and extensible

Traits describe situational metadata about a vector use, not a closed list of
framework-known cases.

Prefer:

- open trait objects or name/value metadata
- producer/consumer-owned trait catalogs when a shared vocabulary is helpful
- code that checks for the specific traits it understands and ignores the rest

Avoid:

- enums that imply the framework knows every valid trait ahead of time
- treating traits as a closed logical type system
- baking assumptions about all possible trait names into shared vector APIs
- putting string-specific or domain-specific trait constants inside
  `BinaryVector` itself

For example, a binary vector may represent UTF-8 strings in one query, opaque
bytes in another, and an application-specific encoded payload elsewhere. Traits
should let those uses attach the metadata they need without modifying the base
vector contract.

## Keep benchmark quirks out of generic functions

Generic operators and scalar functions must implement their declared semantics,
not the special cases of a particular benchmark corpus.

Prefer:

- generic implementations driven by the function contract
- specialization by encoding, trait, or reusable capability
- benchmark-specific lowering in the test/benchmark layer when needed

Avoid:

- hard-coded special handling for named benchmark queries or literals
- helpers such as "fast path for the ClickBench regex"
- generic functions whose semantics quietly depend on a particular workload

If a benchmark reveals a hot pattern, optimize the general mechanism that
implements that pattern. Do not encode benchmark-specific assumptions into the
function itself.

## Keep evaluator dispatch capability-based

The evaluator should not grow special cases for individual primitive function
names such as `eq`, `lt`, or `contains_utf8`.

Prefer:

- generic capability interfaces such as mask-evaluable or encoding-aware
  primitive hooks
- function-local implementations of those capabilities
- evaluator logic that asks "can this primitive do X?" instead of "is this the
  `eq` function?"

Avoid:

- helper methods in `PlanEvaluator` named after particular primitive families
- rebuilding primitive semantics in evaluator-side code
- branching on function names inside the evaluator hot path

This keeps scalar semantics with the primitive implementation and helps prevent
the evaluator from becoming a second, harder-to-maintain dispatch layer.

## Distinguish available vs completed companion streams

When adding optimized mask-only or encoded execution paths, be explicit about
whether a primitive needs:

- completed companion streams
- or merely available companion streams when they already exist upstream

These are not the same thing.

A mask-only path may legitimately avoid synthesizing full row-wise `NULLS` or
`ERRORS` outputs while still requiring upstream null/error streams for correct
three-valued semantics. In those cases, prefer reusing existing companion
streams over forcing full completion, but do not silently drop them.

## Do not assume companion streams are flat

`NULLS` and `ERRORS` are logical boolean streams, not a promise that the
runtime has already flattened them to `BooleanVector`.

Prefer:

- helper code that accepts boolean-encoded vectors such as flat, dictionary,
  or RLE
- one dispatch point that unwraps the supported encodings before the hot loop
- aggregate and scalar code that treats companion streams as ordinary encoded
  inputs

Avoid:

- direct casts from `stream(..., Stream.NULLS)` or `stream(..., Stream.ERRORS)`
  to `BooleanVector` unless the surrounding code has already proven that shape
- using flat `boolean[]` helpers as the only representation of null/error
  state
- reintroducing eager flattening requirements at aggregation or evaluator
  boundaries

If a code path truly requires a flat boolean buffer, make that flattening step
explicit and local, rather than letting a hidden cast encode the assumption.

## Treat input loading separately from stream completion

When working in the evaluator, loading source input streams and completing a
derived stream bundle are different operations.

Prefer:

- loading all requested input streams directly from the input/source resolver
- using stream completion only after a primitive, merge, copy, or other
  derived operation has already produced its partial bundle

Avoid:

- loading one input stream and then expecting generic completion logic to
  synthesize or discover the rest of the requested input bundle
- treating missing input `VALUES` as optional when the consumer explicitly
  requested them

This keeps the source boundary honest and avoids bugs where projected input
values work in isolation but fail once a sibling expression also needs the
same input's companion streams.

## Prefer single predicate kernels over evaluator-built OR trees

When a query shape naturally means "value is one of these literals", prefer a
single primitive kernel such as `in_utf8` over lowering that condition into a
wide `OR` tree of repeated `eq_*` calls.

Prefer:

- one primitive call that can evaluate dictionary values once
- one mask-capable predicate that can narrow an owned mask in place
- query lowering that preserves the semantic shape of membership tests

Avoid:

- constructing large `OR(eq(...), eq(...), ...)` trees for hot filters
- relying on the evaluator to reorder and combine many nearly identical terms
- repeating dictionary/literal comparison setup per term when the condition is
  semantically one membership test

This keeps filter execution flatter, makes mask-native evaluation easier, and
avoids spending evaluator CPU on expression bookkeeping instead of predicate
work.

## Do not add bespoke benchmark operators

Benchmark and query harnesses should be assembled from standard operators and
reusable primitive functions. If a query needs special logic, first ask whether
it belongs in:

- a reusable primitive function
- a constant lookup table plus a standard join
- a normal `FilterOperator` / `ProjectOperator` / aggregation combination

Avoid adding query-specific operators such as `SomeQueryProjectOperator` or
`SomeQueryFilterOperator` in benchmark support code. Those operators hide the
real engine shape, make side-by-side comparisons less faithful, and tend to
push optimization work into harness code instead of core execution paths.

If a harness looks like it needs a bespoke operator, treat that as a design
warning and look for a more general operator-assembly or primitive-function
solution first.

## Do not use host-side benchmark side channels

Benchmark harnesses should not precompute query subresults in Java collections
and then inject those results back into the operator graph.

Prefer:

- lowerings that keep subqueries as operator-side relations
- standard join, semi-join, grouping, and projection machinery
- Trino and Nitro harnesses that use the same lowered topology

Avoid:

- collecting membership sets such as `Set<String>` in harness code and turning
  them into `value IN (...)` predicates
- building query results through host-side `Map` or `Set` lookups when the
  same logic should be expressed as join or semi-join operators
- lowering a query differently on Nitro and Trino just because one side is
  easier to wire through Java helpers

Constant tables are still fine for small literal relations that are part of the
lowered query itself. The problem is host-side extraction of dynamic query
subresults, not literal build-side data.

## Use logical EXPLAIN to drive complex benchmark lowerings

For complex benchmark queries, especially unsupported TPC-DS queries, derive
the intended operator topology from Trino's optimized logical `EXPLAIN` plan.

Prefer:

- reading the logical plan first
- treating `WITH` clauses as inline subplan trees at each use site
- implementing the same lowered topology in Nitro and Trino harnesses
- using the plan to identify missing operators such as `Window`, `GroupId`,
  `TopNRanking`, `FullJoin`, or `EnforceSingleRow`

Avoid:

- guessing the lowering directly from the SQL text when the optimizer is doing
  important shape changes
- implementing a query before checking whether Trino lowered it to a different
  operator family
- letting Nitro and Trino benchmark harnesses drift because the lowering was
  reverse-engineered informally instead of from the same plan
