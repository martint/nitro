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
