# Expression Evaluator Specification

## Overview

The expression evaluator is a vectorized, mask-driven evaluation engine for computing expressions over batches of rows. It is designed around three core properties:

- **Lazy**: expressions are only computed when explicitly requested.
- **Memoized**: each expression tracks which positions have been evaluated. Requesting an already-computed position is free.
- **Additive**: requesting a superset of already-computed positions evaluates only the new positions and merges them into the existing result buffer in-place.

These properties together allow the evaluator to serve as a general building block for operators that need to compute expressions incrementally, conditionally, or across multiple overlapping masks.


## Concepts

### Batch

A batch is the unit of execution: a fixed-size collection of rows processed together. The batch size is implicit — it is the length of the input vectors provided to the evaluator.

### Position

A position is an integer index into the current batch, ranging from `0` to `batchSize - 1`.

### Mask

A `Mask` represents a subset of positions within a batch that are active for a given operation. It is an ordered list of distinct integers.

Key properties:
- A mask is always sorted in ascending order.
- `Mask.all(n)` — all `n` positions; the `all` flag is set as an optimization.
- `Mask.sparse(int[] positions, int batchSize)` — `positions[0..k-1]` are the `k` active positions; the array is padded to `batchSize` length. The `count` is `positions.length` (the length of the input before padding), not `batchSize`.
- `Mask.range(start, length)` — consecutive positions starting at `start`.

Relevant operations on `Mask`:

| Method | Description |
|--------|-------------|
| `count()` | number of active positions |
| `none()` | true if no positions are active |
| `all()` | true if the mask was created as all-inclusive |
| `containsAll(Mask other)` | true if every position in `other` is in this mask |
| `difference(Mask other)` | positions in this mask not in `other` |
| `or(Mask other)` | union of two same-length masks (merge sort; requires equal `positions` array length) |
| `and(BooleanVector)` | positions where the boolean vector is true |
| `andNot(BooleanVector)` | positions where the boolean vector is false |
| `contains(int position)` | membership test |

### Vector

A `Vector` holds the values for one column across a batch. The evaluator is vector-type-agnostic — it deals in `Vector` references and delegates type-specific computation to function implementations.

Common flat vector types: `I64Vector` (`long[]`), `BooleanVector` (`boolean[]`), `F64VectorWithNulls`, `I64VectorWithNulls`.

A vector's `length()` is the number of slots it was allocated with. For a batch of size `n`, a flat vector has `length() == n`. Only the positions specified by the evaluation mask are guaranteed to contain meaningful values — other positions are uninitialized or contain stale data from previous batches.


## Expression Graph

The evaluator operates on an expression graph encoded as a flat list of `Function` objects indexed by integer ordinal:

```
List<Function> expressions = [
    expression_0,
    expression_1,
    ...
    expression_N
]
```

Each expression has a unique index (its position in the list). Expressions reference their inputs by index — there are no symbolic names. Leaf nodes reference source input columns via `EvaluationContext.input(inputIndex, mask)`. Interior nodes reference other expressions via `EvaluationContext.evaluate(expressionIndex, mask)`.

This means the expression graph is encoded implicitly through the integer references stored inside each `Function` object. A directed edge from expression `B` to expression `A` means "B depends on A" and is represented by B holding A's index and calling `context.evaluate(A_index, mask)`.

**Acyclicity is a requirement.** The evaluator does not detect cycles; a cycle would result in infinite recursion.


## The Function Interface

Every node in the expression graph — including leaf nodes that wrap input columns — implements the same interface:

```java
public interface Function {
    Vector apply(Vector output, Mask mask, EvaluationContext context);
}
```

### Parameters

**`output`** — The current output buffer for this expression. It is:
- `null` on the first call (no result has been computed yet).
- A partially-filled vector on subsequent additive calls. It contains valid values at all positions previously computed, and may contain garbage at positions not yet evaluated.

A function must write into `output` for all positions in `mask`. It must not overwrite positions outside `mask` that were filled by earlier calls.

**`mask`** — The set of positions to evaluate in this call. The evaluator guarantees that every position in `mask` has not been computed before for this expression in the current batch. The function should not re-check this — it can trust that `mask` contains only new work.

**`context`** — The callback interface used to request evaluation of sub-expressions and to access the allocator.

### Return value

The function returns the output vector. If `output` was null, the function must allocate a fresh vector using `context.allocator()` and return it. If `output` was non-null, the function must return the same object after mutating it in-place.

### Allocation contract

All vector allocations inside function implementations must go through `context.allocator()` — never with `new VectorType(n)` directly. This keeps allocation visible to the memory tracking infrastructure.

The standard pattern uses `allocateOrGrow`, which reuses the existing buffer if it is large enough or allocates a fresh one otherwise:

```java
output = context.allocator().allocateOrGrow(CONTEXT, output, inputVec.length(), I64Vector::new);
```

Each function class declares a static `Allocator.Context` constant for identification:

```java
private static final Allocator.Context CONTEXT = new Allocator.Context("AddI64");
```

Error-producing functions use a separate context for their error vector:

```java
private static final Allocator.Context ERRORS_CONTEXT = new Allocator.Context("AddI64Exact.errors");
```

Allocation is done in `apply()` before dispatching to private computation methods. Private methods always receive pre-allocated vectors and never allocate themselves.

The evaluator never re-allocates the buffer; the same vector object persists across additive calls within a batch. Buffers are released (via `reset()`) between batches.


## EvaluationContext

```java
public interface EvaluationContext {
    Vector evaluate(int expressionIndex, Mask mask);
    Vector input(int inputIndex, Mask mask);
    Allocator allocator();
}
```

### `evaluate(expressionIndex, mask)`

Requests the evaluator to compute expression `expressionIndex` for the given `mask`, returning the expression's output vector. This call:

1. Applies the full memoization and additivity logic of the evaluator.
2. Returns the expression's buffer, which may contain valid data at positions beyond `mask` (from prior calls).
3. Is idempotent for positions already computed.

A function calls this for each of its inputs. The returned vector is valid at all positions in `mask` (and possibly more).

### `input(inputIndex, mask)`

Fetches a source input column by ordinal. The `mask` is passed to allow lazy input loading; the implementation may ignore it if inputs are pre-loaded. The returned vector is valid at the requested positions.

### `allocator()`

Returns the shared `Allocator` for tracking memory statistics. Functions use it via `allocator().allocateOrGrow(context, existingVector, size, VectorType::new)` to manage vector lifecycle.


## The Evaluator

```java
public class Evaluator {
    public Evaluator(List<Function> expressions, Input input, Allocator allocator)
    public Vector evaluate(int expressionIndex, Mask mask)
    public void reset()
}
```

### Internal state

For a program of `N` expressions, the evaluator maintains two parallel arrays:

- `buffers[N]` — the memoized output vector for each expression. `null` until the expression is first evaluated.
- `masks[N]` — the union of all masks for which each expression has been evaluated. `null` until first evaluated.

### Evaluation algorithm

`evaluate(index, mask)`:

1. If `mask.none()`, return `buffers[index]` immediately.
2. If `masks[index] != null && masks[index].containsAll(mask)`, the result is already fully computed. Return `buffers[index]`.
3. Compute `remaining`:
   - If `masks[index] == null`: `remaining = mask`
   - Otherwise: `remaining = mask.difference(masks[index])`
4. If `remaining.none()`, return `buffers[index]`.
5. Invoke `expressions.get(index).apply(buffers[index], remaining, context)` and store the result in `buffers[index]`.
6. Update `masks[index]`:
   - If previously null: `masks[index] = remaining`
   - Otherwise: `masks[index] = masks[index].or(remaining)`
7. Return `buffers[index]`.

### Additivity invariant

After `evaluate(i, mask1)` and then `evaluate(i, mask2)`:
- `buffers[i]` contains valid results at every position in `mask1 ∪ mask2`.
- `masks[i] == mask1.or(remaining)` where `remaining = mask2.difference(mask1)`.
- The `Function.apply()` call was made at most twice: once with `mask1`, once with `remaining = mask2 \ mask1`. Overlapping positions were computed exactly once.

### CSE (common subexpression elimination)

Because every expression is identified by a single integer index and its result is stored in `buffers[index]`, any expression referenced by multiple parent expressions is automatically computed at most once per batch. There is no need for an explicit CSE pass.

Example: if both expression 5 (`IF`) and expression 6 (`some_other_function`) reference expression 3 (`a + b`):
- First request to expression 5 causes `evaluate(3, trueMask)`.
- `buffers[3]` is now populated for `trueMask`.
- When expression 6 requests `evaluate(3, fullMask)`, the evaluator computes only `fullMask.difference(trueMask)` — the positions not yet evaluated.

### `reset()`

Clears `buffers[]` and `masks[]` (sets all to `null`) and releases allocator statistics. Must be called between batches. After reset, the evaluator is ready to process the next batch.


## Leaf Nodes: InputReference

Source input columns are modeled as leaf `Function` nodes:

```java
public class InputReference implements Function {
    private final int index;  // source input ordinal

    public Vector apply(Vector output, Mask mask, EvaluationContext context) {
        return context.input(index, mask);
    }
}
```

By implementing `Function`, source inputs participate in the evaluator's uniform memoization. If two parent expressions both reference `InputReference(0)`, the input is fetched at most once per batch (subsequent calls hit the memoization cache).

Note that `InputReference` does not use the `output` parameter — it returns whatever the `Input` implementation provides. The expectation is that `Input.get()` returns a pre-populated vector covering at least the requested positions.


## Simple Functions

A simple function (no branching, no conditional logic) follows this pattern:

```java
public class AddI64 implements Function {
    private final int left;   // expression index for left operand
    private final int right;  // expression index for right operand

    public Vector apply(Vector output, Mask mask, EvaluationContext context) {
        Vector leftVec = context.evaluate(left, mask);
        Vector rightVec = context.evaluate(right, mask);
        // Allocate output if null; otherwise reuse existing buffer.
        // Write output[pos] = leftVec[pos] + rightVec[pos] for each pos in mask.
        return output;
    }
}
```

Key points:
- Call `context.evaluate(inputIndex, mask)` with the same `mask` that was passed in. The evaluator handles ensuring the inputs are computed for exactly those positions.
- Read only positions in `mask` from the input vectors. Other positions may be uninitialized.
- Write only positions in `mask` into the output vector.
- Preserve positions outside `mask` in the output vector — they may hold results from prior additive calls.


## Conditional Functions (IF)

Conditional functions are not special-cased in the evaluator. They implement the standard `Function` interface and use mask arithmetic to selectively evaluate their branches.

The `IF(condition, ifTrue, ifFalse)` function:

```java
public class If implements Function {
    private final int condition;
    private final int ifTrue;
    private final int ifFalse;

    public Vector apply(Vector output, Mask mask, EvaluationContext context) {
        // 1. Evaluate condition for all positions in mask.
        BooleanVector cond = (BooleanVector) context.evaluate(condition, mask);

        // 2. Split mask into true and false subsets.
        Mask trueMask = mask.and(cond);
        Mask falseMask = mask.andNot(cond);

        // 3. Evaluate each branch only for its applicable positions.
        Vector trueResult  = trueMask.none()  ? null : context.evaluate(ifTrue,  trueMask);
        Vector falseResult = falseMask.none() ? null : context.evaluate(ifFalse, falseMask);

        // 4. Allocate output if needed (first call only).
        if (output == null) { output = allocate(trueResult or falseResult); }

        // 5. Merge both branch results into output in-place.
        merge(output, trueResult,  trueMask);
        merge(output, falseResult, falseMask);

        return output;
    }
}
```

### Why this works with additivity

Suppose `evaluate(ifExpr, mask1)` is called, then `evaluate(ifExpr, mask2)`.

- On the first call, the evaluator passes `mask1` to `If.apply()`.
  - `If` computes condition for `mask1`, splits into `trueMask1` and `falseMask1`.
  - Evaluates `ifTrue` for `trueMask1` and `ifFalse` for `falseMask1`.
  - Merges into a fresh `output` vector and returns it. `buffers[ifExpr] = output`.
- On the second call, the evaluator computes `remaining = mask2.difference(mask1)` and passes only `remaining` to `If.apply(output, remaining, context)`.
  - `If` computes condition for `remaining`, splits into `trueMask2` and `falseMask2`.
  - Evaluates `ifTrue` for `trueMask2` and `ifFalse` for `falseMask2` (new positions only — the evaluator's memoization handles this automatically for the branch expressions too).
  - Merges results into the existing `output` buffer at the new positions, leaving `mask1` positions intact.

The result: `buffers[ifExpr]` accumulates correct values across both calls, and no position is evaluated more than once in any branch expression.

### Interaction with CSE

If both `IF(cond, a+b, a-b)` and `a+b` are requested independently over the same mask:
- `IF` internally requests `evaluate(a_plus_b_index, trueMask)`.
- The separate request for `a+b` over the full mask causes `evaluate(a_plus_b_index, fullMask)`, but the evaluator computes only the positions not already evaluated by `IF` (the false-branch positions).


## Implementing New Functions: Guidelines

### Simple (non-conditional) functions

1. Store the expression indices of your inputs as constructor fields.
2. Declare a `private static final Allocator.Context CONTEXT = new Allocator.Context("FunctionName")`.
3. In `apply()`, call `context.evaluate(inputIndex, mask)` for each input, then allocate via the allocator:
   ```java
   output = context.allocator().allocateOrGrow(CONTEXT, output, inputVec.length(), I64Vector::new);
   ```
4. Dispatch to a private method that receives the pre-allocated vector and never allocates itself.
5. Write computed values only for positions in `mask`.
6. Do not read positions outside `mask` from the output vector — they may be uninitialized.

### Conditional / lazy functions (OR, AND, CASE, COALESCE, TRY, ...)

These functions choose which sub-expressions to evaluate and under which masks. The general pattern:

1. Evaluate whatever sub-expressions are needed to determine the active subsets.
2. Partition the incoming `mask` into non-overlapping sub-masks based on conditions.
3. Call `context.evaluate(branchIndex, subMask)` for each branch with its sub-mask.
4. Merge the branch results into `output` at the corresponding positions.

The evaluator places no constraints on how a function partitions the mask or how many sub-evaluations it performs. The function is free to create any masks it needs, provided the sub-masks cover exactly the positions they compute into the output.

### Error-producing functions

Functions that may produce errors (overflow detection, division by zero, etc.) store the error flags in a `BooleanVector errors` field on the function object itself — not through the `Function.apply()` return value. Callers that need error information cast to the concrete function type and call `errors()`.

This keeps the `Function` interface uniform: `apply()` always returns the values vector.

Error-producing functions declare two contexts and allocate both vectors before dispatching:

```java
private static final Allocator.Context CONTEXT = new Allocator.Context("AddI64Exact");
private static final Allocator.Context ERRORS_CONTEXT = new Allocator.Context("AddI64Exact.errors");

private BooleanVector errors;

public Vector apply(Vector output, Mask mask, EvaluationContext context) {
    Vector leftVec = context.evaluate(left, mask);
    Vector rightVec = context.evaluate(right, mask);
    output = context.allocator().allocateOrGrow(CONTEXT, output, leftVec.length(), I64Vector::new);
    errors = (BooleanVector) context.allocator().allocateOrGrow(ERRORS_CONTEXT, errors, leftVec.length(), BooleanVector::new);
    // ... compute, writing into output and errors ...
}
```

The `errors` field persists across additive calls within a batch — it is grown via `allocateOrGrow` just like the output. Callers exclude error positions from subsequent masks:

```java
evaluator.evaluate(addExprIndex, mask);
Mask noOverflow = addExact.errors() != null ? mask.andNot(addExact.errors()) : mask;
// continue with noOverflow
```

`errors()` returns `null` until the function has been evaluated at least once in the current batch.


## Null Handling

Nullability is a driver-level concern — the evaluator and `Function` implementations are null-unaware. Null tracking is expressed as ordinary boolean vectors in the expression graph, and null exclusion is done by narrowing the mask before evaluation.

### Pattern

1. Represent nulls as `BooleanVector` columns in the input (one per nullable input column).
2. Add an `Or` node in the expression graph to combine null vectors:
   ```
   7: Or(1, 3)   -- ab_nulls: true wherever a OR b is null
   ```
3. At the driver level, evaluate the combined null vector and narrow the mask:
   ```java
   BooleanVector abNulls = (BooleanVector) evaluator.evaluate(7, inputMask);
   Mask nonNullMask = inputMask.andNot(abNulls);
   evaluator.evaluate(8, nonNullMask);  // arithmetic only runs on non-null positions
   ```

### Chaining nulls across multiple inputs

When three inputs `a`, `b`, `c` all have nulls, chain the `Or` nodes:

```
6: Or(1, 3)    -- ab_nulls
8: Or(6, 5)    -- abc_nulls
```

Evaluate the final combined null vector, then use one `andNot` to exclude all null positions.

### Interaction with error handling

After null exclusion, error masks (overflow, divide-by-zero) are applied as a further narrowing:

```java
Mask nonNull = inputMask.andNot(nulls);
evaluator.evaluate(addIndex, nonNull);
Mask valid = addExact.errors() != null ? nonNull.andNot(addExact.errors()) : nonNull;
// continue with valid
```

Positions are excluded progressively: nulls first, then errors.

### Conditional expressions

When an `IF(cond, a+b, a-b)` is involved, null exclusion happens before evaluating the `IF`, and overflow is checked per-branch after the `IF` runs:

```java
Mask nonNull = inputMask.andNot(abNulls);
evaluator.evaluate(ifExprIndex, nonNull);  // evaluates a+b for true branch, a-b for false branch

BooleanVector condVec = (BooleanVector) evaluator.evaluate(condIndex, nonNull);
Mask trueMask  = nonNull.and(condVec);
Mask falseMask = nonNull.andNot(condVec);
Mask noOverflow = union(
        addExact.errors() != null  ? trueMask.andNot(addExact.errors())  : trueMask,
        subExact.errors() != null ? falseMask.andNot(subExact.errors()) : falseMask);
```

The `union` helper merges two non-overlapping masks: `a.none() ? b : b.none() ? a : a.or(b)`.


## API Summary

```
Evaluator(List<Function> expressions, Input input, Allocator allocator)
  Vector evaluate(int expressionIndex, Mask mask)
  void reset()

Function
  Vector apply(Vector output, Mask mask, EvaluationContext context)

EvaluationContext
  Vector evaluate(int expressionIndex, Mask mask)
  Vector input(int inputIndex, Mask mask)
  Allocator allocator()

Input (functional interface)
  Vector get(int index, Mask mask)
```


## Example: IF(condition, a+b, a-b)

Expression graph (indices 0–5):

```
0: InputReference(0)   → source column a
1: InputReference(1)   → source column b
2: InputReference(2)   → source column condition (boolean)
3: AddI64(0, 1)        → a + b
4: SubtractI64(0, 1)   → a - b
5: If(2, 3, 4)         → IF(condition, a+b, a-b)
```

Usage:

```java
Evaluator evaluator = new Evaluator(expressions, (index, mask) -> inputs[index], allocator);

// Compute IF result for all rows.
Vector result = evaluator.evaluate(5, Mask.all(8));

// Additive: compute for the first half, then the second half.
evaluator.evaluate(5, firstHalf);
evaluator.evaluate(5, secondHalf); // only computes positions not in firstHalf

// CSE: a+b is already cached after the IF evaluation above.
Vector aPlusB = evaluator.evaluate(3, Mask.all(8)); // no extra computation

evaluator.reset(); // clear state before next batch
```

When `evaluate(5, Mask.all(8))` is called:
1. Evaluator calls `If.apply(null, all, context)`.
2. `If` calls `context.evaluate(2, all)` → evaluator computes condition (calls `InputReference(2).apply`).
3. `If` splits `all` into `trueMask` (condition=true positions) and `falseMask` (condition=false positions).
4. `If` calls `context.evaluate(3, trueMask)` → evaluator computes `AddI64`:
   - `AddI64` calls `context.evaluate(0, trueMask)` → fetches `a`.
   - `AddI64` calls `context.evaluate(1, trueMask)` → fetches `b`.
   - Computes `a[i] + b[i]` for positions in `trueMask`.
5. `If` calls `context.evaluate(4, falseMask)` → evaluator computes `SubtractI64` similarly.
6. `If` merges both results into a fresh `output` vector and returns it.
7. Evaluator stores `buffers[5] = output`, `masks[5] = all`.

When `evaluate(3, all)` is subsequently called:
- `masks[3]` already contains `trueMask` (step 4 above).
- Evaluator computes `remaining = all.difference(trueMask) = falseMask`.
- Calls `AddI64.apply(buffers[3], falseMask, context)` to fill in the remaining positions.
- `buffers[3]` now contains `a + b` for all positions.
