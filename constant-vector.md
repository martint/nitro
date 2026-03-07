# Plan: ConstantVector Encoding

## Context

There is currently no way to represent scalar literals (e.g., `a + 100`) in the evaluator — `InputReference` fetches source columns and the `ir/Literal` stub is empty. The naive fix would allocate a full batch-sized vector filled with a constant value, wasting O(batchSize) memory and writes. A `ConstantVector` type stores `(value, length)` with no array backing. Functions that detect constant inputs can skip loops entirely for constant+constant cases, and hoist the constant out of the loop for constant+flat cases.

This also enables runtime constant propagation: if a column is uniform for a batch (e.g., after a filter reduced it to one value), any expression consuming it can short-circuit.

---

## New Files

| File | Description |
|------|-------------|
| `src/main/java/org/weakref/nitro/data/ConstantVector.java` | Sealed interface; permitted subtypes: `I64ConstantVector`, `BooleanConstantVector` |
| `src/main/java/org/weakref/nitro/data/I64ConstantVector.java` | `record I64ConstantVector(long value, int length)` |
| `src/main/java/org/weakref/nitro/data/BooleanConstantVector.java` | `record BooleanConstantVector(boolean value, int length)` |
| `src/main/java/org/weakref/nitro/operator/evaluator/functions/LiteralI64.java` | Literal function returning a pre-built `I64ConstantVector` |
| `src/main/java/org/weakref/nitro/operator/evaluator/functions/LiteralBoolean.java` | Literal function returning a pre-built `BooleanConstantVector` |

## Modified Files

| File | Change |
|------|--------|
| `src/main/java/org/weakref/nitro/data/Vector.java` | Add `ConstantVector` to `permits` clause |
| `src/main/java/org/weakref/nitro/data/Mask.java` | Add `and(BooleanConstantVector)` / `andNot(BooleanConstantVector)` overloads |
| `src/main/java/org/weakref/nitro/operator/evaluator/functions/AddI64.java` | Constant dispatch paths |
| `src/main/java/org/weakref/nitro/operator/evaluator/functions/SubtractI64.java` | Constant dispatch paths (same structure as AddI64) |
| `src/main/java/org/weakref/nitro/operator/evaluator/functions/Or.java` | Constant dispatch paths |
| `src/main/java/org/weakref/nitro/operator/evaluator/functions/If.java` | Handle `BooleanConstantVector` condition; update `allocate()` and `merge()` |
| `src/main/java/org/weakref/nitro/operator/evaluator/example/Example4.java` | Add a literals-based demo sub-example |

---

## Type Hierarchy

```
Vector (sealed)
  FlatVector (non-sealed)
    I64Vector
    BooleanVector
    ...
  RleVector
  ConstantVector (sealed)        ← new
    I64ConstantVector             ← new record
    BooleanConstantVector         ← new record
```

`ConstantVector` records implement all `Vector` methods:
- `length()` — returns the declared length
- `valueAt(int position)` — always returns the same boxed value (position ignored)
- `copy(int size)` — returns a new constant with updated length

No `Allocator.Context` needed — literal functions return a pre-built constant created once at construction time; no allocation occurs per-call.

---

## LiteralI64 / LiteralBoolean

```java
public class LiteralI64 implements Function {
    private final I64ConstantVector result;  // created once at construction

    public LiteralI64(long value, int batchSize) {
        this.result = new I64ConstantVector(value, batchSize);
    }

    @Override
    public Vector apply(Vector output, Mask mask, EvaluationContext context) {
        return result;  // no allocation, no loop
    }
}
```

The evaluator memoizes `result` in `buffers[index]` after the first call. Subsequent additive calls still invoke `apply()` for remaining positions, but `apply()` just returns the same object. This is correct — the constant is valid at all positions.

---

## Mask Overloads

```java
public Mask and(BooleanConstantVector other) {
    return other.value() ? this : new Mask(..., 0, false);  // short-circuit
}

public Mask andNot(BooleanConstantVector other) {
    return other.value() ? new Mask(..., 0, false) : this;  // short-circuit
}
```

---

## AddI64 / SubtractI64 Dispatch

New cases added at the top of the dispatch chain in `apply(left, right, mask, result, context)`, before the existing RLE cases:

**ConstantConstant** — no loop, no allocation:
```java
if (left instanceof I64ConstantVector lc && right instanceof I64ConstantVector rc) {
    return new I64ConstantVector(lc.value() + rc.value(), lc.length());
}
```

**ConstantFlat / FlatConstant** — constant hoisted out of loop:
```java
if (left instanceof I64ConstantVector lc) {
    return applyConstantFlat(lc.value(), (I64Vector) right, mask, result, context);
}
if (right instanceof I64ConstantVector rc) {
    return applyFlatConstant((I64Vector) left, rc.value(), mask, result, context);
}
```

```java
private Vector applyConstantFlat(long K, I64Vector flat, Mask mask, Vector result, EvaluationContext context) {
    I64Vector output = (I64Vector) context.allocator().allocateOrGrow(CONTEXT, result, flat.length(), I64Vector::new);
    if (mask.all()) {
        for (int i = 0; i <= mask.maxPosition(); i++) { output.values()[i] = K + flat.values()[i]; }
    } else {
        for (int position : mask) { output.values()[position] = K + flat.values()[position]; }
    }
    return output;
}
```

Note: SubtractI64 is non-commutative — `applyConstantFlat` computes `K - flat[i]` and `applyFlatConstant` computes `flat[i] - K`. Two separate methods.

**ConstantRle / RleConstant** — deferred. Handle by falling through or throwing `UnsupportedOperationException` for now.

---

## Or Dispatch

**ConstantConstant**:
```java
if (left instanceof BooleanConstantVector lc && right instanceof BooleanConstantVector rc) {
    return new BooleanConstantVector(lc.value() | rc.value(), lc.length());
}
```

**ConstantFlat / FlatConstant**: if constant is `true`, all positions are true — allocate output and fill true for mask positions. If constant is `false`, copy the flat vector into output unchanged.

---

## If Updates

Three changes:

**1. Condition evaluation** — replace the hard cast `(BooleanVector)` with a check that handles both types:
```java
Vector condVec = context.evaluate(condition, mask);
Mask trueMask  = mask.and(condVec);    // needs overload dispatch
Mask falseMask = mask.andNot(condVec); // needs overload dispatch
```
Use `Mask.and(BooleanConstantVector)` / `Mask.andNot(BooleanConstantVector)` overloads (see above), or pattern-match inline.

**2. `allocate(template, context)`** — add cases for constant vector templates (output of `If` is always flat, not constant, since branches may differ):
```java
if (template instanceof I64ConstantVector) {
    return context.allocator().allocate(CONTEXT, template.length(), I64Vector::new);
}
if (template instanceof BooleanConstantVector) {
    return context.allocator().allocate(CONTEXT, template.length(), BooleanVector::new);
}
```

**3. `merge(output, source, mask)`** — add constant source cases:
```java
else if (output instanceof I64Vector out && source instanceof I64ConstantVector src) {
    long v = src.value();
    for (int position : mask) { out.values()[position] = v; }
}
else if (output instanceof BooleanVector out && source instanceof BooleanConstantVector src) {
    boolean v = src.value();
    for (int position : mask) { out.values()[position] = v; }
}
```

---

## AddI64Exact / SubtractI64Exact / DivideI64 — Deferred

These produce per-position error flags stored as `BooleanVector errors`. Adding constant dispatch (overflow is deterministic for two constants; divide-by-zero is detectable at construction) would require changing the `errors()` return type to accept `BooleanConstantVector`. Defer to a follow-up once the base types are established.

---

## Verification

```bash
mvn compiler:compile -Dair.check.skip-checkstyle=true
mvn test -Dtest=TestOperators -Dair.check.skip-checkstyle=true
```

Add to `Example4` (or a new `Example5`) a demo using `LiteralI64` to compute `a + 100` and `a + 0`, showing that:
- constant+flat → flat result (loop with hoisted constant)
- constant+constant → `I64ConstantVector` result (no loop, no allocation)
