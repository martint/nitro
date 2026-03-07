# Plan: DictionaryVector Encoding

## Context

`DictionaryVector` is the most impactful encoding for analytic workloads. It stores a compact dictionary of distinct values and a per-row `int[] indices` array mapping each row to its dictionary entry. When both operands of a function share the same indices array, the function can compute on just the K distinct dictionary entries instead of the full N-row batch — a factor of N/K reduction. For a 1024-row batch with 5 distinct values, that is 200× fewer operations.

The existing `RleVector` optimizes sorted runs; `DictionaryVector` handles arbitrary repetition (e.g., a `country` column with values scattered in no particular order).

---

## New Files

| File | Description |
|------|-------------|
| `src/main/java/org/weakref/nitro/data/DictionaryVector.java` | `DictionaryVector(int[] indices, Vector dictionary)` |

## Modified Files

| File | Change |
|------|--------|
| `src/main/java/org/weakref/nitro/data/Vector.java` | Add `DictionaryVector` to `permits` clause |
| `src/main/java/org/weakref/nitro/operator/evaluator/functions/AddI64.java` | Dictionary dispatch paths |
| `src/main/java/org/weakref/nitro/operator/evaluator/functions/SubtractI64.java` | Dictionary dispatch paths (same structure as AddI64) |
| `src/main/java/org/weakref/nitro/operator/evaluator/example/Example4.java` | Add dictionary-based demo |

---

## Design

### Type

```java
public final class DictionaryVector implements Vector {
    private final int[] indices;     // length = batchSize; indices[i] → dict position for row i
    private final Vector dictionary; // length = dictSize (K << N typically)

    public DictionaryVector(int[] indices, Vector dictionary) { ... }

    public int[] indices() { return indices; }
    public Vector dictionary() { return dictionary; }

    @Override public int length() { return indices.length; }
    @Override public Object valueAt(int position) { return dictionary.valueAt(indices[position]); }
    @Override public Vector copy(int size) { throw new UnsupportedOperationException(); }
}
```

### AddI64 / SubtractI64 dispatch

New cases added at the top of `apply(left, right, mask, result, context)`, before the existing RLE cases:

**1. DictDict — same indices (peeling path):**
```java
if (left instanceof DictionaryVector ld && right instanceof DictionaryVector rd
        && ld.indices() == rd.indices()) {
    return applyDictDict(ld, rd, result, context);
}
```

```java
private DictionaryVector applyDictDict(DictionaryVector left, DictionaryVector right,
                                        Vector result, EvaluationContext context)
{
    // Additive short-circuit: if output already uses the same indices, dictionary is fully computed
    if (result instanceof DictionaryVector outDv && outDv.indices() == left.indices()) {
        return outDv;
    }

    I64Vector leftDict  = (I64Vector) left.dictionary();
    I64Vector rightDict = (I64Vector) right.dictionary();
    int dictSize = leftDict.length();

    I64Vector existingResultDict = result instanceof DictionaryVector r ? (I64Vector) r.dictionary() : null;
    I64Vector resultDict = (I64Vector) context.allocator().allocateOrGrow(CONTEXT, existingResultDict, dictSize, I64Vector::new);

    // K operations instead of N
    for (int di = 0; di < dictSize; di++) {
        resultDict.values()[di] = leftDict.values()[di] + rightDict.values()[di];
    }

    return new DictionaryVector(left.indices(), resultDict);
}
```

**2. DictDict — different indices, and DictFlat / FlatDict (expand, fall through):**
```java
if (left instanceof DictionaryVector ld) {
    return apply(expand(ld), right, mask, result, context);
}
if (right instanceof DictionaryVector rd) {
    return apply(left, expand(rd), mask, result, context);
}
```

`expand` materializes a `DictionaryVector` into a flat `I64Vector`:
```java
private static I64Vector expand(DictionaryVector dv)
{
    I64Vector dict = (I64Vector) dv.dictionary();
    I64Vector flat = new I64Vector(dv.length());
    for (int i = 0; i < dv.length(); i++) {
        flat.values()[i] = dict.values()[dv.indices()[i]];
    }
    return flat;
}
```

Expansion is only called on the mismatch path. No Allocator needed here since it is a fallback.

### Additivity with DictionaryVector output

When `applyDictDict` is called again with new rows (additive call), `result` is already a `DictionaryVector` with the same `indices[]` reference. The short-circuit at the top returns it immediately. This is correct: the dictionary covers all K distinct values regardless of which rows were in the first mask.

### If / Or — not modified in this phase

`If.apply()` casts the condition to `BooleanVector`. If a boolean column is dictionary-encoded, expand it before building the expression graph, or expand inline when the cast fails. No changes in this phase.

### AddI64Exact / SubtractI64Exact / DivideI64 — deferred

The `errors` field is `BooleanVector`. Dictionary dispatch would need to produce a dictionary-encoded errors vector. Deferred.

---

## Verification

```bash
mvn compiler:compile -Dair.check.skip-checkstyle=true
mvn test -Dtest=TestOperators -Dair.check.skip-checkstyle=true
```

Add to `Example4` a demo that:
1. Constructs a `DictionaryVector` with 5 distinct values over a 1024-row batch (random indices)
2. Evaluates `a + b` where both are dictionary-encoded with the same indices array
3. Asserts the result `instanceof DictionaryVector` — confirming no row-level expansion occurred
4. Spot-checks `result.valueAt(i)` against the expected value for a few rows
