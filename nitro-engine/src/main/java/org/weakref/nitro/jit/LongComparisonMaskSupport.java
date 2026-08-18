/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.weakref.nitro.jit;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64BinaryDispatch;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.MaskOutcome;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;

import java.util.List;

final class LongComparisonMaskSupport
{
    private LongComparisonMaskSupport() {}

    @FunctionalInterface
    interface ComparisonKernel
    {
        boolean test(long left, long right);
    }

    /**
     * A dictionary-encoded comparison input paired with a constant literal: the per-entry comparison result
     * ({@code result[e]}) is evaluated once over the dictionary's distinct values, and each row's outcome is then a
     * single {@code result[ids[position]]} array read — turning a per-row long compare over millions of rows into one
     * compare per distinct value. (Per-row null/error are still applied separately by each caller.)
     */
    private record DictionaryComparison(boolean[] result, int[] ids) {}

    /**
     * Detect the dictionary-vs-literal shape (either argument order) and precompute the per-entry comparison, or return
     * null. {@code values} on each side carries the encoding; nulls/errors are separate streams handled by the caller,
     * so a null dictionary entry's (garbage) result is simply never read.
     */
    private static DictionaryComparison tryDictionaryComparison(Vector left, Vector right, ComparisonKernel kernel, int positionCount)
    {
        if (left instanceof DictionaryVector dictionary && singleRunLong(right) != null) {
            long literal = singleRunLong(right);
            return perEntry(dictionary, literal, kernel, true, positionCount);
        }
        if (right instanceof DictionaryVector dictionary && singleRunLong(left) != null) {
            long literal = singleRunLong(left);
            return perEntry(dictionary, literal, kernel, false, positionCount);
        }
        return null;
    }

    private static DictionaryComparison perEntry(DictionaryVector dictionary, long literal, ComparisonKernel kernel, boolean dictionaryOnLeft, int positionCount)
    {
        Vector entries = dictionary.values();
        if (!(entries instanceof I64Vector || entries instanceof I32Vector)) {
            return null;   // nested encodings are rare here; fall back to the per-row path
        }
        // Per-entry pays off only when there are fewer distinct entries than rows. A hash-join output wraps the whole
        // build column in a DictionaryVector whose values() holds one entry per build row -- far more than a batch
        // references -- so computing the comparison over every entry each batch is more work than the per-row path.
        if (entries.length() > positionCount) {
            return null;
        }
        VectorAccess.LongValues entryValues = VectorAccess.longValues(entries);
        int entryCount = entries.length();
        boolean[] result = new boolean[entryCount];
        for (int entry = 0; entry < entryCount; entry++) {
            long value = entryValues.value(entry);
            result[entry] = dictionaryOnLeft ? kernel.test(value, literal) : kernel.test(literal, value);
        }
        return new DictionaryComparison(result, dictionary.ids());
    }

    /** The single repeated value of a one-run RLE over longs, or null if the vector is not that shape. */
    private static Long singleRunLong(Vector vector)
    {
        if (vector instanceof RleVector rle && rle.counts().length == 1
                && (rle.values() instanceof I64Vector || rle.values() instanceof I32Vector)) {
            return VectorAccess.longValues(rle.values()).value(0);
        }
        return null;
    }

    /** Collect into {@code out} the non-null/non-error positions whose per-entry comparison equals {@code wanted}. */
    private static int collectDictionary(DictionaryComparison comparison, boolean wanted, Mask mask,
            VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls,
            VectorAccess.BooleanValues leftErrors, VectorAccess.BooleanValues rightErrors, int[] out)
    {
        boolean[] result = comparison.result();
        int[] ids = comparison.ids();
        int count = 0;
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                if (isError(leftErrors, position) || isError(rightErrors, position) || isNull(leftNulls, position) || isNull(rightNulls, position)) {
                    continue;
                }
                if (result[ids[position]] == wanted) {
                    out[count++] = position;
                }
            }
            return count;
        }
        for (int position : mask) {
            if (isError(leftErrors, position) || isError(rightErrors, position) || isNull(leftNulls, position) || isNull(rightNulls, position)) {
                continue;
            }
            if (result[ids[position]] == wanted) {
                out[count++] = position;
            }
        }
        return count;
    }

    private static MaskOutcome dictionaryMaskOutcome(DictionaryComparison comparison, Mask mask,
            PrimitiveExecutionContext context, Allocator.Context allocationContext,
            VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls,
            VectorAccess.BooleanValues leftErrors, VectorAccess.BooleanValues rightErrors)
    {
        int capacity = mask.count();
        Mask trueMask = context.allocator().allocateUninitializedSparseMask(allocationContext, capacity, mask.size());
        Mask nullMask = context.allocator().allocateUninitializedSparseMask(allocationContext, capacity, mask.size());
        Mask errorMask = context.allocator().allocateUninitializedSparseMask(allocationContext, capacity, mask.size());
        int[] truePositions = trueMask.positionsArrayForOverwrite(capacity);
        int[] nullPositions = nullMask.positionsArrayForOverwrite(capacity);
        int[] errorPositions = errorMask.positionsArrayForOverwrite(capacity);
        boolean[] result = comparison.result();
        int[] ids = comparison.ids();
        int trueCount = 0;
        int nullCount = 0;
        int errorCount = 0;
        for (int position : mask) {
            if (isError(leftErrors, position) || isError(rightErrors, position)) {
                errorPositions[errorCount++] = position;
            }
            else if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                nullPositions[nullCount++] = position;
            }
            else if (result[ids[position]]) {
                truePositions[trueCount++] = position;
            }
        }
        trueMask.finishRetain(trueCount);
        nullMask.finishRetain(nullCount);
        errorMask.finishRetain(errorCount);
        return new MaskOutcome(trueMask, nullMask, errorMask);
    }

    public static MaskOutcome tryEvaluateMaskOutcome(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, Allocator.Context allocationContext, ComparisonKernel kernel)
    {
        if (!supportsLongComparison(inputs)) {
            return null;
        }

        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.data.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.data.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.data.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.data.Stream.ERRORS));

        DictionaryComparison comparison = tryDictionaryComparison(inputs.get(0).values(), inputs.get(1).values(), kernel, mask.count());
        if (comparison != null) {
            return dictionaryMaskOutcome(comparison, mask, context, allocationContext, leftNulls, rightNulls, leftErrors, rightErrors);
        }

        int[] counts = new int[3];
        I64BinaryDispatch.forEachPair(inputs.get(0).values(), inputs.get(1).values(), mask, (leftValue, rightValue, position) -> {
            if (isError(leftErrors, position) || isError(rightErrors, position)) {
                counts[2]++;
            }
            else if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                counts[1]++;
            }
            else if (kernel.test(leftValue, rightValue)) {
                counts[0]++;
            }
        });

        int[] truePositions = new int[counts[0]];
        int[] nullPositions = new int[counts[1]];
        int[] errorPositions = new int[counts[2]];
        int[] indexes = new int[3];
        I64BinaryDispatch.forEachPair(inputs.get(0).values(), inputs.get(1).values(), mask, (leftValue, rightValue, position) -> {
            if (isError(leftErrors, position) || isError(rightErrors, position)) {
                errorPositions[indexes[2]++] = position;
            }
            else if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                nullPositions[indexes[1]++] = position;
            }
            else if (kernel.test(leftValue, rightValue)) {
                truePositions[indexes[0]++] = position;
            }
        });

        return new MaskOutcome(
                context.allocator().allocateSparseMask(allocationContext, truePositions, truePositions.length, mask.size()),
                context.allocator().allocateSparseMask(allocationContext, nullPositions, nullPositions.length, mask.size()),
                context.allocator().allocateSparseMask(allocationContext, errorPositions, errorPositions.length, mask.size()));
    }

    public static Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, Allocator.Context allocationContext, ComparisonKernel kernel, Mask.ComparisonOperator operator, boolean returnedConstantComparisonMasks)
    {
        if (!supportsLongComparison(inputs)) {
            return null;
        }

        Mask constantMask = tryConstantComparisonMask(inputs, mask, context, allocationContext, operator, true, returnedConstantComparisonMasks);
        if (constantMask != null) {
            return constantMask;
        }

        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.data.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.data.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.data.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.data.Stream.ERRORS));

        int[] truePositions = new int[mask.count()];
        DictionaryComparison comparison = tryDictionaryComparison(inputs.get(0).values(), inputs.get(1).values(), kernel, mask.count());
        if (comparison != null) {
            int count = collectDictionary(comparison, true, mask, leftNulls, rightNulls, leftErrors, rightErrors, truePositions);
            return context.allocator().allocateSparseMask(allocationContext, truePositions, count, mask.size());
        }
        int[] trueIndex = new int[1];
        I64BinaryDispatch.forEachPair(inputs.get(0).values(), inputs.get(1).values(), mask, (leftValue, rightValue, position) -> {
            if (isError(leftErrors, position) || isError(rightErrors, position) || isNull(leftNulls, position) || isNull(rightNulls, position)) {
                return;
            }
            if (kernel.test(leftValue, rightValue)) {
                truePositions[trueIndex[0]++] = position;
            }
        });
        return context.allocator().allocateSparseMask(allocationContext, truePositions, trueIndex[0], mask.size());
    }

    public static Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, Allocator.Context allocationContext, ComparisonKernel kernel, Mask.ComparisonOperator operator, boolean returnedConstantComparisonMasks)
    {
        if (!supportsLongComparison(inputs)) {
            return null;
        }

        Mask constantMask = tryConstantComparisonMask(inputs, mask, context, allocationContext, operator, false, returnedConstantComparisonMasks);
        if (constantMask != null) {
            return constantMask;
        }

        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.data.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.data.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.data.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.data.Stream.ERRORS));

        int[] falsePositions = new int[mask.count()];
        DictionaryComparison comparison = tryDictionaryComparison(inputs.get(0).values(), inputs.get(1).values(), kernel, mask.count());
        if (comparison != null) {
            int count = collectDictionary(comparison, false, mask, leftNulls, rightNulls, leftErrors, rightErrors, falsePositions);
            return context.allocator().allocateSparseMask(allocationContext, falsePositions, count, mask.size());
        }
        int[] falseIndex = new int[1];
        I64BinaryDispatch.forEachPair(inputs.get(0).values(), inputs.get(1).values(), mask, (leftValue, rightValue, position) -> {
            if (isError(leftErrors, position) || isError(rightErrors, position) || isNull(leftNulls, position) || isNull(rightNulls, position)) {
                return;
            }
            if (!kernel.test(leftValue, rightValue)) {
                falsePositions[falseIndex[0]++] = position;
            }
        });
        return context.allocator().allocateSparseMask(allocationContext, falsePositions, falseIndex[0], mask.size());
    }

    public static boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, ComparisonKernel kernel, Mask.ComparisonOperator operator)
    {
        if (!supportsLongComparison(inputs)) {
            return false;
        }

        if (tryConstantComparisonInPlace(inputs, mask, operator, true)) {
            return true;
        }

        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.data.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.data.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.data.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.data.Stream.ERRORS));

        DictionaryComparison comparison = tryDictionaryComparison(inputs.get(0).values(), inputs.get(1).values(), kernel, mask.count());
        if (comparison != null) {
            boolean[] result = comparison.result();
            int[] ids = comparison.ids();
            if (tryApplyDictionaryMonomorphic(inputs, mask, result, ids, true)) {
                return true;
            }
            mask.retainIf(position -> !isError(leftErrors, position) && !isError(rightErrors, position)
                    && !isNull(leftNulls, position) && !isNull(rightNulls, position) && result[ids[position]]);
            return true;
        }

        VectorAccess.LongValues leftValues = VectorAccess.longValues(inputs.get(0).values());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(inputs.get(1).values());
        mask.retainIf(position -> {
            if (isError(leftErrors, position) || isError(rightErrors, position) || isNull(leftNulls, position) || isNull(rightNulls, position)) {
                return false;
            }
            return kernel.test(leftValues.value(position), rightValues.value(position));
        });
        return true;
    }

    public static boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, ComparisonKernel kernel, Mask.ComparisonOperator operator)
    {
        if (!supportsLongComparison(inputs)) {
            return false;
        }

        if (tryConstantComparisonInPlace(inputs, mask, operator, false)) {
            return true;
        }

        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.data.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.data.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.data.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.data.Stream.ERRORS));

        DictionaryComparison comparison = tryDictionaryComparison(inputs.get(0).values(), inputs.get(1).values(), kernel, mask.count());
        if (comparison != null) {
            boolean[] result = comparison.result();
            int[] ids = comparison.ids();
            if (tryApplyDictionaryMonomorphic(inputs, mask, result, ids, false)) {
                return true;
            }
            mask.retainIf(position -> !isError(leftErrors, position) && !isError(rightErrors, position)
                    && !isNull(leftNulls, position) && !isNull(rightNulls, position) && !result[ids[position]]);
            return true;
        }

        VectorAccess.LongValues leftValues = VectorAccess.longValues(inputs.get(0).values());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(inputs.get(1).values());
        mask.retainIf(position -> {
            if (isError(leftErrors, position) || isError(rightErrors, position) || isNull(leftNulls, position) || isNull(rightNulls, position)) {
                return false;
            }
            return !kernel.test(leftValues.value(position), rightValues.value(position));
        });
        return true;
    }

    /**
     * Fast path for the {@code column OPERATOR constant} filter shape: one operand is a flat integer column and the
     * other a single-run (constant) literal, with both inputs null-free and error-free. The retain runs as a
     * monomorphic array scan ({@link Mask#retainConstantComparison}) rather than the per-position value/null/error
     * accessor lambdas of the general path. Returns {@code true} when it handled the mask, {@code false} to fall back.
     *
     * <p>{@code baseOperator} is the operator the kernel implements with the column on the left; the effective
     * operator is derived by swapping operands when the constant is on the left and negating for a false mask.
     */
    private static boolean tryConstantComparisonInPlace(List<Streams> inputs, Mask mask, Mask.ComparisonOperator baseOperator, boolean wantTrue)
    {
        if (baseOperator == null) {
            return false;
        }

        // Match the column-vs-constant shape with cheap checks (instanceof + single-run literal) before paying for the
        // null/error-free scan, so a column-vs-column comparison falls back without scanning the null streams.
        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();

        Vector column;
        long literal;
        Mask.ComparisonOperator operator;
        Long rightConstant = singleRunLong(right);
        if (rightConstant != null && isFlatInteger(left)) {
            column = left;
            literal = rightConstant;
            operator = baseOperator;
        }
        else {
            Long leftConstant = singleRunLong(left);
            if (leftConstant == null || !isFlatInteger(right)) {
                return false;
            }
            column = right;
            literal = leftConstant;
            operator = swapOperands(baseOperator);
        }

        boolean columnOnLeft = column == left;
        Streams columnInput = columnOnLeft ? inputs.get(0) : inputs.get(1);
        Streams literalInput = columnOnLeft ? inputs.get(1) : inputs.get(0);

        // The literal operand (a constant) must be null/error-free, and the column must be error-free. A nullable
        // column is still handled monomorphically: null positions are excluded inside the scan via its null array.
        if (!isNullAndErrorFree(literalInput) || !isErrorFree(columnInput)) {
            return false;
        }
        boolean[] nulls = null;
        Vector columnNulls = columnInput.getOrNull(org.weakref.nitro.data.Stream.NULLS);
        if (!VectorAccess.isAllFalseNulls(columnNulls)) {
            nulls = VectorAccess.flatBooleans(columnNulls);
            if (nulls == null) {
                return false;   // non-flat null encoding: fall back to the general per-position path
            }
        }

        applyConstantComparison(mask, column, literal, effectiveOperator(operator, wantTrue), nulls);
        return true;
    }

    private static Mask tryConstantComparisonMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, Allocator.Context allocationContext, Mask.ComparisonOperator baseOperator, boolean wantTrue, boolean returnedConstantComparisonMasks)
    {
        if (!returnedConstantComparisonMasks || baseOperator == null) {
            return null;
        }

        Vector left = inputs.get(0).values();
        Vector right = inputs.get(1).values();

        Vector column;
        long literal;
        Mask.ComparisonOperator operator;
        Long rightConstant = singleRunLong(right);
        if (rightConstant != null && isFlatInteger(left)) {
            column = left;
            literal = rightConstant;
            operator = baseOperator;
        }
        else {
            Long leftConstant = singleRunLong(left);
            if (leftConstant == null || !isFlatInteger(right)) {
                return null;
            }
            column = right;
            literal = leftConstant;
            operator = swapOperands(baseOperator);
        }

        boolean columnOnLeft = column == left;
        Streams columnInput = columnOnLeft ? inputs.get(0) : inputs.get(1);
        Streams literalInput = columnOnLeft ? inputs.get(1) : inputs.get(0);
        if (!isNullAndErrorFree(literalInput) || !isErrorFree(columnInput)) {
            return null;
        }

        boolean[] nulls = null;
        Vector columnNulls = columnInput.getOrNull(org.weakref.nitro.data.Stream.NULLS);
        if (!VectorAccess.isAllFalseNulls(columnNulls)) {
            nulls = VectorAccess.flatBooleans(columnNulls);
            if (nulls == null) {
                return null;
            }
        }

        Mask.ComparisonOperator effectiveOperator = effectiveOperator(operator, wantTrue);
        if (column instanceof I32Vector values) {
            return context.allocator().constantComparisonMask(allocationContext, mask, values.values(), literal, effectiveOperator, nulls);
        }
        return context.allocator().constantComparisonMask(allocationContext, mask, ((I64Vector) column).values(), literal, effectiveOperator, nulls);
    }

    private static boolean isErrorFree(Streams input)
    {
        return VectorAccess.isAllFalseNulls(input.getOrNull(org.weakref.nitro.data.Stream.ERRORS));
    }

    /**
     * Apply a precomputed per-entry dictionary comparison as a monomorphic scan ({@link Mask#retainDictionaryComparison})
     * rather than a per-position {@link java.util.function.IntPredicate}. Handles a nullable (but error-free) dictionary
     * column by excluding null positions in the scan; the literal operand must be null/error-free. {@code wanted}
     * selects the true ({@code true}) or false ({@code false}) mask. Returns {@code false} to fall back when an input
     * carries errors or a non-flat null stream.
     */
    private static boolean tryApplyDictionaryMonomorphic(List<Streams> inputs, Mask mask, boolean[] keep, int[] ids, boolean wanted)
    {
        if (!isErrorFree(inputs.get(0)) || !isErrorFree(inputs.get(1))) {
            return false;
        }
        boolean dictionaryOnLeft = inputs.get(0).values() instanceof DictionaryVector;
        Streams columnInput = dictionaryOnLeft ? inputs.get(0) : inputs.get(1);
        Streams literalInput = dictionaryOnLeft ? inputs.get(1) : inputs.get(0);
        if (!isNullAndErrorFree(literalInput)) {
            return false;
        }
        boolean[] nulls = null;
        Vector columnNulls = columnInput.getOrNull(org.weakref.nitro.data.Stream.NULLS);
        if (!VectorAccess.isAllFalseNulls(columnNulls)) {
            nulls = VectorAccess.flatBooleans(columnNulls);
            if (nulls == null) {
                return false;
            }
        }
        mask.retainDictionaryComparison(ids, keep, nulls, wanted);
        return true;
    }

    private static boolean isNullAndErrorFree(Streams input)
    {
        return VectorAccess.isAllFalseNulls(input.getOrNull(org.weakref.nitro.data.Stream.NULLS))
                && VectorAccess.isAllFalseNulls(input.getOrNull(org.weakref.nitro.data.Stream.ERRORS));
    }

    private static boolean isFlatInteger(Vector vector)
    {
        return vector instanceof I32Vector || vector instanceof I64Vector;
    }

    private static void applyConstantComparison(Mask mask, Vector column, long literal, Mask.ComparisonOperator operator, boolean[] nulls)
    {
        if (column instanceof I32Vector values) {
            mask.retainConstantComparison(values.values(), literal, operator, nulls);
        }
        else {
            mask.retainConstantComparison(((I64Vector) column).values(), literal, operator, nulls);
        }
    }

    private static Mask.ComparisonOperator effectiveOperator(Mask.ComparisonOperator operator, boolean wantTrue)
    {
        return wantTrue ? operator : negate(operator);
    }

    private static Mask.ComparisonOperator negate(Mask.ComparisonOperator operator)
    {
        return switch (operator) {
            case EQUAL -> Mask.ComparisonOperator.NOT_EQUAL;
            case NOT_EQUAL -> Mask.ComparisonOperator.EQUAL;
            case LESS_THAN -> Mask.ComparisonOperator.GREATER_THAN_OR_EQUAL;
            case LESS_THAN_OR_EQUAL -> Mask.ComparisonOperator.GREATER_THAN;
            case GREATER_THAN -> Mask.ComparisonOperator.LESS_THAN_OR_EQUAL;
            case GREATER_THAN_OR_EQUAL -> Mask.ComparisonOperator.LESS_THAN;
        };
    }

    private static Mask.ComparisonOperator swapOperands(Mask.ComparisonOperator operator)
    {
        return switch (operator) {
            case EQUAL -> Mask.ComparisonOperator.EQUAL;
            case NOT_EQUAL -> Mask.ComparisonOperator.NOT_EQUAL;
            case LESS_THAN -> Mask.ComparisonOperator.GREATER_THAN;
            case LESS_THAN_OR_EQUAL -> Mask.ComparisonOperator.GREATER_THAN_OR_EQUAL;
            case GREATER_THAN -> Mask.ComparisonOperator.LESS_THAN;
            case GREATER_THAN_OR_EQUAL -> Mask.ComparisonOperator.LESS_THAN_OR_EQUAL;
        };
    }

    private static boolean supportsLongComparison(List<Streams> inputs)
    {
        return inputs.size() == 2 && supportsLongValues(inputs.get(0).values()) && supportsLongValues(inputs.get(1).values());
    }

    private static boolean supportsLongValues(Vector vector)
    {
        return switch (vector) {
            case I32Vector _, I64Vector _ -> true;
            case DictionaryVector dictionary -> supportsLongValues(dictionary.values());
            case RleVector rle -> supportsLongValues(rle.values());
            default -> false;
        };
    }

    private static boolean isNull(VectorAccess.BooleanValues nulls, int position)
    {
        return nulls.value(position);
    }

    private static boolean isError(VectorAccess.BooleanValues errors, int position)
    {
        return errors.value(position);
    }
}
