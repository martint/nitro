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
package org.weakref.nitro.function.scalar.builtin;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.MaskOutcome;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;

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
    private static DictionaryComparison tryDictionaryComparison(Vector left, Vector right, ComparisonKernel kernel)
    {
        if (left instanceof DictionaryVector dictionary && singleRunLong(right) != null) {
            long literal = singleRunLong(right);
            return perEntry(dictionary, literal, kernel, true);
        }
        if (right instanceof DictionaryVector dictionary && singleRunLong(left) != null) {
            long literal = singleRunLong(left);
            return perEntry(dictionary, literal, kernel, false);
        }
        return null;
    }

    private static DictionaryComparison perEntry(DictionaryVector dictionary, long literal, ComparisonKernel kernel, boolean dictionaryOnLeft)
    {
        Vector entries = dictionary.values();
        if (!(entries instanceof I64Vector || entries instanceof I32Vector)) {
            return null;   // nested encodings are rare here; fall back to the per-row path
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

    /** Split positions into true / null / error using the per-entry comparison; counts only when {@code fill} is false. */
    private static void categorizeDictionary(DictionaryComparison comparison, Mask mask,
            VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls,
            VectorAccess.BooleanValues leftErrors, VectorAccess.BooleanValues rightErrors,
            int[] truePositions, int[] nullPositions, int[] errorPositions, int[] outCounts, boolean fill)
    {
        boolean[] result = comparison.result();
        int[] ids = comparison.ids();
        int trueCount = 0;
        int nullCount = 0;
        int errorCount = 0;
        if (mask.all()) {
            int max = mask.maxPosition();
            for (int position = 0; position <= max; position++) {
                if (isError(leftErrors, position) || isError(rightErrors, position)) {
                    if (fill) {
                        errorPositions[errorCount] = position;
                    }
                    errorCount++;
                }
                else if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                    if (fill) {
                        nullPositions[nullCount] = position;
                    }
                    nullCount++;
                }
                else if (result[ids[position]]) {
                    if (fill) {
                        truePositions[trueCount] = position;
                    }
                    trueCount++;
                }
            }
        }
        else {
            for (int position : mask) {
                if (isError(leftErrors, position) || isError(rightErrors, position)) {
                    if (fill) {
                        errorPositions[errorCount] = position;
                    }
                    errorCount++;
                }
                else if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                    if (fill) {
                        nullPositions[nullCount] = position;
                    }
                    nullCount++;
                }
                else if (result[ids[position]]) {
                    if (fill) {
                        truePositions[trueCount] = position;
                    }
                    trueCount++;
                }
            }
        }
        outCounts[0] = trueCount;
        outCounts[1] = nullCount;
        outCounts[2] = errorCount;
    }

    private static MaskOutcome dictionaryMaskOutcome(DictionaryComparison comparison, Mask mask,
            PrimitiveExecutionContext context, Allocator.Context allocationContext,
            VectorAccess.BooleanValues leftNulls, VectorAccess.BooleanValues rightNulls,
            VectorAccess.BooleanValues leftErrors, VectorAccess.BooleanValues rightErrors)
    {
        int[] counts = new int[3];
        categorizeDictionary(comparison, mask, leftNulls, rightNulls, leftErrors, rightErrors, null, null, null, counts, false);
        int[] truePositions = new int[counts[0]];
        int[] nullPositions = new int[counts[1]];
        int[] errorPositions = new int[counts[2]];
        categorizeDictionary(comparison, mask, leftNulls, rightNulls, leftErrors, rightErrors, truePositions, nullPositions, errorPositions, counts, true);
        return new MaskOutcome(
                context.allocator().allocateSparseMask(allocationContext, truePositions, counts[0], mask.size()),
                context.allocator().allocateSparseMask(allocationContext, nullPositions, counts[1], mask.size()),
                context.allocator().allocateSparseMask(allocationContext, errorPositions, counts[2], mask.size()));
    }

    public static MaskOutcome tryEvaluateMaskOutcome(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, Allocator.Context allocationContext, ComparisonKernel kernel)
    {
        if (!supportsLongComparison(inputs)) {
            return null;
        }

        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));

        DictionaryComparison comparison = tryDictionaryComparison(inputs.get(0).values(), inputs.get(1).values(), kernel);
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

    public static Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, Allocator.Context allocationContext, ComparisonKernel kernel)
    {
        if (!supportsLongComparison(inputs)) {
            return null;
        }

        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));

        int[] truePositions = new int[mask.count()];
        DictionaryComparison comparison = tryDictionaryComparison(inputs.get(0).values(), inputs.get(1).values(), kernel);
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

    public static Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, Allocator.Context allocationContext, ComparisonKernel kernel)
    {
        if (!supportsLongComparison(inputs)) {
            return null;
        }

        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));

        int[] falsePositions = new int[mask.count()];
        DictionaryComparison comparison = tryDictionaryComparison(inputs.get(0).values(), inputs.get(1).values(), kernel);
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

    public static boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, ComparisonKernel kernel)
    {
        if (!supportsLongComparison(inputs)) {
            return false;
        }

        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));

        DictionaryComparison comparison = tryDictionaryComparison(inputs.get(0).values(), inputs.get(1).values(), kernel);
        if (comparison != null) {
            boolean[] result = comparison.result();
            int[] ids = comparison.ids();
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

    public static boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, ComparisonKernel kernel)
    {
        if (!supportsLongComparison(inputs)) {
            return false;
        }

        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));

        DictionaryComparison comparison = tryDictionaryComparison(inputs.get(0).values(), inputs.get(1).values(), kernel);
        if (comparison != null) {
            boolean[] result = comparison.result();
            int[] ids = comparison.ids();
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
