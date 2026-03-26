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
import org.weakref.nitro.data.BooleanVector;
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

    public static MaskOutcome tryEvaluateMaskOutcome(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, Allocator.Context allocationContext, ComparisonKernel kernel)
    {
        if (!supportsLongComparison(inputs)) {
            return null;
        }

        Vector leftValues = inputs.get(0).values();
        Vector rightValues = inputs.get(1).values();
        BooleanVector leftNulls = (BooleanVector) inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS);
        BooleanVector rightNulls = (BooleanVector) inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS);
        BooleanVector leftErrors = (BooleanVector) inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS);
        BooleanVector rightErrors = (BooleanVector) inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS);

        int trueCount = 0;
        int nullCount = 0;
        int errorCount = 0;
        for (int position : mask) {
            if (isError(leftErrors, position) || isError(rightErrors, position)) {
                errorCount++;
            }
            else if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                nullCount++;
            }
            else if (kernel.test(integerValue(leftValues, position), integerValue(rightValues, position))) {
                trueCount++;
            }
        }

        int[] truePositions = new int[trueCount];
        int[] nullPositions = new int[nullCount];
        int[] errorPositions = new int[errorCount];
        int trueIndex = 0;
        int nullIndex = 0;
        int errorIndex = 0;
        for (int position : mask) {
            if (isError(leftErrors, position) || isError(rightErrors, position)) {
                errorPositions[errorIndex++] = position;
            }
            else if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                nullPositions[nullIndex++] = position;
            }
            else if (kernel.test(integerValue(leftValues, position), integerValue(rightValues, position))) {
                truePositions[trueIndex++] = position;
            }
        }

        return new MaskOutcome(
                context.allocator().allocateSparseMask(allocationContext, truePositions, trueIndex, mask.size()),
                context.allocator().allocateSparseMask(allocationContext, nullPositions, nullIndex, mask.size()),
                context.allocator().allocateSparseMask(allocationContext, errorPositions, errorIndex, mask.size()));
    }

    public static Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, Allocator.Context allocationContext, ComparisonKernel kernel)
    {
        if (!supportsLongComparison(inputs)) {
            return null;
        }

        Vector leftValues = inputs.get(0).values();
        Vector rightValues = inputs.get(1).values();
        BooleanVector leftNulls = (BooleanVector) inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS);
        BooleanVector rightNulls = (BooleanVector) inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS);
        BooleanVector leftErrors = (BooleanVector) inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS);
        BooleanVector rightErrors = (BooleanVector) inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS);

        int trueCount = 0;
        for (int position : mask) {
            if (isError(leftErrors, position) || isError(rightErrors, position) || isNull(leftNulls, position) || isNull(rightNulls, position)) {
                continue;
            }
            if (kernel.test(integerValue(leftValues, position), integerValue(rightValues, position))) {
                trueCount++;
            }
        }

        int[] truePositions = new int[trueCount];
        int trueIndex = 0;
        for (int position : mask) {
            if (isError(leftErrors, position) || isError(rightErrors, position) || isNull(leftNulls, position) || isNull(rightNulls, position)) {
                continue;
            }
            if (kernel.test(integerValue(leftValues, position), integerValue(rightValues, position))) {
                truePositions[trueIndex++] = position;
            }
        }
        return context.allocator().allocateSparseMask(allocationContext, truePositions, trueIndex, mask.size());
    }

    public static Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, Allocator.Context allocationContext, ComparisonKernel kernel)
    {
        if (!supportsLongComparison(inputs)) {
            return null;
        }

        Vector leftValues = inputs.get(0).values();
        Vector rightValues = inputs.get(1).values();
        BooleanVector leftNulls = (BooleanVector) inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS);
        BooleanVector rightNulls = (BooleanVector) inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS);
        BooleanVector leftErrors = (BooleanVector) inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS);
        BooleanVector rightErrors = (BooleanVector) inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS);

        int falseCount = 0;
        for (int position : mask) {
            if (isError(leftErrors, position) || isError(rightErrors, position) || isNull(leftNulls, position) || isNull(rightNulls, position)) {
                continue;
            }
            if (!kernel.test(integerValue(leftValues, position), integerValue(rightValues, position))) {
                falseCount++;
            }
        }

        int[] falsePositions = new int[falseCount];
        int falseIndex = 0;
        for (int position : mask) {
            if (isError(leftErrors, position) || isError(rightErrors, position) || isNull(leftNulls, position) || isNull(rightNulls, position)) {
                continue;
            }
            if (!kernel.test(integerValue(leftValues, position), integerValue(rightValues, position))) {
                falsePositions[falseIndex++] = position;
            }
        }
        return context.allocator().allocateSparseMask(allocationContext, falsePositions, falseIndex, mask.size());
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

    private static long integerValue(Vector vector, int position)
    {
        return switch (vector) {
            case I32Vector values -> values.values()[position];
            case I64Vector values -> values.values()[position];
            case DictionaryVector values -> integerValue(values.values(), values.ids()[position]);
            case RleVector values -> integerValue(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected integer vector but got " + vector.getClass().getSimpleName());
        };
    }

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }

    private static boolean isError(BooleanVector errors, int position)
    {
        return errors != null && errors.values()[position];
    }
}
