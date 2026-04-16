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

    public static MaskOutcome tryEvaluateMaskOutcome(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context, Allocator.Context allocationContext, ComparisonKernel kernel)
    {
        if (!supportsLongComparison(inputs)) {
            return null;
        }

        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));

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

        VectorAccess.LongValues leftValues = VectorAccess.longValues(inputs.get(0).values());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(inputs.get(1).values());
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));

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

        VectorAccess.LongValues leftValues = VectorAccess.longValues(inputs.get(0).values());
        VectorAccess.LongValues rightValues = VectorAccess.longValues(inputs.get(1).values());
        VectorAccess.BooleanValues leftNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues rightNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.NULLS));
        VectorAccess.BooleanValues leftErrors = VectorAccess.booleanValues(inputs.get(0).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));
        VectorAccess.BooleanValues rightErrors = VectorAccess.booleanValues(inputs.get(1).getOrNull(org.weakref.nitro.operator.evaluator.ir.Stream.ERRORS));

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
