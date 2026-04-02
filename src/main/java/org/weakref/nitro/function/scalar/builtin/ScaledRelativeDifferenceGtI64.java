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
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.MaskEvaluablePrimitiveFunction;
import org.weakref.nitro.operator.evaluator.MaskOutcome;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "scaled_relative_difference_gt_i64")
public final class ScaledRelativeDifferenceGtI64
        implements PrimitiveFunction, MaskEvaluablePrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ScaledRelativeDifferenceGtI64");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAlwaysNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Set<Stream> requiredMaskInputStreams(int inputIndex)
    {
        return PrimitiveFunction.VALUES_AND_NULLS_INPUT_STREAMS;
    }

    @Override
    public boolean requiresCompletedInputCompanionStreamsForMask()
    {
        return false;
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 3, "Unexpected argument count for scaled_relative_difference_gt_i64");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector value = inputs.get(0).values();
        Vector baseline = inputs.get(1).values();
        Vector scale = inputs.get(2).values();
        Vector valueNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector baselineNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector scaleNulls = inputs.get(2).getOrNull(Stream.NULLS);
        BooleanVector existingValues = output != null && output.has(Stream.VALUES) ? (BooleanVector) output.values() : null;
        BooleanVector existingNulls = output != null && output.has(Stream.NULLS) ? (BooleanVector) output.get(Stream.NULLS) : null;

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        int requiredLength = Math.max(mask.maxPosition() + 1, Math.max(value.length(), Math.max(baseline.length(), scale.length())));
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    existingNulls,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            applyNulls(valueNulls, baselineNulls, scaleNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        BooleanVector outputValues = context.allocator().allocateOrGrow(
                ALLOCATION_CONTEXT,
                existingValues,
                BooleanVector.class,
                requiredLength,
                BooleanVector::new);
        applyValues(value, baseline, scale, valueNulls, baselineNulls, scaleNulls, mask, outputValues);
        return result.with(Stream.VALUES, outputValues);
    }

    @Override
    public MaskOutcome tryEvaluateMaskOutcome(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        if (!supportsInputs(inputs)) {
            return null;
        }

        int trueCount = 0;
        int nullCount = 0;
        for (int position : mask) {
            if (isNull(inputs.get(0).getOrNull(Stream.NULLS), position) ||
                    isNull(inputs.get(1).getOrNull(Stream.NULLS), position) ||
                    isNull(inputs.get(2).getOrNull(Stream.NULLS), position)) {
                nullCount++;
                continue;
            }
            if (apply(integerValue(inputs.get(0).values(), position), integerValue(inputs.get(1).values(), position), integerValue(inputs.get(2).values(), position))) {
                trueCount++;
            }
        }

        int[] truePositions = new int[trueCount];
        int[] nullPositions = new int[nullCount];
        int trueIndex = 0;
        int nullIndex = 0;
        for (int position : mask) {
            if (isNull(inputs.get(0).getOrNull(Stream.NULLS), position) ||
                    isNull(inputs.get(1).getOrNull(Stream.NULLS), position) ||
                    isNull(inputs.get(2).getOrNull(Stream.NULLS), position)) {
                nullPositions[nullIndex++] = position;
                continue;
            }
            if (apply(integerValue(inputs.get(0).values(), position), integerValue(inputs.get(1).values(), position), integerValue(inputs.get(2).values(), position))) {
                truePositions[trueIndex++] = position;
            }
        }

        return new MaskOutcome(
                context.allocator().allocateSparseMask(ALLOCATION_CONTEXT, truePositions, trueIndex, mask.size()),
                context.allocator().allocateSparseMask(ALLOCATION_CONTEXT, nullPositions, nullIndex, mask.size()),
                context.allocator().allocateSparseMask(ALLOCATION_CONTEXT, new int[0], 0, mask.size()));
    }

    @Override
    public Mask tryEvaluateTrueMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        if (!supportsInputs(inputs)) {
            return null;
        }

        int[] truePositions = new int[mask.selectedCount()];
        int trueIndex = 0;
        for (int position : mask) {
            if (isNull(inputs.get(0).getOrNull(Stream.NULLS), position) ||
                    isNull(inputs.get(1).getOrNull(Stream.NULLS), position) ||
                    isNull(inputs.get(2).getOrNull(Stream.NULLS), position)) {
                continue;
            }
            if (apply(integerValue(inputs.get(0).values(), position), integerValue(inputs.get(1).values(), position), integerValue(inputs.get(2).values(), position))) {
                truePositions[trueIndex++] = position;
            }
        }
        return context.allocator().allocateSparseMask(ALLOCATION_CONTEXT, truePositions, trueIndex, mask.size());
    }

    @Override
    public Mask tryEvaluateFalseMask(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        if (!supportsInputs(inputs)) {
            return null;
        }

        int[] falsePositions = new int[mask.selectedCount()];
        int falseIndex = 0;
        for (int position : mask) {
            if (isNull(inputs.get(0).getOrNull(Stream.NULLS), position) ||
                    isNull(inputs.get(1).getOrNull(Stream.NULLS), position) ||
                    isNull(inputs.get(2).getOrNull(Stream.NULLS), position)) {
                continue;
            }
            if (!apply(integerValue(inputs.get(0).values(), position), integerValue(inputs.get(1).values(), position), integerValue(inputs.get(2).values(), position))) {
                falsePositions[falseIndex++] = position;
            }
        }
        return context.allocator().allocateSparseMask(ALLOCATION_CONTEXT, falsePositions, falseIndex, mask.size());
    }

    @Override
    public boolean tryEvaluateTrueMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        if (!supportsInputs(inputs)) {
            return false;
        }

        mask.retainIf(position ->
                !isNull(inputs.get(0).getOrNull(Stream.NULLS), position) &&
                        !isNull(inputs.get(1).getOrNull(Stream.NULLS), position) &&
                        !isNull(inputs.get(2).getOrNull(Stream.NULLS), position) &&
                        apply(integerValue(inputs.get(0).values(), position), integerValue(inputs.get(1).values(), position), integerValue(inputs.get(2).values(), position)));
        return true;
    }

    @Override
    public boolean tryEvaluateFalseMaskInPlace(List<Streams> inputs, Mask mask, PrimitiveExecutionContext context)
    {
        if (!supportsInputs(inputs)) {
            return false;
        }

        mask.retainIf(position ->
                !isNull(inputs.get(0).getOrNull(Stream.NULLS), position) &&
                        !isNull(inputs.get(1).getOrNull(Stream.NULLS), position) &&
                        !isNull(inputs.get(2).getOrNull(Stream.NULLS), position) &&
                        !apply(integerValue(inputs.get(0).values(), position), integerValue(inputs.get(1).values(), position), integerValue(inputs.get(2).values(), position)));
        return true;
    }

    private static void applyNulls(Vector valueNulls, Vector baselineNulls, Vector scaleNulls, Mask mask, BooleanVector outputNulls)
    {
        boolean[] nulls = outputNulls.values();
        java.util.Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            nulls[position] = isNull(valueNulls, position) || isNull(baselineNulls, position) || isNull(scaleNulls, position);
        }
    }

    private static void applyValues(Vector value, Vector baseline, Vector scale, Vector valueNulls, Vector baselineNulls, Vector scaleNulls, Mask mask, BooleanVector outputValues)
    {
        boolean[] values = outputValues.values();
        for (int position : mask) {
            if (isNull(valueNulls, position) || isNull(baselineNulls, position) || isNull(scaleNulls, position)) {
                continue;
            }
            values[position] = apply(integerValue(value, position), integerValue(baseline, position), integerValue(scale, position));
        }
    }

    private static boolean apply(long value, long baseline, long scale)
    {
        if (!(0L < baseline)) {
            return false;
        }

        long absoluteDifference = value < baseline ? baseline - value : value - baseline;
        long scaledDifference = absoluteDifference * scale;
        return baseline < scaledDifference;
    }

    private static boolean supportsInputs(List<Streams> inputs)
    {
        return inputs.size() == 3 &&
                supportsLongValues(inputs.get(0).values()) &&
                supportsLongValues(inputs.get(1).values()) &&
                supportsLongValues(inputs.get(2).values());
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

    private static boolean isNull(Vector nulls, int position)
    {
        if (nulls == null) {
            return false;
        }
        return switch (nulls) {
            case BooleanVector values -> values.values()[position];
            case DictionaryVector values -> isNull(values.values(), values.ids()[position]);
            case RleVector values -> isNull(values.values(), values.runIndex(position));
            default -> throw new IllegalArgumentException("Expected boolean vector but got " + nulls.getClass().getSimpleName());
        };
    }
}
