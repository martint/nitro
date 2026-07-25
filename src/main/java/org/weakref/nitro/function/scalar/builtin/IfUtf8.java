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
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "if_utf8", capabilities = IfUtf8Optimization.class)
public final class IfUtf8
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("IfUtf8");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(allocationContext);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAndNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 3, "Unexpected argument count for if_utf8");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector condition = inputs.get(0).values();
        Vector conditionNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector trueValues = inputs.get(1).values();
        Vector falseValues = inputs.get(2).values();
        Vector trueNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector falseNulls = inputs.get(2).getOrNull(Stream.NULLS);
        VectorAccess.BooleanValues conditionValues = VectorAccess.booleanValues(condition);
        VectorAccess.BinaryValues trueBranchValues = VectorAccess.binaryValues(trueValues);
        VectorAccess.BinaryValues falseBranchValues = VectorAccess.binaryValues(falseValues);
        int requiredLength = Math.max(mask.maxPosition() + 1, Math.max(trueValues.length(), falseValues.length()));

        int totalBytes = 0;
        if (requestedStreams.contains(Stream.VALUES)) {
            for (int position : mask) {
                VectorAccess.BinaryValues selected = conditionValue(conditionValues, conditionNulls, position) ? trueBranchValues : falseBranchValues;
                totalBytes += selected.value(position).length();
            }
        }

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    requiredLength);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            BinaryVector outputValues = BinaryVector.allocateOrGrow(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof BinaryVector vector ? vector : null,
                    requiredLength,
                    totalBytes);
            outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
            applyValues(conditionValues, conditionNulls, trueBranchValues, falseBranchValues, trueNulls, falseNulls, mask, outputValues, outputNulls);
            result = result.with(Stream.VALUES, outputValues);
            return result;
        }

        if (outputNulls != null) {
            applyNulls(conditionValues, conditionNulls, trueNulls, falseNulls, mask, outputNulls);
        }
        return result;
    }

    private static void applyValues(VectorAccess.BooleanValues conditionValues, Vector conditionNulls, VectorAccess.BinaryValues trueValues, VectorAccess.BinaryValues falseValues, Vector trueNulls, Vector falseNulls, Mask mask, BinaryVector outputValues, BooleanVector outputNulls)
    {
        VectorAccess.BooleanValues conditionNullValues = VectorAccess.booleanValues(conditionNulls);
        VectorAccess.BooleanValues trueNullValues = VectorAccess.booleanValues(trueNulls);
        VectorAccess.BooleanValues falseNullValues = VectorAccess.booleanValues(falseNulls);
        // The mask can be sparse, but the offsets array is a cumulative chain over ALL positions -- fill the
        // skipped positions' offsets forward (empty values) so positional writes after a gap stay aligned.
        int currentOffset = 0;
        int lastPosition = -1;
        for (int position : mask) {
            fillOffsets(outputValues, lastPosition + 1, position, currentOffset);
            boolean takeTrue = !conditionNullValues.value(position) && conditionValues.value(position);
            VectorAccess.BinaryValues selectedValues = takeTrue ? trueValues : falseValues;
            VectorAccess.BooleanValues selectedNullValues = takeTrue ? trueNullValues : falseNullValues;
            if (selectedNullValues.value(position)) {
                outputValues.setNull(position);
                if (outputNulls != null) {
                    outputNulls.values()[position] = true;
                }
            }
            else {
                copyBytes(selectedValues, position, outputValues, position);
                currentOffset = outputValues.endOffset(position);
                if (outputNulls != null) {
                    outputNulls.values()[position] = false;
                }
            }
            lastPosition = position;
        }
        fillOffsets(outputValues, lastPosition + 1, outputValues.length(), currentOffset);
    }

    private static void fillOffsets(BinaryVector outputValues, int startInclusive, int endExclusive, int offset)
    {
        for (int position = startInclusive; position <= endExclusive; position++) {
            outputValues.offsets()[position] = offset;
        }
    }

    private static void applyNulls(VectorAccess.BooleanValues conditionValues, Vector conditionNulls, Vector trueNulls, Vector falseNulls, Mask mask, BooleanVector outputNulls)
    {
        VectorAccess.BooleanValues conditionNullValues = VectorAccess.booleanValues(conditionNulls);
        VectorAccess.BooleanValues trueNullValues = VectorAccess.booleanValues(trueNulls);
        VectorAccess.BooleanValues falseNullValues = VectorAccess.booleanValues(falseNulls);
        boolean[] nulls = outputNulls.values();
        Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            boolean takeTrue = !conditionNullValues.value(position) && conditionValues.value(position);
            nulls[position] = takeTrue ? trueNullValues.value(position) : falseNullValues.value(position);
        }
    }

    private static boolean conditionValue(VectorAccess.BooleanValues values, Vector nulls, int position)
    {
        if (VectorAccess.isNull(nulls, position)) {
            return false;
        }
        return values.value(position);
    }

    private static void copyBytes(VectorAccess.BinaryValues values, int inputPosition, BinaryVector output, int outputPosition)
    {
        VectorAccess.BinarySlice value = values.value(inputPosition);
        output.setBytes(outputPosition, value.data(), value.offset(), value.length());
    }
}
