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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarFunction;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.PrimitiveExecutionContext;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "upper_utf8")
public final class UpperUtf8
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("UpperUtf8");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAndNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 1, "Unexpected argument count for upper_utf8");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector values = inputs.getFirst().values();
        Vector valueNulls = inputs.getFirst().getOrNull(Stream.NULLS);
        VectorAccess.BooleanValues valueNullValues = VectorAccess.booleanValues(valueNulls);
        int requiredLength = mask.none() ? 0 : Math.max(mask.maxPosition() + 1, values.length());

        int totalBytes = 0;
        if (requestedStreams.contains(Stream.VALUES)) {
            for (int position : mask) {
                if (valueNullValues.value(position)) {
                    continue;
                }
                totalBytes += upperValue(values, position).length;
            }
        }

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    ALLOCATION_CONTEXT,
                    output != null ? output.getOrNull(Stream.NULLS) : null,
                    requiredLength);
            applyNulls(valueNullValues, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            BinaryVector outputValues = BinaryVector.allocateOrGrow(
                    context.allocator(),
                    ALLOCATION_CONTEXT,
                    output != null && output.getOrNull(Stream.VALUES) instanceof BinaryVector vector ? vector : null,
                    requiredLength,
                    totalBytes);
            outputValues.clearTraits();
            outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
            applyValues(values, valueNullValues, mask, outputValues, outputNulls);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void applyNulls(VectorAccess.BooleanValues valueNulls, Mask mask, BooleanVector outputNulls)
    {
        Arrays.fill(outputNulls.values(), false);
        for (int position : mask) {
            outputNulls.values()[position] = valueNulls.value(position);
        }
    }

    private static void applyValues(Vector values, VectorAccess.BooleanValues valueNulls, Mask mask, BinaryVector outputValues, BooleanVector outputNulls)
    {
        int currentOffset = 0;
        int lastPosition = -1;
        boolean asciiOnly = true;
        for (int position : mask) {
            fillOffsets(outputValues, lastPosition + 1, position, currentOffset);
            outputValues.offsets()[position] = currentOffset;
            if (valueNulls.value(position)) {
                outputValues.setNull(position);
                if (outputNulls != null) {
                    outputNulls.values()[position] = true;
                }
            }
            else {
                byte[] upper = upperValue(values, position);
                outputValues.setBytes(position, upper);
                currentOffset = outputValues.endOffset(position);
                if (outputNulls != null) {
                    outputNulls.values()[position] = false;
                }
                if (asciiOnly && !isAsciiOnly(upper)) {
                    asciiOnly = false;
                }
            }
            lastPosition = position;
        }
        fillOffsets(outputValues, lastPosition + 1, outputValues.length(), currentOffset);
        if (asciiOnly) {
            outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        }
    }

    private static byte[] upperValue(Vector values, int position)
    {
        return switch (values) {
            case BinaryVector vector -> Utf8Support.upper(vector, position);
            case DictionaryVector vector -> upperValue(vector.values(), vector.ids()[position]);
            case RleVector vector -> upperValue(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported upper_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static void fillOffsets(BinaryVector outputValues, int startInclusive, int endExclusive, int offset)
    {
        for (int index = startInclusive; index <= endExclusive; index++) {
            outputValues.offsets()[index] = offset;
        }
    }

    private static boolean isAsciiOnly(byte[] value)
    {
        for (byte current : value) {
            if ((current & 0x80) != 0) {
                return false;
            }
        }
        return true;
    }
}
