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
import org.weakref.nitro.data.I64Vector;
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

@ScalarFunction(name = "substring_utf8")
public final class SubstringUtf8
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("SubstringUtf8");

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(ALLOCATION_CONTEXT);
    }

    @Override
    public boolean requiresInputCompanionStreams()
    {
        return true;
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 3, "Unexpected argument count for substring_utf8");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        BooleanVector valueNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        BooleanVector startNulls = (BooleanVector) inputs.get(1).getOrNull(Stream.NULLS);
        BooleanVector lengthNulls = (BooleanVector) inputs.get(2).getOrNull(Stream.NULLS);
        int requiredLength = mask.none() ? 0 : mask.maxPosition() + 1;
        int totalBytes = 0;
        Vector values = null;
        Vector startValues = null;
        Vector lengthValues = null;
        if (requestedStreams.contains(Stream.VALUES)) {
            values = inputs.get(0).values();
            startValues = inputs.get(1).values();
            lengthValues = inputs.get(2).values();
            requiredLength = Math.max(requiredLength, values.length());
            for (int position : mask) {
                if (isNull(valueNulls, position) || isNull(startNulls, position) || isNull(lengthNulls, position)) {
                    continue;
                }
                totalBytes += substringValue(values, startValues, lengthValues, position).length;
            }
        }

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.getOrNull(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            applyNulls(valueNulls, startNulls, lengthNulls, mask, outputNulls);
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
            applyValues(values, startValues, lengthValues, valueNulls, startNulls, lengthNulls, mask, outputValues, outputNulls);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void applyNulls(BooleanVector valueNulls, BooleanVector startNulls, BooleanVector lengthNulls, Mask mask, BooleanVector outputNulls)
    {
        Arrays.fill(outputNulls.values(), false);
        for (int position : mask) {
            outputNulls.values()[position] = isNull(valueNulls, position) || isNull(startNulls, position) || isNull(lengthNulls, position);
        }
    }

    private static void applyValues(Vector values, Vector startValues, Vector lengthValues, BooleanVector valueNulls, BooleanVector startNulls, BooleanVector lengthNulls, Mask mask, BinaryVector outputValues, BooleanVector outputNulls)
    {
        int currentOffset = 0;
        int lastPosition = -1;
        boolean asciiOnly = true;
        for (int position : mask) {
            fillOffsets(outputValues, lastPosition + 1, position, currentOffset);
            outputValues.offsets()[position] = currentOffset;
            if (isNull(valueNulls, position) || isNull(startNulls, position) || isNull(lengthNulls, position)) {
                outputValues.setNull(position);
                if (outputNulls != null) {
                    outputNulls.values()[position] = true;
                }
            }
            else {
                byte[] substring = substringValue(values, startValues, lengthValues, position);
                outputValues.setBytes(position, substring);
                currentOffset = outputValues.endOffset(position);
                if (outputNulls != null) {
                    outputNulls.values()[position] = false;
                }
                if (asciiOnly && !isAsciiOnly(substring)) {
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

    private static byte[] substringValue(Vector values, Vector startValues, Vector lengthValues, int position)
    {
        long start = longValue(startValues, position);
        return switch (values) {
            case BinaryVector vector -> Utf8Support.substring(vector, position, start, longValue(lengthValues, position));
            case DictionaryVector vector -> substringValue(vector.values(), startValues, lengthValues, vector.ids()[position]);
            case RleVector vector -> substringValue(vector.values(), startValues, lengthValues, vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported substring_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static long longValue(Vector values, int position)
    {
        return switch (values) {
            case I64Vector vector -> vector.values()[position];
            case DictionaryVector vector -> longValue(vector.values(), vector.ids()[position]);
            case RleVector vector -> longValue(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported substring_utf8 index vector type: " + values.getClass().getSimpleName());
        };
    }

    private static void fillOffsets(BinaryVector outputValues, int startInclusive, int endExclusive, int offset)
    {
        for (int index = startInclusive; index <= endExclusive; index++) {
            outputValues.offsets()[index] = offset;
        }
    }

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
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
