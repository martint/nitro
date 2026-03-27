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

@ScalarFunction(name = "concat_utf8")
public final class ConcatUtf8
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ConcatUtf8");

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
        checkArgument(inputs.size() == 2, "Unexpected argument count for concat_utf8");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Vector leftValues = inputs.get(0).values();
        Vector rightValues = inputs.get(1).values();
        BooleanVector leftNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        BooleanVector rightNulls = (BooleanVector) inputs.get(1).getOrNull(Stream.NULLS);
        int requiredLength = mask.none() ? 0 : Math.max(mask.maxPosition() + 1, Math.max(leftValues.length(), rightValues.length()));

        int totalBytes = 0;
        if (requestedStreams.contains(Stream.VALUES)) {
            for (int position : mask) {
                if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                    continue;
                }
                totalBytes += byteLength(leftValues, position) + byteLength(rightValues, position);
            }
        }

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) && output.get(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            applyNulls(leftNulls, rightNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            BinaryVector outputValues = BinaryVector.allocateOrGrow(
                    context.allocator(),
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof BinaryVector vector ? vector : null,
                    requiredLength,
                    totalBytes);
            outputValues.clearTraits();
            outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
            if (isAsciiOnly(leftValues) && isAsciiOnly(rightValues)) {
                outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
            }
            applyValues(leftValues, rightValues, leftNulls, rightNulls, mask, outputValues, outputNulls);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void applyNulls(BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BooleanVector outputNulls)
    {
        Arrays.fill(outputNulls.values(), false);
        for (int position : mask) {
            outputNulls.values()[position] = isNull(leftNulls, position) || isNull(rightNulls, position);
        }
    }

    private static void applyValues(Vector leftValues, Vector rightValues, BooleanVector leftNulls, BooleanVector rightNulls, Mask mask, BinaryVector outputValues, BooleanVector outputNulls)
    {
        int currentOffset = 0;
        int lastPosition = -1;
        for (int position : mask) {
            fillOffsets(outputValues, lastPosition + 1, position, currentOffset);
            outputValues.offsets()[position] = currentOffset;
            if (isNull(leftNulls, position) || isNull(rightNulls, position)) {
                outputValues.setNull(position);
                if (outputNulls != null) {
                    outputNulls.values()[position] = true;
                }
            }
            else {
                currentOffset = copyBytes(leftValues, position, outputValues, position, currentOffset);
                currentOffset = copyBytes(rightValues, position, outputValues, position, currentOffset);
                outputValues.offsets()[position + 1] = currentOffset;
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
        for (int index = startInclusive; index <= endExclusive; index++) {
            outputValues.offsets()[index] = offset;
        }
    }

    private static int copyBytes(Vector values, int inputPosition, BinaryVector outputValues, int outputPosition, int currentOffset)
    {
        return switch (values) {
            case BinaryVector vector -> {
                int length = vector.length(inputPosition);
                System.arraycopy(vector.data(), vector.startOffset(inputPosition), outputValues.data(), currentOffset, length);
                yield currentOffset + length;
            }
            case DictionaryVector vector -> copyBytes(vector.values(), vector.ids()[inputPosition], outputValues, outputPosition, currentOffset);
            case RleVector vector -> copyBytes(vector.values(), vector.runIndex(inputPosition), outputValues, outputPosition, currentOffset);
            default -> throw new IllegalArgumentException("Unsupported concat_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static int byteLength(Vector values, int position)
    {
        return switch (values) {
            case BinaryVector vector -> vector.length(position);
            case DictionaryVector vector -> byteLength(vector.values(), vector.ids()[position]);
            case RleVector vector -> byteLength(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported concat_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static boolean isAsciiOnly(Vector values)
    {
        return switch (values) {
            case BinaryVector vector -> vector.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
            case DictionaryVector vector -> isAsciiOnly(vector.values());
            case RleVector vector -> isAsciiOnly(vector.values());
            default -> false;
        };
    }

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }
}
