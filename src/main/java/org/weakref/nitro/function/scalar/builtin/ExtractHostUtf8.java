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

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "extract_host_utf8")
public final class ExtractHostUtf8
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ExtractHostUtf8");

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
        checkArgument(inputs.size() == 1, "Unexpected argument count for extract_host_utf8");

        Vector values = inputs.getFirst().values();
        BooleanVector inputNulls = (BooleanVector) inputs.getFirst().getOrNull(Stream.NULLS);
        int requiredLength = Math.max(mask.maxPosition() + 1, values.length());

        int totalBytes = 0;
        if (requestedStreams.contains(Stream.VALUES)) {
            for (int position : mask) {
                if (!isNull(inputNulls, position)) {
                    totalBytes += extractLength(values, position);
                }
            }
        }

        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) && output.get(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            copyNulls(inputNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            BinaryVector outputValues = context.allocator().allocateOrGrowBinary(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.VALUES) && output.values() instanceof BinaryVector vector ? vector : null,
                    requiredLength,
                    totalBytes);
            outputValues.addTrait(BinaryVector.Trait.UTF8_STRING);
            applyValues(values, inputNulls, mask, outputValues);
            result = result.with(Stream.VALUES, outputValues);
        }
        return result;
    }

    private static void applyValues(Vector values, BooleanVector inputNulls, Mask mask, BinaryVector output)
    {
        for (int position : mask) {
            if (isNull(inputNulls, position)) {
                output.setNull(position);
                continue;
            }
            writeExtracted(values, position, output, position);
        }
    }

    private static int extractLength(Vector values, int position)
    {
        return switch (values) {
            case BinaryVector vector -> extractRange(vector.data(), vector.startOffset(position), vector.endOffset(position))[1];
            case DictionaryVector vector -> extractLength(vector.values(), vector.ids()[position]);
            case RleVector vector -> extractLength(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported extract_host_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static void writeExtracted(Vector values, int inputPosition, BinaryVector output, int outputPosition)
    {
        switch (values) {
            case BinaryVector vector -> {
                int[] range = extractRange(vector.data(), vector.startOffset(inputPosition), vector.endOffset(inputPosition));
                output.setBytes(outputPosition, vector.data(), range[0], range[1]);
            }
            case DictionaryVector vector -> writeExtracted(vector.values(), vector.ids()[inputPosition], output, outputPosition);
            case RleVector vector -> writeExtracted(vector.values(), vector.runIndex(inputPosition), output, outputPosition);
            default -> throw new IllegalArgumentException("Unsupported extract_host_utf8 vector type: " + values.getClass().getSimpleName());
        }
    }

    private static int[] extractRange(byte[] data, int start, int end)
    {
        int hostStart = start;
        if (startsWith(data, start, end, "http://")) {
            hostStart += 7;
        }
        else if (startsWith(data, start, end, "https://")) {
            hostStart += 8;
        }
        if (startsWith(data, hostStart, end, "www.")) {
            hostStart += 4;
        }
        int hostEnd = hostStart;
        while (hostEnd < end && data[hostEnd] != '/') {
            hostEnd++;
        }
        return new int[] {hostStart, hostEnd - hostStart};
    }

    private static boolean startsWith(byte[] data, int start, int end, String prefix)
    {
        byte[] prefixBytes = prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (start + prefixBytes.length > end) {
            return false;
        }
        for (int index = 0; index < prefixBytes.length; index++) {
            if (data[start + index] != prefixBytes[index]) {
                return false;
            }
        }
        return true;
    }

    private static void copyNulls(BooleanVector inputNulls, Mask mask, BooleanVector output)
    {
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = isNull(inputNulls, position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = isNull(inputNulls, position);
        }
    }

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }
}
