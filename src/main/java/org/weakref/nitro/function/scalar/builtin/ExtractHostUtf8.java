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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "extract_host_utf8")
public final class ExtractHostUtf8
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ExtractHostUtf8");
    private static final byte[] HTTP_PREFIX = "http://".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HTTPS_PREFIX = "https://".getBytes(StandardCharsets.UTF_8);
    private static final byte[] WWW_PREFIX = "www.".getBytes(StandardCharsets.UTF_8);

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
        checkArgument(inputs.size() == 1, "Unexpected argument count for extract_host_utf8");

        Vector values = inputs.getFirst().values();
        Vector inputNulls = inputs.getFirst().getOrNull(Stream.NULLS);
        VectorAccess.BooleanValues inputNullValues = VectorAccess.booleanValues(inputNulls);
        int requiredLength = Math.max(mask.maxPosition() + 1, values.length());

        int totalBytes = 0;
        if (requestedStreams.contains(Stream.VALUES) && values instanceof DictionaryVector dictionary && dictionary.values() instanceof BinaryVector binaryValues) {
            totalBytes = dictionaryTotalBytes(binaryValues);
        }
        else if (requestedStreams.contains(Stream.VALUES)) {
            for (int position : mask) {
                if (!inputNullValues.value(position)) {
                    totalBytes += extractLength(values, position);
                }
            }
        }

        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.NULLS)) {
            BooleanVector outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    requiredLength);
            copyNulls(inputNullValues, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }
        if (requestedStreams.contains(Stream.VALUES)) {
            result = result.with(Stream.VALUES, applyValues(values, inputNullValues, mask, totalBytes, requiredLength, output, context));
        }
        return result;
    }

    private static Vector applyValues(Vector values, VectorAccess.BooleanValues inputNulls, Mask mask, int totalBytes, int requiredLength, Streams output, PrimitiveExecutionContext context)
    {
        if (values instanceof DictionaryVector dictionary && dictionary.values() instanceof BinaryVector dictionaryValues) {
            return applyDictionary(dictionary, dictionaryValues, totalBytes, requiredLength, output, context);
        }

        BinaryVector outputValues = BinaryVector.allocateOrGrow(
                context.allocator(),
                ALLOCATION_CONTEXT,
                output != null && output.has(Stream.VALUES) && output.values() instanceof BinaryVector vector ? vector : null,
                requiredLength,
                totalBytes);
        outputValues.clearTraits();
        outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        if (values instanceof BinaryVector binaryValues && binaryValues.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY)) {
            outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        }
        applyFlatValues(values, inputNulls, mask, outputValues);
        return outputValues;
    }

    private static void applyFlatValues(Vector values, VectorAccess.BooleanValues inputNulls, Mask mask, BinaryVector output)
    {
        for (int position : mask) {
            if (inputNulls.value(position)) {
                output.setNull(position);
                continue;
            }
            writeExtracted(values, position, output, position);
        }
    }

    private static DictionaryVector applyDictionary(DictionaryVector dictionary, BinaryVector dictionaryValues, int totalBytes, int requiredLength, Streams output, PrimitiveExecutionContext context)
    {
        BinaryVector existingDictionaryValues = output != null && output.has(Stream.VALUES) && output.values() instanceof DictionaryVector existingDictionary && existingDictionary.values() instanceof BinaryVector vector
                ? vector
                : null;
        BinaryVector extractedValues = BinaryVector.allocateOrGrow(
                context.allocator(),
                ALLOCATION_CONTEXT,
                existingDictionaryValues,
                dictionaryValues.length(),
                totalBytes);
        extractedValues.clearTraits();
        extractedValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        if (dictionaryValues.hasTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY)) {
            extractedValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        }
        for (int position = 0; position < dictionaryValues.length(); position++) {
            writeExtracted(dictionaryValues, position, extractedValues, position);
        }
        return context.allocator().adopt(ALLOCATION_CONTEXT, DictionaryVector.wrap(Arrays.copyOf(dictionary.ids(), requiredLength), extractedValues));
    }

    private static int dictionaryTotalBytes(BinaryVector dictionaryValues)
    {
        int totalBytes = 0;
        for (int position = 0; position < dictionaryValues.length(); position++) {
            totalBytes += extractLength(dictionaryValues, position);
        }
        return totalBytes;
    }

    private static int extractLength(Vector values, int position)
    {
        return switch (values) {
            case BinaryVector vector -> rangeLength(extractRange(vector.data(), vector.startOffset(position), vector.endOffset(position)));
            case DictionaryVector vector -> extractLength(vector.values(), vector.ids()[position]);
            case RleVector vector -> extractLength(vector.values(), vector.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported extract_host_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static void writeExtracted(Vector values, int inputPosition, BinaryVector output, int outputPosition)
    {
        switch (values) {
            case BinaryVector vector -> {
                long range = extractRange(vector.data(), vector.startOffset(inputPosition), vector.endOffset(inputPosition));
                output.setBytes(outputPosition, vector.data(), rangeStart(range), rangeLength(range));
            }
            case DictionaryVector vector -> writeExtracted(vector.values(), vector.ids()[inputPosition], output, outputPosition);
            case RleVector vector -> writeExtracted(vector.values(), vector.runIndex(inputPosition), output, outputPosition);
            default -> throw new IllegalArgumentException("Unsupported extract_host_utf8 vector type: " + values.getClass().getSimpleName());
        }
    }

    private static long extractRange(byte[] data, int start, int end)
    {
        int hostStart = start;
        if (startsWith(data, start, end, HTTP_PREFIX)) {
            hostStart += HTTP_PREFIX.length;
        }
        else if (startsWith(data, start, end, HTTPS_PREFIX)) {
            hostStart += HTTPS_PREFIX.length;
        }
        if (startsWith(data, hostStart, end, WWW_PREFIX)) {
            hostStart += WWW_PREFIX.length;
        }
        int hostEnd = hostStart;
        while (hostEnd < end && data[hostEnd] != '/') {
            hostEnd++;
        }
        return packRange(hostStart, hostEnd - hostStart);
    }

    private static boolean startsWith(byte[] data, int start, int end, byte[] prefix)
    {
        if (start + prefix.length > end) {
            return false;
        }
        for (int index = 0; index < prefix.length; index++) {
            if (data[start + index] != prefix[index]) {
                return false;
            }
        }
        return true;
    }

    private static long packRange(int start, int length)
    {
        return (((long) start) << 32) | (length & 0xFFFF_FFFFL);
    }

    private static int rangeStart(long range)
    {
        return (int) (range >>> 32);
    }

    private static int rangeLength(long range)
    {
        return (int) range;
    }

    private static void copyNulls(VectorAccess.BooleanValues inputNulls, Mask mask, BooleanVector output)
    {
        boolean[] outputValues = output.values();
        if (mask.all()) {
            for (int position = 0; position < mask.size(); position++) {
                outputValues[position] = inputNulls.value(position);
            }
            return;
        }
        for (int position : mask) {
            outputValues[position] = inputNulls.value(position);
        }
    }
}
