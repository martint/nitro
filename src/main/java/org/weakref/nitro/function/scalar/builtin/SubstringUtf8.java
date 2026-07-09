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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
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
import java.util.OptionalLong;
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
    public boolean propagatesNulls()
    {
        return true;
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAndNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 3, "Unexpected argument count for substring_utf8");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        VectorAccess.BooleanValues valueNulls = VectorAccess.booleanValues(inputs.get(0).getOrNull(Stream.NULLS));
        VectorAccess.BooleanValues startNulls = VectorAccess.booleanValues(inputs.get(1).getOrNull(Stream.NULLS));
        VectorAccess.BooleanValues lengthNulls = VectorAccess.booleanValues(inputs.get(2).getOrNull(Stream.NULLS));
        Vector inputValues = null;
        int requiredLength = mask.none() ? 0 : mask.maxPosition() + 1;
        if (requestedStreams.contains(Stream.VALUES)) {
            inputValues = inputs.get(0).values();
            requiredLength = Math.max(requiredLength, inputValues.length());

            Streams encodedResult = tryApplyEncodedConstantSubstring(inputs, mask, requestedStreams, requiredLength, context);
            if (encodedResult != null) {
                return encodedResult;
            }
        }

        int totalBytes = 0;
        VectorAccess.BinaryValues values = null;
        VectorAccess.LongValues startValues = null;
        VectorAccess.LongValues lengthValues = null;
        if (requestedStreams.contains(Stream.VALUES)) {
            values = VectorAccess.binaryValues(inputValues);
            startValues = VectorAccess.longValues(inputs.get(1).values());
            lengthValues = VectorAccess.longValues(inputs.get(2).values());
            for (int position : mask) {
                if (valueNulls.value(position) || startNulls.value(position) || lengthNulls.value(position)) {
                    continue;
                }
                totalBytes += substringValue(values.value(position), startValues.value(position), lengthValues.value(position)).length;
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

    private static Streams tryApplyEncodedConstantSubstring(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, int requiredLength, PrimitiveExecutionContext context)
    {
        if (!requestedStreams.contains(Stream.VALUES)) {
            return null;
        }

        Vector inputValues = inputs.get(0).values();
        if (!(inputValues instanceof DictionaryVector || inputValues instanceof RleVector)) {
            return null;
        }

        OptionalLong start = constantNonNullLong(inputs.get(1));
        OptionalLong length = constantNonNullLong(inputs.get(2));
        if (start.isEmpty() || length.isEmpty()) {
            return null;
        }

        Streams sparseDictionaryResult = tryApplySparseDictionaryConstantSubstring(
                inputValues,
                inputs.get(0).getOrNull(Stream.NULLS),
                mask,
                requestedStreams,
                requiredLength,
                start.getAsLong(),
                length.getAsLong(),
                context);
        if (sparseDictionaryResult != null) {
            return sparseDictionaryResult;
        }
        if (encodedValueCount(inputValues) > (long) mask.selectedCount() * 8) {
            return null;
        }

        Vector values = deriveEncodedSubstrings(inputValues, start.getAsLong(), length.getAsLong(), context);
        Streams result = Streams.ofValues(values);
        if (requestedStreams.contains(Stream.NULLS)) {
            Vector valueNulls = inputs.get(0).getOrNull(Stream.NULLS);
            if (valueNulls != null) {
                result = result.with(Stream.NULLS, valueNulls);
            }
            else {
                BooleanVector nulls = VectorAccess.writableBooleanVector(context.allocator(), ALLOCATION_CONTEXT, null, requiredLength);
                nulls.markAllFalse();
                result = result.with(Stream.NULLS, nulls);
            }
        }
        return result;
    }

    private static Streams tryApplySparseDictionaryConstantSubstring(Vector inputValues, Vector valueNullsVector, Mask mask, Set<Stream> requestedStreams, int requiredLength, long start, long length, PrimitiveExecutionContext context)
    {
        if (!(inputValues instanceof DictionaryVector dictionary) || mask.none()) {
            return null;
        }
        if (encodedValueCount(dictionary.values()) <= (long) mask.selectedCount() * 8) {
            return null;
        }

        int[] sourceIds = dictionary.ids();
        int[] nestedIds = null;
        BinaryVector binaryValues = null;
        if (dictionary.values() instanceof BinaryVector values) {
            binaryValues = values;
        }
        else if (dictionary.values() instanceof DictionaryVector nestedDictionary && nestedDictionary.values() instanceof BinaryVector values) {
            nestedIds = nestedDictionary.ids();
            binaryValues = values;
        }
        int[] outputIds = new int[dictionary.length()];
        VectorAccess.BooleanValues valueNulls = VectorAccess.isAllFalseNulls(valueNullsVector)
                ? _ -> false
                : VectorAccess.booleanValues(valueNullsVector);
        VectorAccess.BinaryValues dictionaryValues = binaryValues == null ? VectorAccess.binaryValues(dictionary.values()) : null;
        Int2IntOpenHashMap outputIdBySourceId = new Int2IntOpenHashMap(Math.min(mask.selectedCount(), 1024));
        outputIdBySourceId.defaultReturnValue(-1);
        byte[][] substrings = new byte[Math.max(1, mask.selectedCount())][];
        int uniqueCount = 0;
        int totalBytes = 0;
        boolean asciiOnly = true;

        for (int position : mask) {
            if (valueNulls.value(position)) {
                continue;
            }
            int sourceId = sourceIds[position];
            int valueId = nestedIds == null ? sourceId : nestedIds[sourceId];
            int outputId = outputIdBySourceId.get(valueId);
            if (outputId < 0) {
                byte[] substring;
                if (binaryValues == null) {
                    VectorAccess.BinarySlice value = dictionaryValues.value(sourceId);
                    substring = Utf8Support.substring(value.data(), value.offset(), value.length(), start, length);
                }
                else {
                    substring = Utf8Support.substring(binaryValues.data(), binaryValues.startOffset(valueId), binaryValues.length(valueId), start, length);
                }
                outputId = uniqueCount++;
                outputIdBySourceId.put(valueId, outputId);
                substrings[outputId] = substring;
                totalBytes += substring.length;
                if (asciiOnly && !isAsciiOnly(substring)) {
                    asciiOnly = false;
                }
            }
            outputIds[position] = outputId;
        }
        if (uniqueCount == 0) {
            substrings[0] = new byte[0];
            uniqueCount = 1;
        }

        BinaryVector outputValues = BinaryVector.allocate(context.allocator(), ALLOCATION_CONTEXT, uniqueCount, totalBytes);
        outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        for (int position = 0; position < uniqueCount; position++) {
            outputValues.setBytes(position, substrings[position]);
        }
        if (asciiOnly) {
            outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        }

        Streams result = Streams.ofValues(context.allocator().adopt(ALLOCATION_CONTEXT, DictionaryVector.wrap(outputIds, dictionary.length(), outputValues)));
        if (requestedStreams.contains(Stream.NULLS)) {
            if (valueNullsVector != null) {
                result = result.with(Stream.NULLS, valueNullsVector);
            }
            else {
                BooleanVector nulls = VectorAccess.writableBooleanVector(context.allocator(), ALLOCATION_CONTEXT, null, requiredLength);
                nulls.markAllFalse();
                result = result.with(Stream.NULLS, nulls);
            }
        }
        return result;
    }

    private static int encodedValueCount(Vector values)
    {
        return switch (values) {
            case BinaryVector binary -> binary.length();
            case DictionaryVector dictionary -> encodedValueCount(dictionary.values());
            case RleVector rle -> encodedValueCount(rle.values());
            default -> Integer.MAX_VALUE;
        };
    }

    private static OptionalLong constantNonNullLong(Streams input)
    {
        if (!(input.values() instanceof RleVector rle) || rle.counts().length != 1 || rle.length() == 0) {
            return OptionalLong.empty();
        }
        if (!VectorAccess.isAllFalseNulls(input.getOrNull(Stream.NULLS))) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(VectorAccess.longValues(rle).value(0));
    }

    private static Vector deriveEncodedSubstrings(Vector values, long start, long length, PrimitiveExecutionContext context)
    {
        return switch (values) {
            case BinaryVector binary -> deriveBinarySubstrings(binary, start, length, context);
            case DictionaryVector dictionary -> context.allocator().allocateDictionary(
                    ALLOCATION_CONTEXT,
                    Arrays.copyOf(dictionary.ids(), dictionary.length()),
                    deriveEncodedSubstrings(dictionary.values(), start, length, context));
            case RleVector rle -> context.allocator().allocateRle(
                    ALLOCATION_CONTEXT,
                    rle.counts(),
                    deriveEncodedSubstrings(rle.values(), start, length, context));
            default -> throw new IllegalArgumentException("Unsupported substring_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static BinaryVector deriveBinarySubstrings(BinaryVector values, long start, long length, PrimitiveExecutionContext context)
    {
        byte[][] substrings = new byte[values.length()][];
        int totalBytes = 0;
        boolean asciiOnly = true;
        for (int position = 0; position < values.length(); position++) {
            byte[] substring = Utf8Support.substring(values.data(), values.startOffset(position), values.length(position), start, length);
            substrings[position] = substring;
            totalBytes += substring.length;
            if (asciiOnly && !isAsciiOnly(substring)) {
                asciiOnly = false;
            }
        }

        BinaryVector output = BinaryVector.allocate(context.allocator(), ALLOCATION_CONTEXT, values.length(), totalBytes);
        output.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        for (int position = 0; position < substrings.length; position++) {
            output.setBytes(position, substrings[position]);
        }
        if (asciiOnly) {
            output.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        }
        return output;
    }

    private static void applyNulls(VectorAccess.BooleanValues valueNulls, VectorAccess.BooleanValues startNulls, VectorAccess.BooleanValues lengthNulls, Mask mask, BooleanVector outputNulls)
    {
        Arrays.fill(outputNulls.values(), false);
        for (int position : mask) {
            outputNulls.values()[position] = valueNulls.value(position) || startNulls.value(position) || lengthNulls.value(position);
        }
    }

    private static void applyValues(VectorAccess.BinaryValues values, VectorAccess.LongValues startValues, VectorAccess.LongValues lengthValues, VectorAccess.BooleanValues valueNulls, VectorAccess.BooleanValues startNulls, VectorAccess.BooleanValues lengthNulls, Mask mask, BinaryVector outputValues, BooleanVector outputNulls)
    {
        int currentOffset = 0;
        int lastPosition = -1;
        boolean asciiOnly = true;
        for (int position : mask) {
            fillOffsets(outputValues, lastPosition + 1, position, currentOffset);
            outputValues.offsets()[position] = currentOffset;
            if (valueNulls.value(position) || startNulls.value(position) || lengthNulls.value(position)) {
                outputValues.setNull(position);
                if (outputNulls != null) {
                    outputNulls.values()[position] = true;
                }
            }
            else {
                byte[] substring = substringValue(values.value(position), startValues.value(position), lengthValues.value(position));
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

    private static byte[] substringValue(VectorAccess.BinarySlice value, long start, long length)
    {
        return Utf8Support.substring(value.data(), value.offset(), value.length(), start, length);
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
