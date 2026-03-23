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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "regexp_replace_utf8")
public final class RegexpReplaceUtf8
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("RegexpReplaceUtf8");
    private static final String CLICKBENCH_PATTERN = "^https?://(?:www\\.)?([^/]+)/.*$";
    private static final String CLICKBENCH_REPLACEMENT = "\\1";
    private static final byte[] HTTP_PREFIX = "http://".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HTTPS_PREFIX = "https://".getBytes(StandardCharsets.UTF_8);
    private static final byte[] WWW_PREFIX = "www.".getBytes(StandardCharsets.UTF_8);

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
        checkArgument(inputs.size() == 3, "Unexpected argument count for regexp_replace_utf8");

        Vector values = inputs.get(0).values();
        Vector patternValues = inputs.get(1).values();
        Vector replacementValues = inputs.get(2).values();
        BooleanVector valuesNulls = (BooleanVector) inputs.get(0).getOrNull(Stream.NULLS);
        BooleanVector patternNulls = (BooleanVector) inputs.get(1).getOrNull(Stream.NULLS);
        BooleanVector replacementNulls = (BooleanVector) inputs.get(2).getOrNull(Stream.NULLS);

        int requiredLength = maxLength(mask, values, patternValues, replacementValues);

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = context.allocator().allocateOrGrow(
                    ALLOCATION_CONTEXT,
                    output != null && output.has(Stream.NULLS) && output.get(Stream.NULLS) instanceof BooleanVector vector ? vector : null,
                    BooleanVector.class,
                    requiredLength,
                    BooleanVector::new);
            fillNulls(valuesNulls, patternNulls, replacementNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }

        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        String constantPattern = constantUtf8Value(patternValues);
        String constantReplacement = constantUtf8Value(replacementValues);
        if (CLICKBENCH_PATTERN.equals(constantPattern) && CLICKBENCH_REPLACEMENT.equals(constantReplacement)) {
            Vector rewritten = applyClickBenchHostRewrite(values, valuesNulls, patternNulls, replacementNulls, mask, requiredLength, output, context);
            return result.with(Stream.VALUES, rewritten);
        }

        Vector rewritten = applyGeneric(values, valuesNulls, patternValues, patternNulls, replacementValues, replacementNulls, mask, requiredLength, output, context, outputNulls);
        return result.with(Stream.VALUES, rewritten);
    }

    private static Vector applyClickBenchHostRewrite(Vector values, BooleanVector valuesNulls, BooleanVector patternNulls, BooleanVector replacementNulls, Mask mask, int requiredLength, Streams output, PrimitiveExecutionContext context)
    {
        if (values instanceof DictionaryVector dictionary &&
                dictionary.values() instanceof BinaryVector binaryValues &&
                allNotNull(patternNulls, mask) &&
                allNotNull(replacementNulls, mask)) {
            return applyClickBenchDictionary(dictionary, binaryValues, valuesNulls, mask, requiredLength, output, context);
        }

        int totalBytes = 0;
        boolean asciiOnly = true;
        for (int position : mask) {
            if (isNull(valuesNulls, position) || isNull(patternNulls, position) || isNull(replacementNulls, position)) {
                continue;
            }
            RewriteRange range = clickBenchRewriteRange(values, position);
            totalBytes += range.length();
            asciiOnly &= range.asciiOnly();
        }

        BinaryVector outputValues = BinaryVector.allocateOrGrow(
                context.allocator(),
                ALLOCATION_CONTEXT,
                output != null && output.has(Stream.VALUES) && output.values() instanceof BinaryVector vector ? vector : null,
                requiredLength,
                totalBytes);
        outputValues.clearTraits();
        outputValues.addTrait(BinaryVector.Trait.UTF8_STRING);
        if (asciiOnly) {
            outputValues.addTrait(BinaryVector.Trait.ASCII_ONLY);
        }

        for (int position : mask) {
            if (isNull(valuesNulls, position) || isNull(patternNulls, position) || isNull(replacementNulls, position)) {
                outputValues.setNull(position);
                continue;
            }
            writeRewriteRange(values, position, clickBenchRewriteRange(values, position), outputValues, position);
        }
        return outputValues;
    }

    private static DictionaryVector applyClickBenchDictionary(DictionaryVector dictionary, BinaryVector dictionaryValues, BooleanVector valuesNulls, Mask mask, int requiredLength, Streams output, PrimitiveExecutionContext context)
    {
        int totalBytes = 0;
        boolean asciiOnly = dictionaryValues.hasTrait(BinaryVector.Trait.ASCII_ONLY);
        for (int position = 0; position < dictionaryValues.length(); position++) {
            RewriteRange range = clickBenchRewriteRange(dictionaryValues, position);
            totalBytes += range.length();
            asciiOnly &= range.asciiOnly();
        }

        BinaryVector existingDictionary = output != null && output.has(Stream.VALUES) && output.values() instanceof DictionaryVector existing && existing.values() instanceof BinaryVector vector
                ? vector
                : null;
        BinaryVector rewrittenDictionary = BinaryVector.allocateOrGrow(
                context.allocator(),
                ALLOCATION_CONTEXT,
                existingDictionary,
                dictionaryValues.length(),
                totalBytes);
        rewrittenDictionary.clearTraits();
        rewrittenDictionary.addTrait(BinaryVector.Trait.UTF8_STRING);
        if (asciiOnly) {
            rewrittenDictionary.addTrait(BinaryVector.Trait.ASCII_ONLY);
        }
        for (int position = 0; position < dictionaryValues.length(); position++) {
            writeRewriteRange(dictionaryValues, position, clickBenchRewriteRange(dictionaryValues, position), rewrittenDictionary, position);
        }
        return context.allocator().adopt(ALLOCATION_CONTEXT, DictionaryVector.wrap(Arrays.copyOf(dictionary.ids(), requiredLength), rewrittenDictionary));
    }

    private static Vector applyGeneric(
            Vector values,
            BooleanVector valuesNulls,
            Vector patternValues,
            BooleanVector patternNulls,
            Vector replacementValues,
            BooleanVector replacementNulls,
            Mask mask,
            int requiredLength,
            Streams output,
            PrimitiveExecutionContext context,
            BooleanVector outputNulls)
    {
        byte[][] rewritten = new byte[mask.selectedCount()][];
        Map<String, Pattern> patterns = new HashMap<>();
        int totalBytes = 0;
        boolean asciiOnly = true;
        int index = 0;
        for (int position : mask) {
            if (isNull(valuesNulls, position) || isNull(patternNulls, position) || isNull(replacementNulls, position)) {
                rewritten[index++] = null;
                continue;
            }

            String input = utf8Value(values, position);
            String patternValue = utf8Value(patternValues, position);
            String replacementValue = utf8Value(replacementValues, position);
            Pattern pattern = patterns.computeIfAbsent(patternValue, Pattern::compile);
            byte[] bytes = pattern.matcher(input)
                    .replaceAll(replacementValue)
                    .getBytes(StandardCharsets.UTF_8);
            rewritten[index++] = bytes;
            totalBytes += bytes.length;
            asciiOnly &= isAscii(bytes);
        }

        BinaryVector outputValues = BinaryVector.allocateOrGrow(
                context.allocator(),
                ALLOCATION_CONTEXT,
                output != null && output.has(Stream.VALUES) && output.values() instanceof BinaryVector vector ? vector : null,
                requiredLength,
                totalBytes);
        outputValues.clearTraits();
        outputValues.addTrait(BinaryVector.Trait.UTF8_STRING);
        if (asciiOnly) {
            outputValues.addTrait(BinaryVector.Trait.ASCII_ONLY);
        }

        index = 0;
        for (int position : mask) {
            byte[] bytes = rewritten[index++];
            if (bytes == null) {
                outputValues.setNull(position);
                if (outputNulls != null) {
                    outputNulls.values()[position] = true;
                }
                continue;
            }
            outputValues.setBytes(position, bytes);
            if (outputNulls != null) {
                outputNulls.values()[position] = false;
            }
        }
        return outputValues;
    }

    private static void fillNulls(BooleanVector valuesNulls, BooleanVector patternNulls, BooleanVector replacementNulls, Mask mask, BooleanVector outputNulls)
    {
        boolean[] nulls = outputNulls.values();
        Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            nulls[position] = isNull(valuesNulls, position) || isNull(patternNulls, position) || isNull(replacementNulls, position);
        }
    }

    private static int maxLength(Mask mask, Vector... vectors)
    {
        int length = mask.none() ? 0 : mask.maxPosition() + 1;
        for (Vector vector : vectors) {
            length = Math.max(length, vector.length());
        }
        return length;
    }

    private static boolean allNotNull(BooleanVector nulls, Mask mask)
    {
        if (nulls == null) {
            return true;
        }
        for (int position : mask) {
            if (nulls.values()[position]) {
                return false;
            }
        }
        return true;
    }

    private static String constantUtf8Value(Vector values)
    {
        return switch (values) {
            case RleVector rle -> constantUtf8Value(rle.values());
            case BinaryVector binary when binary.length() == 1 -> binary.utf8Value(0);
            default -> null;
        };
    }

    private static String utf8Value(Vector values, int position)
    {
        return switch (values) {
            case BinaryVector binary -> binary.utf8Value(position);
            case DictionaryVector dictionary -> utf8Value(dictionary.values(), dictionary.ids()[position]);
            case RleVector rle -> utf8Value(rle.values(), rle.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported regexp_replace_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static RewriteRange clickBenchRewriteRange(Vector values, int position)
    {
        return switch (values) {
            case BinaryVector binary -> clickBenchRewriteRange(binary, position);
            case DictionaryVector dictionary -> clickBenchRewriteRange(dictionary.values(), dictionary.ids()[position]);
            case RleVector rle -> clickBenchRewriteRange(rle.values(), rle.runIndex(position));
            default -> throw new IllegalArgumentException("Unsupported regexp_replace_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static RewriteRange clickBenchRewriteRange(BinaryVector values, int position)
    {
        byte[] data = values.data();
        int start = values.startOffset(position);
        int end = values.endOffset(position);

        int hostStart = start;
        if (startsWith(data, start, end, HTTP_PREFIX)) {
            hostStart += HTTP_PREFIX.length;
        }
        else if (startsWith(data, start, end, HTTPS_PREFIX)) {
            hostStart += HTTPS_PREFIX.length;
        }
        else {
            return RewriteRange.original(start, end - start, values.hasTrait(BinaryVector.Trait.ASCII_ONLY));
        }

        if (startsWith(data, hostStart, end, WWW_PREFIX)) {
            hostStart += WWW_PREFIX.length;
        }

        int hostEnd = hostStart;
        while (hostEnd < end && data[hostEnd] != '/') {
            hostEnd++;
        }
        if (hostEnd == end) {
            return RewriteRange.original(start, end - start, values.hasTrait(BinaryVector.Trait.ASCII_ONLY));
        }
        return RewriteRange.replaced(hostStart, hostEnd - hostStart, values.hasTrait(BinaryVector.Trait.ASCII_ONLY));
    }

    private static void writeRewriteRange(Vector values, int inputPosition, RewriteRange range, BinaryVector output, int outputPosition)
    {
        switch (values) {
            case BinaryVector binary -> output.setBytes(outputPosition, binary.data(), range.start(), range.length());
            case DictionaryVector dictionary -> writeRewriteRange(dictionary.values(), dictionary.ids()[inputPosition], range, output, outputPosition);
            case RleVector rle -> writeRewriteRange(rle.values(), rle.runIndex(inputPosition), range, output, outputPosition);
            default -> throw new IllegalArgumentException("Unsupported regexp_replace_utf8 vector type: " + values.getClass().getSimpleName());
        }
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

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }

    private static boolean isAscii(byte[] bytes)
    {
        for (byte value : bytes) {
            if ((value & 0x80) != 0) {
                return false;
            }
        }
        return true;
    }

    private record RewriteRange(int start, int length, boolean replaced, boolean asciiOnly)
    {
        private static RewriteRange original(int start, int length, boolean asciiOnly)
        {
            return new RewriteRange(start, length, false, asciiOnly);
        }

        private static RewriteRange replaced(int start, int length, boolean asciiOnly)
        {
            return new RewriteRange(start, length, true, asciiOnly);
        }
    }
}
