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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

@ScalarFunction(name = "regexp_replace_utf8")
public final class RegexpReplaceUtf8
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("RegexpReplaceUtf8");

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

        Vector rewritten = applyGeneric(values, valuesNulls, patternValues, patternNulls, replacementValues, replacementNulls, mask, requiredLength, output, context, outputNulls);
        return result.with(Stream.VALUES, rewritten);
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
            String javaReplacement = toJavaReplacement(replacementValue);
            byte[] bytes = pattern.matcher(input)
                    .replaceAll(javaReplacement)
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
        outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        if (asciiOnly) {
            outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
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

    private static String utf8Value(Vector values, int position)
    {
        return switch (values) {
            case BinaryVector binary -> binary.utf8Value(position);
            case RleVector rle -> utf8Value(rle.values(), rle.runIndex(position));
            case org.weakref.nitro.data.DictionaryVector dictionary -> utf8Value(dictionary.values(), dictionary.ids()[position]);
            default -> throw new IllegalArgumentException("Unsupported regexp_replace_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    private static boolean isNull(BooleanVector nulls, int position)
    {
        return nulls != null && nulls.values()[position];
    }

    private static String toJavaReplacement(String replacement)
    {
        StringBuilder translated = new StringBuilder(replacement.length());
        for (int index = 0; index < replacement.length(); index++) {
            char current = replacement.charAt(index);
            if (current == '\\' && index + 1 < replacement.length()) {
                char escaped = replacement.charAt(index + 1);
                if (Character.isDigit(escaped)) {
                    translated.append('$');
                    translated.append(escaped);
                }
                else {
                    translated.append(Matcher.quoteReplacement(String.valueOf(escaped)));
                }
                index++;
                continue;
            }
            translated.append(Matcher.quoteReplacement(String.valueOf(current)));
        }
        return translated.toString();
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
}
