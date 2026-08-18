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

import io.airlift.joni.Regex;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ScalarFunction(name = "regexp_replace_utf8")
public final class RegexpReplaceUtf8
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("RegexpReplaceUtf8");
    private final RegexpReplaceUtf8Policy policy;
    private final JoniRegexpPolicy joniPolicy;

    public RegexpReplaceUtf8()
    {
        this(RegexpReplaceUtf8Policy.defaults(), JoniRegexpPolicy.defaults());
    }

    public RegexpReplaceUtf8(RegexpReplaceUtf8Policy policy)
    {
        this(policy, JoniRegexpPolicy.defaults());
    }

    public RegexpReplaceUtf8(RegexpReplaceUtf8Policy policy, JoniRegexpPolicy joniPolicy)
    {
        this.policy = requireNonNull(policy, "policy is null");
        this.joniPolicy = requireNonNull(joniPolicy, "joniPolicy is null");
    }

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
        checkArgument(inputs.size() == 3, "Unexpected argument count for regexp_replace_utf8");

        Vector values = inputs.get(0).values();
        Vector patternValues = inputs.get(1).values();
        Vector replacementValues = inputs.get(2).values();
        Vector valuesNulls = inputs.get(0).getOrNull(Stream.NULLS);
        Vector patternNulls = inputs.get(1).getOrNull(Stream.NULLS);
        Vector replacementNulls = inputs.get(2).getOrNull(Stream.NULLS);

        int requiredLength = maxLength(mask, values, patternValues, replacementValues);

        Streams result = Streams.empty();
        BooleanVector outputNulls = null;
        if (requestedStreams.contains(Stream.NULLS)) {
            outputNulls = VectorAccess.writableBooleanVector(
                    context.allocator(),
                    allocationContext,
                    output != null && output.has(Stream.NULLS) ? output.get(Stream.NULLS) : null,
                    requiredLength);
            fillNulls(valuesNulls, patternNulls, replacementNulls, mask, outputNulls);
            result = result.with(Stream.NULLS, outputNulls);
        }

        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        Vector rewritten = applyGeneric(values, valuesNulls, patternValues, patternNulls, replacementValues, replacementNulls, mask, requiredLength, output, context, outputNulls);
        return result.with(Stream.VALUES, rewritten);
    }

    private Vector applyGeneric(
            Vector values,
            Vector valuesNulls,
            Vector patternValues,
            Vector patternNulls,
            Vector replacementValues,
            Vector replacementNulls,
            Mask mask,
            int requiredLength,
            Streams output,
            PrimitiveExecutionContext context,
            BooleanVector outputNulls)
    {
        VectorAccess.BooleanValues valueNullValues = VectorAccess.booleanValues(valuesNulls);
        VectorAccess.BooleanValues patternNullValues = VectorAccess.booleanValues(patternNulls);
        VectorAccess.BooleanValues replacementNullValues = VectorAccess.booleanValues(replacementNulls);
        byte[][] rewritten = new byte[mask.selectedCount()][];
        Slice constantReplacement = null;
        Regex constantPattern = null;
        if (policy.constantArguments() &&
                patternValues instanceof RleVector patternRle && patternRle.counts().length == 1 &&
                replacementValues instanceof RleVector replacementRle && replacementRle.counts().length == 1 &&
                VectorAccess.isAllFalseNulls(patternNulls) && VectorAccess.isAllFalseNulls(replacementNulls)) {
            constantPattern = JoniRegexpSupport.compile(utf8Slice(patternRle.values(), 0));
            constantReplacement = translateReplacement(utf8Slice(replacementRle.values(), 0));
        }
        Map<Slice, Regex> patterns = constantPattern == null ? new HashMap<>() : null;
        int totalBytes = 0;
        boolean asciiOnly = true;
        int index = 0;
        for (int position : mask) {
            if (valueNullValues.value(position) || patternNullValues.value(position) || replacementNullValues.value(position)) {
                rewritten[index++] = null;
                continue;
            }

            Slice input = utf8Slice(values, position);
            Slice replacementValue = constantReplacement;
            Regex pattern = constantPattern;
            if (pattern == null) {
                Slice patternValue = utf8Slice(patternValues, position);
                replacementValue = translateReplacement(utf8Slice(replacementValues, position));
                pattern = patterns.computeIfAbsent(patternValue, JoniRegexpSupport::compile);
            }
            byte[] bytes = JoniRegexpSupport.replace(input, pattern, replacementValue, joniPolicy).getBytes();
            rewritten[index++] = bytes;
            totalBytes += bytes.length;
            asciiOnly &= isAscii(bytes);
        }

        BinaryVector outputValues = BinaryVector.allocateOrGrow(
                context.allocator(),
                allocationContext,
                output != null && output.has(Stream.VALUES) && output.values() instanceof BinaryVector vector ? vector : null,
                requiredLength,
                totalBytes);
        outputValues.clearTraits();
        outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        if (asciiOnly) {
            outputValues.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        }

        // The mask can be sparse, but the offsets array is a cumulative chain over ALL positions -- a bare
        // positional setBytes after a gap would inherit a stale start offset and shear every later value.
        // Fill the skipped positions' offsets forward (empty values) exactly like ConcatUtf8.
        index = 0;
        int currentOffset = 0;
        int lastPosition = -1;
        for (int position : mask) {
            fillOffsets(outputValues, lastPosition + 1, position, currentOffset);
            byte[] bytes = rewritten[index++];
            if (bytes == null) {
                outputValues.setNull(position);
                if (outputNulls != null) {
                    outputNulls.values()[position] = true;
                }
            }
            else {
                outputValues.setBytes(position, bytes);
                currentOffset += bytes.length;
                if (outputNulls != null) {
                    outputNulls.values()[position] = false;
                }
            }
            lastPosition = position;
        }
        fillOffsets(outputValues, lastPosition + 1, outputValues.length(), currentOffset);
        return outputValues;
    }

    private static void fillOffsets(BinaryVector outputValues, int startInclusive, int endExclusive, int offset)
    {
        for (int position = startInclusive; position <= endExclusive; position++) {
            outputValues.offsets()[position] = offset;
        }
    }

    private static void fillNulls(Vector valuesNulls, Vector patternNulls, Vector replacementNulls, Mask mask, BooleanVector outputNulls)
    {
        VectorAccess.BooleanValues valueNullValues = VectorAccess.booleanValues(valuesNulls);
        VectorAccess.BooleanValues patternNullValues = VectorAccess.booleanValues(patternNulls);
        VectorAccess.BooleanValues replacementNullValues = VectorAccess.booleanValues(replacementNulls);
        boolean[] nulls = outputNulls.values();
        Arrays.fill(nulls, 0, outputNulls.length(), false);
        for (int position : mask) {
            nulls[position] = valueNullValues.value(position) || patternNullValues.value(position) || replacementNullValues.value(position);
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

    private static Slice utf8Slice(Vector values, int position)
    {
        return switch (values) {
            case BinaryVector binary -> Slices.wrappedBuffer(binary.data(), binary.startOffset(position), binary.length(position));
            case RleVector rle -> utf8Slice(rle.values(), rle.runIndex(position));
            case org.weakref.nitro.data.DictionaryVector dictionary -> utf8Slice(dictionary.values(), dictionary.ids()[position]);
            default -> throw new IllegalArgumentException("Unsupported regexp_replace_utf8 vector type: " + values.getClass().getSimpleName());
        };
    }

    /**
     * Translate SQL replacement syntax (backslash group references) to RE2J's: public so a dictionary-level
     * derivation (the compiled loader's regexp-derived columns) applies the exact same replacement semantics.
     */
    public static Slice translateReplacement(Slice replacement)
    {
        DynamicSliceOutput translated = new DynamicSliceOutput(replacement.length());
        int index = 0;
        while (index < replacement.length()) {
            byte current = replacement.getByte(index++);
            if (current == '\\') {
                if (index == replacement.length()) {
                    throw new IllegalArgumentException("Illegal replacement sequence");
                }
                byte escaped = replacement.getByte(index++);
                if (escaped >= '0' && escaped <= '9') {
                    translated.appendByte('$');
                    translated.appendByte(escaped);
                    continue;
                }
                appendLiteralByte(translated, escaped);
                continue;
            }
            appendLiteralByte(translated, current);
        }
        return translated.slice();
    }

    private static void appendLiteralByte(DynamicSliceOutput output, byte value)
    {
        if (value == '\\' || value == '$') {
            output.appendByte('\\');
        }
        output.appendByte(value);
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
