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
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.function.scalar.PrimitiveExecutionContext;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarFunction;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * SQL LIKE over a constant {@code %}-wildcard pattern (no {@code _} support -- TPC-H has none): the pattern
 * splits into literal segments that must appear in order, the first anchored to the start and the last to the
 * end unless the pattern opens/closes with {@code %}. A dictionary input evaluates per distinct entry.
 */
@ScalarFunction(name = "like_utf8")
public final class LikeUtf8
        implements PrimitiveFunction
{
    private final Allocator.Context allocationContext = new Allocator.Context("LikeUtf8");
    private final ConcurrentHashMap<String, Pattern> patterns = new ConcurrentHashMap<>();
    private final LikeUtf8Policy policy;

    public LikeUtf8()
    {
        this(LikeUtf8Policy.defaults());
    }

    public LikeUtf8(LikeUtf8Policy policy)
    {
        this.policy = requireNonNull(policy, "policy is null");
    }

    @Override
    public Set<Allocator.Context> allocationContexts()
    {
        return Set.of(allocationContext);
    }

    @Override
    public Set<Stream> requiredInputStreams(int inputIndex, Set<Stream> requestedOutputStreams)
    {
        return PrimitiveFunction.valuesAlwaysNullsWhenRequested(requestedOutputStreams);
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Set<Stream> requestedStreams, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == 2, "Unexpected argument count for like_utf8");
        if (!requestedStreams.contains(Stream.VALUES) && !requestedStreams.contains(Stream.NULLS)) {
            return Streams.empty();
        }

        Pattern pattern = parsePattern(constantPatternBytes(inputs.get(1).values()));
        Vector input = inputs.getFirst().values();
        Vector inputNulls = inputs.getFirst().getOrNull(Stream.NULLS);
        int requiredLength = Math.max(mask.maxPosition() + 1, input.length());

        Streams result = Streams.empty();
        if (requestedStreams.contains(Stream.NULLS)) {
            if (inputNulls != null) {
                result = result.with(Stream.NULLS, inputNulls);
            }
            else {
                BooleanVector nulls = VectorAccess.writableBooleanVector(context.allocator(), allocationContext, null, requiredLength);
                java.util.Arrays.fill(nulls.values(), 0, nulls.length(), false);
                result = result.with(Stream.NULLS, nulls);
            }
        }
        if (!requestedStreams.contains(Stream.VALUES)) {
            return result;
        }

        BooleanVector values = VectorAccess.writableBooleanVector(
                context.allocator(),
                allocationContext,
                output != null ? output.getOrNull(Stream.VALUES) : null,
                requiredLength);
        boolean[] outputValues = values.values();
        switch (input) {
            case BinaryVector binary -> {
                for (int position : mask) {
                    outputValues[position] = pattern.matches(binary.data(), binary.startOffset(position), binary.length(position));
                }
            }
            case DictionaryVector dictionary when dictionary.values() instanceof BinaryVector entries -> {
                boolean[] entryMatches = new boolean[entries.length()];
                for (int entry = 0; entry < entries.length(); entry++) {
                    entryMatches[entry] = pattern.matches(entries.data(), entries.startOffset(entry), entries.length(entry));
                }
                int[] ids = dictionary.ids();
                for (int position : mask) {
                    outputValues[position] = entryMatches[ids[position]];
                }
            }
            case RleVector rle when rle.values() instanceof BinaryVector entries -> {
                for (int position : mask) {
                    int index = rle.runIndex(position);
                    outputValues[position] = pattern.matches(entries.data(), entries.startOffset(index), entries.length(index));
                }
            }
            default -> throw new IllegalArgumentException("Unsupported like_utf8 input vector: " + input.getClass().getSimpleName());
        }
        return result.with(Stream.VALUES, values);
    }

    private static byte[] constantPatternBytes(Vector pattern)
    {
        return switch (pattern) {
            case BinaryVector binary -> {
                byte[] bytes = new byte[binary.length(0)];
                System.arraycopy(binary.data(), binary.startOffset(0), bytes, 0, bytes.length);
                yield bytes;
            }
            case DictionaryVector dictionary when dictionary.values() instanceof BinaryVector entries -> {
                int entry = dictionary.ids()[0];
                byte[] bytes = new byte[entries.length(entry)];
                System.arraycopy(entries.data(), entries.startOffset(entry), bytes, 0, bytes.length);
                yield bytes;
            }
            case RleVector rle when rle.values() instanceof BinaryVector entries -> {
                byte[] bytes = new byte[entries.length(0)];
                System.arraycopy(entries.data(), entries.startOffset(0), bytes, 0, bytes.length);
                yield bytes;
            }
            default -> throw new IllegalArgumentException("like_utf8 pattern must be a constant string");
        };
    }

    private Pattern parsePattern(byte[] pattern)
    {
        String text = new String(pattern, StandardCharsets.UTF_8);
        return patterns.computeIfAbsent(text, value -> compilePattern(value, policy));
    }

    static Pattern compilePattern(String text, LikeUtf8Policy policy)
    {
        requireNonNull(text, "text is null");
        requireNonNull(policy, "policy is null");
        checkArgument(text.indexOf('_') < 0, "like_utf8 does not support the _ wildcard: %s", text);
        boolean anchoredStart = !text.startsWith("%");
        boolean anchoredEnd = !text.endsWith("%");
        List<Segment> segments = new ArrayList<>();
        for (String segment : text.split("%", -1)) {
            if (!segment.isEmpty()) {
                segments.add(new Segment(segment.getBytes(StandardCharsets.UTF_8)));
            }
        }
        return new Pattern(anchoredStart, anchoredEnd, segments, policy.boyerMooreHorspool());
    }

    record Pattern(boolean anchoredStart, boolean anchoredEnd, List<Segment> segments, boolean boyerMooreHorspool)
    {
        boolean matches(byte[] data, int offset, int length)
        {
            if (segments.isEmpty()) {
                return !anchoredStart || !anchoredEnd || length == 0;
            }
            int cursor = offset;
            int end = offset + length;
            for (int index = 0; index < segments.size(); index++) {
                Segment segment = segments.get(index);
                boolean last = index == segments.size() - 1;
                if (index == 0 && anchoredStart) {
                    if (length < segment.bytes.length || !regionEquals(data, offset, segment.bytes)) {
                        return false;
                    }
                    cursor = offset + segment.bytes.length;
                }
                else if (last && anchoredEnd) {
                    int start = end - segment.bytes.length;
                    return start >= cursor && regionEquals(data, start, segment.bytes);
                }
                else {
                    int found = indexOf(data, cursor, end, segment);
                    if (found < 0) {
                        return false;
                    }
                    cursor = found + segment.bytes.length;
                }
                if (last && anchoredEnd && index == 0 && anchoredStart) {
                    return cursor == end;
                }
            }
            // The single anchored-both case is handled above; with a trailing %, reaching here is a match.
            return !anchoredEnd || segments.size() == 1 && anchoredStart && cursor == end;
        }

        private static boolean regionEquals(byte[] data, int offset, byte[] segment)
        {
            for (int index = 0; index < segment.length; index++) {
                if (data[offset + index] != segment[index]) {
                    return false;
                }
            }
            return true;
        }

        private int indexOf(byte[] data, int from, int end, Segment segment)
        {
            byte[] bytes = segment.bytes;
            int limit = end - bytes.length;
            if (boyerMooreHorspool && bytes.length >= 4) {
                int last = bytes.length - 1;
                int start = from;
                while (start <= limit) {
                    int index = last;
                    while (index >= 0 && data[start + index] == bytes[index]) {
                        index--;
                    }
                    if (index < 0) {
                        return start;
                    }
                    start += segment.shifts[data[start + last] & 0xFF];
                }
                return -1;
            }
            for (int start = from; start <= limit; start++) {
                if (regionEquals(data, start, bytes)) {
                    return start;
                }
            }
            return -1;
        }
    }

    private record Segment(byte[] bytes, int[] shifts)
    {
        private Segment(byte[] bytes)
        {
            this(bytes, shifts(bytes));
        }

        private static int[] shifts(byte[] bytes)
        {
            int[] shifts = new int[256];
            java.util.Arrays.fill(shifts, bytes.length);
            for (int index = 0; index < bytes.length - 1; index++) {
                shifts[bytes[index] & 0xFF] = bytes.length - 1 - index;
            }
            return shifts;
        }
    }
}
