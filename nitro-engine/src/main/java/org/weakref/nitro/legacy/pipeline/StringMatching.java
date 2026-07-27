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
package org.weakref.nitro.legacy.pipeline;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.regex.Pattern;

/**
 * Runtime helpers for string predicates a generated pipeline evaluates once per dictionary entry. Kept off the
 * per-row hot path: a {@code LIKE} pattern compiles to a {@link Pattern} a single time, then matches each
 * dictionary entry.
 */
public final class StringMatching
{
    private StringMatching() {}

    /**
     * Translate a SQL {@code LIKE} pattern into a {@link Pattern}: {@code %} -> {@code .*}, {@code _} -> {@code .},
     * with all other characters quoted. {@code DOTALL} so {@code %} also spans newlines, as SQL requires.
     */
    /** Byte-level substring containment (the {@code LIKE '%literal%'} fast path; UTF-8 substrings align on bytes). */
    public static boolean containsBytes(byte[] value, byte[] target)
    {
        if (target.length == 0) {
            return true;
        }
        outer:
        for (int from = 0; from <= value.length - target.length; from++) {
            for (int i = 0; i < target.length; i++) {
                if (value[from + i] != target[i]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    /** The number of code points in UTF-8 bytes (continuation bytes don't start a code point). */
    public static int codePointCount(byte[] utf8)
    {
        return codePointCount(utf8, 0, utf8.length);
    }

    private static final VarHandle LONG_VIEW = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT_VIEW = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final long TOP_MASK_64 = 0x8080808080808080L;
    private static final int TOP_MASK_32 = 0x80808080;

    /**
     * Code points of the UTF-8 slice {@code [from, to)} -- the in-place form for zero-copy view rows. Counts the
     * continuation bytes ({@code 0b10xx_xxxx}) eight at a time with a SWAR bit trick rather than a branch per byte:
     * a long's continuation bytes are exactly the ones where bit 7 is set and bit 6 is clear, so
     * {@code ((w & 0x8080..) >>> 1) & ~w} lands a one in each matching byte's bit 6, and {@link Long#bitCount}
     * counts them. Code points = byte length minus continuation bytes. (This is airlift SliceUtf8's algorithm,
     * which profiling showed ~2x faster than a scalar per-byte loop on long ClickBench URLs; the JIT compiles it
     * to tight scalar code -- unlike a manual Vector-API count, which carried more overhead than it saved.)
     */
    public static int codePointCount(byte[] utf8, int from, int to)
    {
        int length = to - from;
        if (length <= 0) {
            return 0;
        }
        int continuation = 0;
        int offset = from;
        int lastLongStart = to - Long.BYTES;
        for (; offset <= lastLongStart; offset += Long.BYTES) {
            long word = (long) LONG_VIEW.get(utf8, offset);
            continuation += Long.bitCount(((word & TOP_MASK_64) >>> 1) & (~word));
        }
        if (offset <= to - Integer.BYTES) {
            int word = (int) INT_VIEW.get(utf8, offset);
            continuation += Integer.bitCount(((word & TOP_MASK_32) >>> 1) & (~word));
            offset += Integer.BYTES;
        }
        for (; offset < to; offset++) {
            if ((utf8[offset] & 0xC0) == 0x80) {
                continuation++;
            }
        }
        return length - continuation;
    }

    /**
     * A {@code %}-only LIKE pattern as anchored byte segments: each segment must appear in order, the first
     * anchored to the start unless the pattern opens with {@code %}, the last anchored to the end unless it
     * closes with one. Sequential byte scans instead of the regex engine -- a multi-segment pattern over a
     * high-cardinality column (TPC-H Q13's {@code %special%requests%} per order comment) pays an order of
     * magnitude less per value. Callers route patterns containing {@code _} to {@link #likePattern} instead.
     */
    public static LikeSegments likeSegments(String like)
    {
        boolean anchoredStart = !like.startsWith("%");
        boolean anchoredEnd = !like.endsWith("%");
        java.util.List<byte[]> segments = new java.util.ArrayList<>();
        for (String segment : like.split("%", -1)) {
            if (!segment.isEmpty()) {
                segments.add(segment.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        return new LikeSegments(anchoredStart, anchoredEnd, segments.toArray(new byte[0][]));
    }

    public record LikeSegments(boolean anchoredStart, boolean anchoredEnd, byte[][] segments)
    {
        public boolean matches(byte[] value)
        {
            int length = value.length;
            if (segments.length == 0) {
                return !anchoredStart || !anchoredEnd || length == 0;
            }
            int cursor = 0;
            for (int index = 0; index < segments.length; index++) {
                byte[] segment = segments[index];
                boolean last = index == segments.length - 1;
                if (index == 0 && anchoredStart) {
                    if (length < segment.length || !regionEquals(value, 0, segment)) {
                        return false;
                    }
                    cursor = segment.length;
                }
                else if (last && anchoredEnd) {
                    int start = length - segment.length;
                    return start >= cursor && regionEquals(value, start, segment);
                }
                else {
                    int found = indexOf(value, cursor, length, segment);
                    if (found < 0) {
                        return false;
                    }
                    cursor = found + segment.length;
                }
                if (last && anchoredEnd && index == 0 && anchoredStart) {
                    return cursor == length;
                }
            }
            // The single anchored-both case returned above; with a trailing %, reaching here is a match.
            return !anchoredEnd || segments.length == 1 && anchoredStart && cursor == length;
        }

        private static boolean regionEquals(byte[] value, int offset, byte[] segment)
        {
            for (int index = 0; index < segment.length; index++) {
                if (value[offset + index] != segment[index]) {
                    return false;
                }
            }
            return true;
        }

        private static int indexOf(byte[] value, int from, int end, byte[] segment)
        {
            int limit = end - segment.length;
            outer:
            for (int start = from; start <= limit; start++) {
                for (int index = 0; index < segment.length; index++) {
                    if (value[start + index] != segment[index]) {
                        continue outer;
                    }
                }
                return start;
            }
            return -1;
        }
    }

    public static Pattern likePattern(String like)
    {
        StringBuilder regex = new StringBuilder();
        StringBuilder literal = new StringBuilder();
        for (int i = 0; i < like.length(); i++) {
            char c = like.charAt(i);
            if (c == '%' || c == '_') {
                if (literal.length() > 0) {
                    regex.append(Pattern.quote(literal.toString()));
                    literal.setLength(0);
                }
                regex.append(c == '%' ? ".*" : ".");
            }
            else {
                literal.append(c);
            }
        }
        if (literal.length() > 0) {
            regex.append(Pattern.quote(literal.toString()));
        }
        return Pattern.compile(regex.toString(), Pattern.DOTALL);
    }
}
