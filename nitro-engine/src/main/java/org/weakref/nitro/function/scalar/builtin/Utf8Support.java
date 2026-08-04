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

import org.weakref.nitro.data.BinaryVector;

import static java.lang.Math.toIntExact;

public final class Utf8Support
{
    private Utf8Support() {}

    public static byte[] substring(BinaryVector vector, int position, long start, long length)
    {
        return substring(vector.data(), vector.startOffset(position), vector.length(position), start, length);
    }

    public static byte[] upper(BinaryVector vector, int position)
    {
        return upper(vector.data(), vector.startOffset(position), vector.length(position));
    }

    public static byte[] substring(byte[] data, int offset, int length, long start, long count)
    {
        long slice = substringSlice(data, offset, length, start, count);
        int startOffset = (int) (slice >>> 32);
        int endOffset = startOffset + (int) slice;
        return java.util.Arrays.copyOfRange(data, startOffset, endOffset);
    }

    /** Returns the exact UTF-8 substring as {@code (sourceOffset << 32) | byteLength}, without copying bytes. */
    public static long substringSlice(byte[] data, int offset, int length, long start, long count)
    {
        int startOffset = substringStartOffset(data, offset, length, start, count);
        int endOffset = substringEndOffset(data, offset, length, count, startOffset);
        return ((long) startOffset << 32) | ((endOffset - startOffset) & 0xFFFF_FFFFL);
    }

    /** Uses byte coordinates for a positive substring when the inspected prefix is ASCII, with exact UTF-8 fallback. */
    public static long substringSlicePositiveAsciiFast(byte[] data, int offset, int length, long start, long count)
    {
        if (start <= 0) {
            throw new IllegalArgumentException("start must be positive");
        }
        if (count <= 0 || length <= 0) {
            return (long) offset << 32;
        }

        int startIndex = toIntExact(start - 1);
        if (startIndex >= length) {
            return (long) (offset + length) << 32;
        }
        long requestedEnd = (long) startIndex + count;
        int checkedLength = requestedEnd < 0 || requestedEnd >= length ? length : (int) requestedEnd;
        for (int index = 0; index < checkedLength; index++) {
            if (data[offset + index] < 0) {
                return substringSlice(data, offset, length, start, count);
            }
        }

        int startOffset = offset + startIndex;
        return ((long) startOffset << 32) | ((checkedLength - startIndex) & 0xFFFF_FFFFL);
    }

    public static boolean substringMatchesAny(byte[] data, int offset, int length, long start, long count, byte[][] values)
    {
        int startOffset = substringStartOffset(data, offset, length, start, count);
        int endOffset = substringEndOffset(data, offset, length, count, startOffset);
        return matchesAny(data, startOffset, endOffset - startOffset, values);
    }

    public static boolean substringMatchesAnyAsciiFast(byte[] data, int offset, int length, long start, long count, byte[][] values)
    {
        if (count <= 0 || length <= 0) {
            return matchesAny(data, offset, 0, values);
        }

        int startCodePoint = Math.max(0, toIntExact(start - 1));
        long requestedEnd = (long) startCodePoint + count;
        int checkedLength = requestedEnd < 0 || requestedEnd >= length ? length : (int) requestedEnd;
        for (int index = 0; index < checkedLength; index++) {
            if (data[offset + index] < 0) {
                return substringMatchesAny(data, offset, length, start, count, values);
            }
        }

        int startOffset = offset + Math.min(startCodePoint, length);
        int endOffset = offset + checkedLength;
        return matchesAny(data, startOffset, endOffset - startOffset, values);
    }

    public static byte[] upper(byte[] data, int offset, int length)
    {
        if (length <= 0) {
            return new byte[0];
        }

        int end = offset + length;
        byte[] output = new byte[length * 4];
        int outputOffset = 0;
        int inputOffset = offset;
        while (inputOffset < end) {
            int codePoint = codePoint(data, inputOffset, end);
            inputOffset = nextCodePointOffset(data, inputOffset, end);
            int upperCodePoint = Character.toUpperCase(codePoint);
            outputOffset = appendCodePoint(output, outputOffset, upperCodePoint);
        }
        return java.util.Arrays.copyOf(output, outputOffset);
    }

    public static int upperLength(byte[] data, int offset, int length)
    {
        int end = offset + length;
        int outputLength = 0;
        while (offset < end) {
            int upperCodePoint = Character.toUpperCase(codePoint(data, offset, end));
            offset = nextCodePointOffset(data, offset, end);
            outputLength += utf8Length(upperCodePoint);
        }
        return outputLength;
    }

    /** Writes the upper-cased value into {@code output} and returns the first offset after it. */
    public static int upperInto(byte[] data, int offset, int length, byte[] output, int outputOffset)
    {
        int end = offset + length;
        while (offset < end) {
            int upperCodePoint = Character.toUpperCase(codePoint(data, offset, end));
            offset = nextCodePointOffset(data, offset, end);
            outputOffset = appendCodePoint(output, outputOffset, upperCodePoint);
        }
        return outputOffset;
    }

    private static int utf8Length(int codePoint)
    {
        if (codePoint < 0x80) {
            return 1;
        }
        if (codePoint < 0x800) {
            return 2;
        }
        if (codePoint < 0x10000) {
            return 3;
        }
        return 4;
    }

    public static int javaStringHash(BinaryVector vector, int position)
    {
        byte[] data = vector.data();
        int offset = vector.startOffset(position);
        int end = vector.endOffset(position);
        int hash = 0;
        while (offset < end) {
            int codePoint = codePoint(data, offset, end);
            offset = nextCodePointOffset(data, offset, end);
            if (codePoint < 0x10000) {
                hash = 31 * hash + codePoint;
                continue;
            }

            int supplementary = codePoint - 0x10000;
            int highSurrogate = 0xD800 + (supplementary >>> 10);
            int lowSurrogate = 0xDC00 + (supplementary & 0x3FF);
            hash = 31 * hash + highSurrogate;
            hash = 31 * hash + lowSurrogate;
        }
        return hash;
    }

    private static int codePoint(byte[] data, int offset, int end)
    {
        int firstByte = Byte.toUnsignedInt(data[offset++]);
        if (firstByte < 0x80) {
            return firstByte;
        }
        if ((firstByte & 0xE0) == 0xC0) {
            return ((firstByte & 0x1F) << 6) | continuation(data, offset++, end);
        }
        if ((firstByte & 0xF0) == 0xE0) {
            return ((firstByte & 0x0F) << 12) |
                    (continuation(data, offset++, end) << 6) |
                    continuation(data, offset++, end);
        }
        if ((firstByte & 0xF8) == 0xF0) {
            return ((firstByte & 0x07) << 18) |
                    (continuation(data, offset++, end) << 12) |
                    (continuation(data, offset++, end) << 6) |
                    continuation(data, offset++, end);
        }
        throw new IllegalArgumentException("Invalid UTF-8 leading byte: " + firstByte);
    }

    private static int appendCodePoint(byte[] output, int offset, int codePoint)
    {
        if (codePoint < 0x80) {
            output[offset] = (byte) codePoint;
            return offset + 1;
        }
        if (codePoint < 0x800) {
            output[offset] = (byte) (0xC0 | (codePoint >>> 6));
            output[offset + 1] = (byte) (0x80 | (codePoint & 0x3F));
            return offset + 2;
        }
        if (codePoint < 0x10000) {
            output[offset] = (byte) (0xE0 | (codePoint >>> 12));
            output[offset + 1] = (byte) (0x80 | ((codePoint >>> 6) & 0x3F));
            output[offset + 2] = (byte) (0x80 | (codePoint & 0x3F));
            return offset + 3;
        }
        output[offset] = (byte) (0xF0 | (codePoint >>> 18));
        output[offset + 1] = (byte) (0x80 | ((codePoint >>> 12) & 0x3F));
        output[offset + 2] = (byte) (0x80 | ((codePoint >>> 6) & 0x3F));
        output[offset + 3] = (byte) (0x80 | (codePoint & 0x3F));
        return offset + 4;
    }

    private static int continuation(byte[] data, int offset, int end)
    {
        if (offset >= end) {
            throw new IllegalArgumentException("Truncated UTF-8 sequence");
        }
        int value = Byte.toUnsignedInt(data[offset]);
        if ((value & 0xC0) != 0x80) {
            throw new IllegalArgumentException("Invalid UTF-8 continuation byte: " + value);
        }
        return value & 0x3F;
    }

    private static int substringStartOffset(byte[] data, int offset, int length, long start, long count)
    {
        if (count <= 0 || length <= 0) {
            return offset;
        }

        int end = offset + length;
        int startCodePoint = Math.max(0, toIntExact(start - 1));
        int startOffset = offset;
        for (int index = 0; index < startCodePoint && startOffset < end; index++) {
            startOffset = nextCodePointOffset(data, startOffset, end);
        }
        return Math.min(startOffset, end);
    }

    private static int substringEndOffset(byte[] data, int offset, int length, long count, int startOffset)
    {
        if (count <= 0 || length <= 0) {
            return startOffset;
        }

        int end = offset + length;
        int endOffset = startOffset;
        for (long index = 0; index < count && endOffset < end; index++) {
            endOffset = nextCodePointOffset(data, endOffset, end);
        }
        return endOffset;
    }

    private static boolean regionEquals(byte[] left, int leftOffset, byte[] right, int length)
    {
        for (int index = 0; index < length; index++) {
            if (left[leftOffset + index] != right[index]) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesAny(byte[] data, int offset, int length, byte[][] values)
    {
        for (byte[] value : values) {
            if (length == value.length && regionEquals(data, offset, value, length)) {
                return true;
            }
        }
        return false;
    }

    private static int nextCodePointOffset(byte[] data, int offset, int end)
    {
        if (offset >= end) {
            return end;
        }

        int firstByte = Byte.toUnsignedInt(data[offset]);
        if (firstByte < 0x80) {
            return offset + 1;
        }
        if ((firstByte & 0xE0) == 0xC0) {
            continuation(data, offset + 1, end);
            return offset + 2;
        }
        if ((firstByte & 0xF0) == 0xE0) {
            continuation(data, offset + 1, end);
            continuation(data, offset + 2, end);
            return offset + 3;
        }
        if ((firstByte & 0xF8) == 0xF0) {
            continuation(data, offset + 1, end);
            continuation(data, offset + 2, end);
            continuation(data, offset + 3, end);
            return offset + 4;
        }
        throw new IllegalArgumentException("Invalid UTF-8 leading byte: " + firstByte);
    }
}
