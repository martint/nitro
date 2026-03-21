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

final class Utf8Support
{
    private Utf8Support() {}

    public static int javaStringHash(BinaryVector vector, int position)
    {
        byte[] data = vector.data();
        int offset = vector.startOffset(position);
        int end = vector.endOffset(position);
        int hash = 0;
        while (offset < end) {
            int firstByte = Byte.toUnsignedInt(data[offset++]);
            if (firstByte < 0x80) {
                hash = 31 * hash + firstByte;
                continue;
            }

            int codePoint;
            if ((firstByte & 0xE0) == 0xC0) {
                codePoint = ((firstByte & 0x1F) << 6) | continuation(data, offset++, end);
            }
            else if ((firstByte & 0xF0) == 0xE0) {
                codePoint = ((firstByte & 0x0F) << 12) |
                        (continuation(data, offset++, end) << 6) |
                        continuation(data, offset++, end);
            }
            else if ((firstByte & 0xF8) == 0xF0) {
                codePoint = ((firstByte & 0x07) << 18) |
                        (continuation(data, offset++, end) << 12) |
                        (continuation(data, offset++, end) << 6) |
                        continuation(data, offset++, end);
            }
            else {
                throw new IllegalArgumentException("Invalid UTF-8 leading byte: " + firstByte);
            }

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
}
