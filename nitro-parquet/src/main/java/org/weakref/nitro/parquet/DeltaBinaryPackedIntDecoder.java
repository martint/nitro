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
package org.weakref.nitro.parquet;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/** Reusable page-local decoder for Parquet DELTA_BINARY_PACKED INT32 streams. */
final class DeltaBinaryPackedIntDecoder
{
    private MemorySegment input;
    private long position;

    void reset(MemorySegment input, long position)
    {
        this.input = requireNonNull(input, "input is null");
        if (position < 0 || position > input.byteSize()) {
            throw new IndexOutOfBoundsException("Invalid delta stream offset: " + position);
        }
        this.position = position;
    }

    void decode(int[] output, int expectedValueCount)
    {
        requireNonNull(output, "output is null");
        if (expectedValueCount < 0 || expectedValueCount > output.length) {
            throw new IndexOutOfBoundsException("Invalid delta output count: " + expectedValueCount);
        }
        int blockSize = readUnsignedVarInt();
        int miniBlockCount = readUnsignedVarInt();
        int valueCount = readUnsignedVarInt();
        if (blockSize <= 0 || blockSize % 128 != 0) {
            throw malformed("block size must be a positive multiple of 128: " + blockSize);
        }
        if (miniBlockCount <= 0 || blockSize % miniBlockCount != 0) {
            throw malformed("invalid mini-block count: " + miniBlockCount);
        }
        int miniBlockSize = blockSize / miniBlockCount;
        if (miniBlockSize % 32 != 0) {
            throw malformed("mini-block size must be a multiple of 32: " + miniBlockSize);
        }
        if (valueCount != expectedValueCount) {
            throw malformed("value count " + valueCount + " does not match page value count " + expectedValueCount);
        }
        if (valueCount == 0) {
            return;
        }

        long previous = zigzagDecode(readUnsignedVarLong());
        output[0] = toIntExact(previous);
        int outputIndex = 1;
        while (outputIndex < valueCount) {
            long minimumDelta = zigzagDecode(readUnsignedVarLong());
            requireBytes(miniBlockCount);
            long bitWidthsOffset = position;
            position += miniBlockCount;

            int miniBlocksToRead = Math.min(miniBlockCount, divideRoundUp(valueCount - outputIndex, miniBlockSize));
            for (int miniBlock = 0; miniBlock < miniBlocksToRead; miniBlock++) {
                int bitWidth = input.get(ValueLayout.JAVA_BYTE, bitWidthsOffset + miniBlock) & 0xFF;
                if (bitWidth > Integer.SIZE) {
                    throw malformed("INT32 delta bit width exceeds 32: " + bitWidth);
                }
                int valuesToRead = Math.min(miniBlockSize, valueCount - outputIndex);
                long encodedBytes = ((long) miniBlockSize * bitWidth + 7) / 8;
                requireBytes(encodedBytes);
                long miniBlockOffset = position;
                for (int index = 0; index < valuesToRead; index++) {
                    long adjustedDelta = readPackedUnsigned(miniBlockOffset, (long) index * bitWidth, bitWidth);
                    previous = Math.addExact(previous, Math.addExact(minimumDelta, adjustedDelta));
                    output[outputIndex++] = toIntExact(previous);
                }
                position += encodedBytes;
            }
        }
    }

    long position()
    {
        return position;
    }

    private long readPackedUnsigned(long byteOffset, long bitOffset, int bitWidth)
    {
        if (bitWidth == 0) {
            return 0;
        }
        int byteIndex = toIntExact(bitOffset >>> 3);
        int shift = (int) (bitOffset & 7);
        int byteCount = (shift + bitWidth + 7) >>> 3;
        long packed = 0;
        for (int index = 0; index < byteCount; index++) {
            packed |= (long) (input.get(ValueLayout.JAVA_BYTE, byteOffset + byteIndex + index) & 0xFF) << (index * 8);
        }
        return (packed >>> shift) & ((1L << bitWidth) - 1);
    }

    private int readUnsignedVarInt()
    {
        long value = readUnsignedVarLong();
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw malformed("integer varint overflow");
        }
        return (int) value;
    }

    private long readUnsignedVarLong()
    {
        long value = 0;
        for (int shift = 0; shift < Long.SIZE; shift += 7) {
            requireBytes(1);
            int next = input.get(ValueLayout.JAVA_BYTE, position++) & 0xFF;
            if (shift == 63 && (next & 0xFE) != 0) {
                throw malformed("integer varint overflow");
            }
            value |= (long) (next & 0x7F) << shift;
            if ((next & 0x80) == 0) {
                return value;
            }
        }
        throw malformed("unterminated varint");
    }

    private void requireBytes(long count)
    {
        if (count < 0 || position > input.byteSize() - count) {
            throw malformed("truncated stream");
        }
    }

    private static long zigzagDecode(long value)
    {
        return (value >>> 1) ^ -(value & 1);
    }

    private static int divideRoundUp(int dividend, int divisor)
    {
        return dividend == 0 ? 0 : 1 + (dividend - 1) / divisor;
    }

    private static IllegalArgumentException malformed(String message)
    {
        return new IllegalArgumentException("Malformed Parquet delta-binary-packed stream: " + message);
    }
}
